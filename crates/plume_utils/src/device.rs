use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use idevice::core_device_proxy::CoreDeviceProxy;
use idevice::installation_proxy::InstallationProxyClient;
use idevice::lockdown::LockdownClient;
use idevice::misagent::MisagentClient;
use idevice::provider::UsbmuxdProvider;
use idevice::remote_pairing::{RemotePairingClient, RpPairingFile, RpPairingSocket};
use idevice::rsd::RsdHandshake;
use idevice::usbmuxd::{Connection, UsbmuxdAddr, UsbmuxdDevice};
use idevice::utils::installation;
use idevice::{IdeviceService, RemoteXpcClient};
use if_addrs::{IfAddr, get_if_addrs};
use plume_core::MobileProvision;
use zeroconf::{
    BrowserEvent, MdnsBrowser, ServiceType,
    prelude::{TEventLoop, TMdnsBrowser},
};

use crate::Error;
use crate::options::SignerAppReal;
use idevice::afc::opcode::AfcFopenMode;
use idevice::house_arrest::HouseArrestClient;
use idevice::usbmuxd::UsbmuxdConnection;
use plist::Value;

pub const CONNECTION_LABEL: &str = "plume_info";
pub const INSTALLATION_LABEL: &str = "plume_install";
pub const HOUSE_ARREST_LABEL: &str = "plume_house_arrest";
const APPLE_TV_MANUAL_PAIRING_SERVICE: &str = "remotepairing-manual-pairing";
const APPLE_TV_LEGACY_PAIRING_SERVICE: &str = "remotepairing";
const MDNS_SERVICE_PROTOCOL: &str = "tcp";
const DEFAULT_APPLE_TV_LEGACY_REMOTE_PAIRING_PORT: u16 = 49152;

macro_rules! get_dict_string {
    ($dict:expr, $key:expr) => {
        $dict
            .as_dictionary()
            .and_then(|dict| dict.get($key))
            .and_then(|v| v.as_string())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "".to_string())
    };
}

#[derive(Debug, Clone)]
pub struct Device {
    pub name: String,
    pub udid: String,
    pub device_id: u32,
    pub product_type: Option<String>,
    pub lockdown_info_available: bool,
    pub usbmuxd_device: Option<UsbmuxdDevice>,
    // On x86_64 macs, `is_mac` variable should never be true
    // since its only true if the device is added manually.
    pub is_mac: bool,
}

#[derive(Debug, Clone)]
struct AppleTvRemotePairingEndpoint {
    service_type: String,
    service_name: String,
    host_name: String,
    service_address: String,
    port: u16,
    device_address: Option<IpAddr>,
}

impl Device {
    pub async fn new(usbmuxd_device: UsbmuxdDevice) -> Self {
        let (name, product_type, lockdown_info_available) =
            match Self::get_info_from_usbmuxd_device(&usbmuxd_device).await {
                Ok((name, product_type)) => (name, product_type, true),
                Err(error) => {
                    log::debug!(
                        "Failed to load device info for {} via Lockdown: {}",
                        usbmuxd_device.udid,
                        error
                    );
                    (String::new(), None, false)
                }
            };

        Device {
            name,
            udid: usbmuxd_device.udid.clone(),
            device_id: usbmuxd_device.device_id.clone(),
            product_type,
            lockdown_info_available,
            usbmuxd_device: Some(usbmuxd_device),
            is_mac: false,
        }
    }

    async fn get_info_from_usbmuxd_device(
        device: &UsbmuxdDevice,
    ) -> Result<(String, Option<String>), Error> {
        let mut lockdown =
            LockdownClient::connect(&device.to_provider(UsbmuxdAddr::default(), CONNECTION_LABEL))
                .await?;
        let values = lockdown.get_value(None, None).await?;
        let product_type = values
            .as_dictionary()
            .and_then(|dict| dict.get("ProductType"))
            .and_then(|value| value.as_string())
            .map(ToOwned::to_owned);

        Ok((get_dict_string!(values, "DeviceName"), product_type))
    }

    pub fn network_address(&self) -> Option<IpAddr> {
        self.usbmuxd_device.as_ref().and_then(|device| {
            if let Connection::Network(addr) = device.connection_type {
                Some(addr)
            } else {
                None
            }
        })
    }

    pub fn can_attempt_remote_pairing(&self) -> bool {
        self.usbmuxd_device
            .as_ref()
            .is_some_and(|device| !matches!(device.connection_type, Connection::Usb))
    }

    pub fn is_apple_tv(&self) -> bool {
        self.product_type
            .as_deref()
            .is_some_and(|product_type| product_type.starts_with("AppleTV"))
            || self.name.to_ascii_lowercase().contains("apple tv")
            || (self.network_address().is_some() && self.udid.contains(':'))
    }

    pub fn supports_apple_tv_pairing(&self) -> bool {
        self.can_attempt_remote_pairing()
            && (self.is_apple_tv() || !self.lockdown_info_available || self.name.is_empty())
    }

    pub async fn installed_apps(&self) -> Result<Vec<SignerAppReal>, Error> {
        let device = match &self.usbmuxd_device {
            Some(dev) => dev,
            None => return Err(Error::Other("Device is not connected via USB".to_string())),
        };

        let provider = device.to_provider(
            UsbmuxdAddr::from_env_var().unwrap_or_default(),
            INSTALLATION_LABEL,
        );

        let mut ic = InstallationProxyClient::connect(&provider).await?;
        let apps = ic.get_apps(Some("User"), None).await?;

        let mut found_apps = Vec::new();

        for (bundle_id, info) in apps {
            let app_name = get_app_name_from_info(&info);
            let signer_app = SignerAppReal::from_bundle_identifier_and_name(
                Some(bundle_id.as_str()),
                app_name.as_deref(),
            );

            if signer_app.app.supports_pairing_file_alt()
                && !found_apps
                    .iter()
                    .any(|a: &SignerAppReal| a.bundle_id == signer_app.bundle_id)
            {
                found_apps.push(signer_app);
            }
        }

        Ok(found_apps)
    }

    pub async fn is_app_installed(&self, bundle_id: &str) -> Result<bool, Error> {
        let device = match &self.usbmuxd_device {
            Some(dev) => dev,
            None => return Err(Error::Other("Device is not connected via USB".to_string())),
        };

        let provider = device.to_provider(
            UsbmuxdAddr::from_env_var().unwrap_or_default(),
            INSTALLATION_LABEL,
        );

        let mut ic = InstallationProxyClient::connect(&provider).await?;
        let apps = ic.get_apps(Some("User"), None).await?;

        Ok(apps.contains_key(bundle_id))
    }

    pub async fn install_profile(&self, profile: &MobileProvision) -> Result<(), Error> {
        if self.usbmuxd_device.is_none() {
            return Err(Error::Other("Device is not connected via USB".to_string()));
        }

        let provider = self.usbmuxd_device.clone().unwrap().to_provider(
            UsbmuxdAddr::from_env_var().unwrap_or_default(),
            INSTALLATION_LABEL,
        );

        let mut mc = MisagentClient::connect(&provider).await?;
        mc.install(profile.data.clone()).await?;

        Ok(())
    }

    pub async fn pair(&self) -> Result<(), Error> {
        if self.usbmuxd_device.is_none() {
            return Err(Error::Other("Device is not connected via USB".to_string()));
        }

        let mut usbmuxd = UsbmuxdConnection::default().await?;

        let provider = self.usbmuxd_device.clone().unwrap().to_provider(
            UsbmuxdAddr::from_env_var().unwrap_or_default(),
            INSTALLATION_LABEL,
        );

        let mut lc = LockdownClient::connect(&provider).await?;
        let id = uuid::Uuid::new_v4().to_string().to_uppercase();
        let buid = usbmuxd.get_buid().await?;
        let mut pairing_file = lc.pair(id, buid, None).await?;
        pairing_file.udid = Some(self.udid.clone());
        let pairing_file = pairing_file.serialize()?;

        usbmuxd.save_pair_record(&self.udid, pairing_file).await?;

        Ok(())
    }

    pub async fn pair_apple_tv<F>(
        &self,
        path_to_store: PathBuf,
        pin_provider: F,
    ) -> Result<(), Error>
    where
        F: Fn() -> String + Send + Sync + 'static,
    {
        let ip = self.network_address();

        let suffix: String = uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(6)
            .collect();
        let hostname = format!("plume-appletv-{suffix}");
        let endpoint = match Self::discover_remote_pairing_endpoint(ip, &self.name) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                log::warn!("Falling back to usbmuxd network address for Apple TV pairing: {error}");
                let Some(ip) = ip else {
                    return Err(error);
                };
                AppleTvRemotePairingEndpoint {
                    service_type: APPLE_TV_LEGACY_PAIRING_SERVICE.to_string(),
                    service_name: self.name.clone(),
                    host_name: ip.to_string(),
                    service_address: ip.to_string(),
                    port: DEFAULT_APPLE_TV_LEGACY_REMOTE_PAIRING_PORT,
                    device_address: Some(ip),
                }
            }
        };
        let pin_provider: Arc<dyn Fn() -> String + Send + Sync> = Arc::new(pin_provider);

        let mut pairing_file = RpPairingFile::generate(&hostname);
        Self::connect_apple_tv_pairing_socket(
            &endpoint,
            &hostname,
            &mut pairing_file,
            Some(pin_provider.clone()),
        )
        .await?;

        // Mirror idevice's tool flow and reconnect once to validate the pairing file.
        Self::connect_apple_tv_pairing_socket(&endpoint, &hostname, &mut pairing_file, None)
            .await?;

        let pairing_dir = path_to_store.join("appletv_pairing");
        tokio::fs::create_dir_all(&pairing_dir).await?;
        let pairing_file_path =
            pairing_dir.join(format!("plume_{}.plist", self.udid.replace(':', "_")));
        pairing_file.write_to_file(&pairing_file_path).await?;

        Ok(())
    }

    pub async fn install_pairing_record(
        &self,
        identifier: &String,
        path: &str,
    ) -> Result<(), Error> {
        if self.usbmuxd_device.is_none() {
            return Err(Error::Other("Device is not connected via USB".to_string()));
        }

        let mut usbmuxd = UsbmuxdConnection::default().await?;
        let provider = self
            .usbmuxd_device
            .clone()
            .unwrap()
            .to_provider(UsbmuxdAddr::default(), HOUSE_ARREST_LABEL);
        let mut pairing_file = usbmuxd.get_pair_record(&self.udid).await?;

        // saving pairing record requires enabling wifi debugging
        // since operations are done over wifi
        let mut lc = LockdownClient::connect(&provider).await?;
        lc.start_session(&pairing_file).await.ok();
        lc.set_value(
            "EnableWifiDebugging",
            true.into(),
            Some("com.apple.mobile.wireless_lockdown"),
        )
        .await
        .ok();

        pairing_file.udid = Some(self.udid.clone());

        let hc = HouseArrestClient::connect(&provider).await?;
        let mut ac = hc.vend_documents(identifier.clone()).await?;
        if let Some(parent) = Path::new(path).parent() {
            let mut current = String::new();
            let has_root = parent.has_root();

            for component in parent.components() {
                if let Component::Normal(dir) = component {
                    if has_root && current.is_empty() {
                        current.push('/');
                    } else if !current.is_empty() && !current.ends_with('/') {
                        current.push('/');
                    }

                    current.push_str(&dir.to_string_lossy());
                    ac.mk_dir(&current).await?;
                }
            }
        }

        let mut f = ac.open(path, AfcFopenMode::Wr).await?;
        f.write_entire(&pairing_file.serialize().unwrap()).await?;

        Ok(())
    }

    pub async fn install_remote_pairing_record(
        &self,
        identifier: &String,
        path: &str,
        path_to_store: PathBuf,
    ) -> Result<(), Error> {
        if self.usbmuxd_device.is_none() {
            return Err(Error::Other("Device is not connected via USB".to_string()));
        }

        let provider = self
            .usbmuxd_device
            .clone()
            .unwrap()
            .to_provider(UsbmuxdAddr::default(), HOUSE_ARREST_LABEL);

        let pairing_file = self.get_rsd_pairing_file(&provider, path_to_store).await?;

        let hc = HouseArrestClient::connect(&provider).await?;
        let mut ac = hc.vend_documents(identifier.clone()).await?;
        if let Some(parent) = Path::new(path).parent() {
            let mut current = String::new();
            let has_root = parent.has_root();

            for component in parent.components() {
                if let Component::Normal(dir) = component {
                    if has_root && current.is_empty() {
                        current.push('/');
                    } else if !current.is_empty() && !current.ends_with('/') {
                        current.push('/');
                    }

                    current.push_str(&dir.to_string_lossy());
                    ac.mk_dir(&current).await?;
                }
            }
        }

        let mut f = ac.open(path, AfcFopenMode::Wr).await?;
        f.write_entire(&pairing_file.to_bytes()).await?;

        Ok(())
    }

    async fn get_rsd_pairing_file(
        &self,
        provider: &UsbmuxdProvider,
        path: PathBuf,
    ) -> Result<RpPairingFile, Error> {
        let pairing_file_path = path.join(format!("plume_{}.plist", self.udid));

        if pairing_file_path.exists() {
            return Ok(RpPairingFile::read_from_file(pairing_file_path).await?);
        } else {
            let cdp = CoreDeviceProxy::connect(provider).await?;
            let cdp_port = cdp.tunnel_info().server_rsd_port;
            let cdp_adapter = cdp.create_software_tunnel()?;
            let mut cdp_adapter = cdp_adapter.to_async_handle();

            let cdp_stream = cdp_adapter.connect(cdp_port).await?;
            let cdp_handshake = RsdHandshake::new(cdp_stream).await?;

            let tunnel_service = cdp_handshake
                .services
                .get("com.apple.internal.dt.coredevice.untrusted.tunnelservice")
                .ok_or_else(|| Error::Other("Tunnel service not found".to_string()))?;

            let tunnel_service_stream = cdp_adapter.connect(tunnel_service.port).await?;
            let mut remote_xpc = RemoteXpcClient::new(tunnel_service_stream).await?;
            remote_xpc.do_handshake().await?;
            let _ = remote_xpc.recv_root().await;

            let suffix: String = uuid::Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(6)
                .collect();

            let hostname = format!("plume-{}", suffix);

            let mut pairing_file = RpPairingFile::generate(&hostname);
            let mut pairing_client =
                RemotePairingClient::new(remote_xpc, &hostname, &mut pairing_file);
            pairing_client
                .connect(async |_| "000000".to_string(), ())
                .await?;

            let pairing_file_bytes = pairing_file.to_bytes();

            tokio::fs::write(&pairing_file_path, &pairing_file_bytes).await?;

            Ok(pairing_file)
        }
    }

    async fn connect_apple_tv_pairing_socket(
        endpoint: &AppleTvRemotePairingEndpoint,
        hostname: &str,
        pairing_file: &mut RpPairingFile,
        pin_provider: Option<Arc<dyn Fn() -> String + Send + Sync>>,
    ) -> Result<(), Error> {
        let stream = Self::connect_apple_tv_remote_pairing_endpoint(endpoint).await?;
        let conn = RpPairingSocket::new(stream);
        let mut pairing_client = RemotePairingClient::new(conn, hostname, pairing_file);

        if let Some(pin_provider) = pin_provider {
            pairing_client
                .connect(
                    move |_| {
                        let pin_provider = pin_provider.clone();
                        async move { (pin_provider.as_ref())() }
                    },
                    (),
                )
                .await?;
        } else {
            pairing_client
                .connect(async |_| "000000".to_string(), ())
                .await?;
        }

        Ok(())
    }

    async fn connect_apple_tv_remote_pairing_endpoint(
        endpoint: &AppleTvRemotePairingEndpoint,
    ) -> Result<tokio::net::TcpStream, Error> {
        let mut attempted_targets = HashSet::new();
        let mut errors = Vec::new();

        for target in [&endpoint.host_name, &endpoint.service_address] {
            let target = Self::sanitize_remote_pairing_target(target);
            if target.is_empty() || !attempted_targets.insert(target.clone()) {
                continue;
            }

            match Self::lookup_host_and_connect(&target, endpoint.port).await {
                Ok(stream) => return Ok(stream),
                Err(error) => {
                    errors.push(format!("{target}: {error}"));
                    log::debug!(
                        "Failed to connect to Apple TV remotepairing target {}:{}: {}",
                        target,
                        endpoint.port,
                        error
                    );
                }
            }
        }

        if let Some(device_address) = endpoint.device_address {
            let target = device_address.to_string();
            if attempted_targets.insert(target.clone()) {
                match Self::connect_ip_addr(device_address, endpoint.port).await {
                    Ok(stream) => return Ok(stream),
                    Err(error) => {
                        errors.push(format!("{target}: {error}"));
                        log::debug!(
                            "Failed to connect to Apple TV device address {}:{}: {}",
                            target,
                            endpoint.port,
                            error
                        );
                    }
                }
            }
        }

        Err(Error::Other(format!(
            "Unable to connect to Apple TV {} service '{}' on port {}. Attempts: {}",
            endpoint.service_type,
            endpoint.service_name,
            endpoint.port,
            if errors.is_empty() {
                "no usable targets were available".to_string()
            } else {
                errors.join(" | ")
            }
        )))
    }

    async fn lookup_host_and_connect(
        target: &str,
        port: u16,
    ) -> Result<tokio::net::TcpStream, Error> {
        let target = Self::sanitize_remote_pairing_target(target);
        if let Ok(ip) = target.parse::<IpAddr>() {
            return Self::connect_ip_addr(ip, port).await;
        }

        let mut addrs: Vec<_> = tokio::net::lookup_host((target.as_str(), port))
            .await
            .map_err(|error| {
                Error::Other(format!(
                    "Failed to resolve Apple TV remotepairing endpoint {target}:{port}: {error}"
                ))
            })?
            .collect();

        if addrs.is_empty() {
            return Err(Error::Other(format!(
                "No socket addresses resolved for Apple TV remotepairing endpoint {target}:{port}"
            )));
        }

        // Apple network pairing commonly prefers IPv6/link-local routes when both families exist.
        addrs.sort_by_key(|addr| if addr.is_ipv6() { 0 } else { 1 });

        let mut last_error = None;
        for addr in addrs {
            match addr {
                SocketAddr::V6(addr_v6)
                    if addr_v6.ip().is_unicast_link_local() && addr_v6.scope_id() == 0 =>
                {
                    match Self::connect_link_local_ipv6_with_scopes(*addr_v6.ip(), port).await {
                        Ok(stream) => return Ok(stream),
                        Err(error) => last_error = Some((SocketAddr::V6(addr_v6), error)),
                    }
                }
                _ => match tokio::net::TcpStream::connect(addr).await {
                    Ok(stream) => return Ok(stream),
                    Err(error) => last_error = Some((addr, Error::Io(error))),
                },
            }
        }

        if let Some((addr, error)) = last_error {
            Err(Error::Other(format!(
                "Failed to connect to Apple TV remotepairing endpoint {target}:{port} via {addr}: {error}"
            )))
        } else {
            Err(Error::Other(format!(
                "Unable to connect to Apple TV remotepairing endpoint {target}:{port}"
            )))
        }
    }

    async fn connect_ip_addr(ip: IpAddr, port: u16) -> Result<tokio::net::TcpStream, Error> {
        match ip {
            IpAddr::V6(v6) if v6.is_unicast_link_local() => {
                Self::connect_link_local_ipv6_with_scopes(v6, port).await
            }
            _ => tokio::net::TcpStream::connect(SocketAddr::new(ip, port))
                .await
                .map_err(Error::Io),
        }
    }

    async fn connect_link_local_ipv6_with_scopes(
        ip: Ipv6Addr,
        port: u16,
    ) -> Result<tokio::net::TcpStream, Error> {
        let interfaces = get_if_addrs().map_err(|error| {
            Error::Other(format!(
                "Failed to enumerate local interfaces for Apple TV IPv6 routing: {error}"
            ))
        })?;

        let mut seen_indices = HashSet::new();
        let mut candidates = Vec::new();

        for interface in interfaces {
            let Some(index) = interface.index else {
                continue;
            };

            if !interface.is_oper_up() || interface.is_loopback() || !seen_indices.insert(index) {
                continue;
            }

            if matches!(interface.addr, IfAddr::V6(ref addr) if addr.is_link_local()) {
                candidates.push((interface.name, index));
            }
        }

        if candidates.is_empty() {
            return Err(Error::Other(format!(
                "No active IPv6 link-local interfaces were found for Apple TV address {ip}"
            )));
        }

        let mut last_error = None;
        for (name, index) in candidates {
            let scoped_addr = SocketAddr::V6(SocketAddrV6::new(ip, port, 0, index));

            match tokio::net::TcpStream::connect(scoped_addr).await {
                Ok(stream) => return Ok(stream),
                Err(error) => last_error = Some((name, index, error)),
            }
        }

        if let Some((name, index, error)) = last_error {
            Err(Error::Other(format!(
                "Failed to connect to Apple TV link-local address {ip}%{name} (scope {index}) on port {port}: {error}"
            )))
        } else {
            Err(Error::Other(format!(
                "Unable to connect to Apple TV link-local address {ip} on port {port}"
            )))
        }
    }

    fn discover_remote_pairing_endpoint(
        ip: Option<IpAddr>,
        device_name: &str,
    ) -> Result<AppleTvRemotePairingEndpoint, Error> {
        #[cfg(target_vendor = "apple")]
        match Self::discover_remote_pairing_endpoint_with_dns_sd(ip, device_name) {
            Ok(endpoint) => return Ok(endpoint),
            Err(error) => log::debug!(
                "dns-sd Apple TV pairing discovery failed for {}: {}",
                device_name,
                error
            ),
        }

        if let Ok(endpoint) = Self::discover_remote_pairing_endpoint_with_mdns(
            ip,
            device_name,
            APPLE_TV_MANUAL_PAIRING_SERVICE,
        ) {
            return Ok(endpoint);
        }

        if let Ok(endpoint) = Self::discover_remote_pairing_endpoint_with_mdns(
            ip,
            device_name,
            APPLE_TV_LEGACY_PAIRING_SERVICE,
        ) {
            return Ok(endpoint);
        }

        Err(Error::Other(format!(
            "No matching Apple TV pairing service was found for '{}' ({})",
            device_name,
            ip.map(|ip| ip.to_string())
                .unwrap_or_else(|| "unknown address".to_string())
        )))
    }

    fn discover_remote_pairing_endpoint_with_mdns(
        ip: Option<IpAddr>,
        device_name: &str,
        service_name: &str,
    ) -> Result<AppleTvRemotePairingEndpoint, Error> {
        let target_ip = ip.map(|ip| Self::normalize_network_address(&ip.to_string()));
        let target_name = Self::normalize_service_label(device_name);
        let (tx, rx) = std::sync::mpsc::channel();

        let service = ServiceType::new(service_name, MDNS_SERVICE_PROTOCOL)
            .map_err(|e| Error::Other(format!("Failed to create mDNS service type: {e}")))?;
        let mut browser = MdnsBrowser::new(service);
        let service_name_owned = service_name.to_string();
        browser.set_service_callback(Box::new(move |result, _| {
            if let Ok(BrowserEvent::Add(service)) = result {
                let _ = tx.send(AppleTvRemotePairingEndpoint {
                    service_type: service_name_owned.clone(),
                    service_name: service.name().to_string(),
                    host_name: service.host_name().to_string(),
                    service_address: service.address().to_string(),
                    port: *service.port(),
                    device_address: ip,
                });
            }
        }));

        let event_loop = browser
            .browse_services()
            .map_err(|e| Error::Other(format!("Failed to browse {service_name} services: {e}")))?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut discovered = Vec::new();

        while Instant::now() < deadline {
            event_loop
                .poll(Duration::from_millis(100))
                .map_err(|e| Error::Other(format!("Failed to poll {service_name} services: {e}")))?;

            while let Ok(endpoint) = rx.try_recv() {
                if Self::apple_tv_endpoint_matches_ip(&endpoint, target_ip.as_deref()) {
                    return Ok(endpoint);
                }

                discovered.push(endpoint);
            }
        }

        if let Some(endpoint) = discovered
            .iter()
            .find(|endpoint| Self::apple_tv_endpoint_matches_name(endpoint, &target_name))
            .cloned()
        {
            log::warn!(
                "Matched Apple TV remotepairing service by name instead of IP: {} ({})",
                endpoint.service_name,
                endpoint.service_address
            );
            return Ok(endpoint);
        }

        if discovered.len() == 1 {
            let endpoint = discovered.remove(0);
            log::warn!(
                "Using the only discovered Apple TV remotepairing service: {} ({})",
                endpoint.service_name,
                endpoint.service_address
            );
            return Ok(endpoint);
        }

        Err(Error::Other(format!(
            "No matching _{service_name}._tcp service found for Apple TV '{}' ({})",
            device_name,
            ip.map(|ip| ip.to_string())
                .unwrap_or_else(|| "unknown address".to_string())
        )))
    }

    fn apple_tv_endpoint_matches_ip(
        endpoint: &AppleTvRemotePairingEndpoint,
        target_ip: Option<&str>,
    ) -> bool {
        target_ip.is_some_and(|target_ip| {
            Self::normalize_network_address(&endpoint.service_address) == target_ip
        })
    }

    fn apple_tv_endpoint_matches_name(
        endpoint: &AppleTvRemotePairingEndpoint,
        target_name: &str,
    ) -> bool {
        if target_name.is_empty() {
            return false;
        }

        [
            Self::normalize_service_label(&endpoint.service_name),
            Self::normalize_service_label(&endpoint.host_name),
        ]
        .into_iter()
        .any(|candidate| {
            !candidate.is_empty()
                && (candidate == target_name
                    || candidate.contains(target_name)
                    || target_name.contains(&candidate))
        })
    }

    fn normalize_network_address(address: &str) -> String {
        address
            .trim()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .split('%')
            .next()
            .unwrap_or_default()
            .trim_end_matches('.')
            .to_ascii_lowercase()
    }

    fn normalize_service_label(label: &str) -> String {
        label
            .trim()
            .trim_end_matches('.')
            .to_ascii_lowercase()
            .replace('-', " ")
    }

    fn sanitize_remote_pairing_target(target: &str) -> String {
        target
            .trim()
            .trim_matches('"')
            .trim_start_matches('[')
            .trim_end_matches(']')
            .trim_end_matches('.')
            .to_string()
    }

    #[cfg(target_vendor = "apple")]
    fn discover_remote_pairing_endpoint_with_dns_sd(
        ip: Option<IpAddr>,
        device_name: &str,
    ) -> Result<AppleTvRemotePairingEndpoint, Error> {
        let mut errors = Vec::new();

        for service_name in [
            APPLE_TV_MANUAL_PAIRING_SERVICE,
            APPLE_TV_LEGACY_PAIRING_SERVICE,
        ] {
            match Self::discover_remote_pairing_endpoint_with_dns_sd_service(
                ip,
                device_name,
                service_name,
            ) {
                Ok(endpoint) => return Ok(endpoint),
                Err(error) => errors.push(format!("{service_name}: {error}")),
            }
        }

        Err(Error::Other(format!(
            "dns-sd could not find a matching Apple TV pairing service for '{}'. Attempts: {}",
            device_name,
            errors.join(" | ")
        )))
    }

    #[cfg(target_vendor = "apple")]
    fn discover_remote_pairing_endpoint_with_dns_sd_service(
        ip: Option<IpAddr>,
        device_name: &str,
        service_name: &str,
    ) -> Result<AppleTvRemotePairingEndpoint, Error> {
        let target_ip = ip.map(|ip| Self::normalize_network_address(&ip.to_string()));
        let target_name = Self::normalize_service_label(device_name);
        let service_type = format!("_{service_name}._tcp");
        let browse_output =
            Self::run_dns_sd_command(&["-B", service_type.as_str()], Duration::from_secs(2))?;
        let instances =
            Self::parse_dns_sd_browse_instances(&browse_output, format!("{service_type}.").as_str());
        let mut endpoints = Vec::new();

        for instance in instances {
            let lookup_output = match Self::run_dns_sd_command(
                &["-L", instance.as_str(), service_type.as_str(), "local."],
                Duration::from_secs(2),
            ) {
                Ok(output) => output,
                Err(error) => {
                    log::debug!(
                        "Failed to resolve Apple TV {} instance {}: {}",
                        service_name,
                        instance,
                        error
                    );
                    continue;
                }
            };

            let Some((host_name, port)) = Self::parse_dns_sd_lookup(&lookup_output) else {
                continue;
            };

            let address_output = match Self::run_dns_sd_command(
                &["-G", "v4v6", host_name.as_str()],
                Duration::from_secs(2),
            ) {
                Ok(output) => output,
                Err(error) => {
                    log::debug!(
                        "Failed to resolve Apple TV host {} via dns-sd: {}",
                        host_name,
                        error
                    );
                    String::new()
                }
            };

            let addresses = Self::parse_dns_sd_addresses(&address_output);
            let matched_address = target_ip.as_deref().and_then(|target_ip| {
                addresses
                    .iter()
                    .find(|address| Self::normalize_network_address(address) == target_ip)
                    .cloned()
            });
            let service_address = matched_address
                .clone()
                .or_else(|| addresses.into_iter().find(|address| !address.is_empty()))
                .unwrap_or_else(|| host_name.clone());

            let endpoint = AppleTvRemotePairingEndpoint {
                service_type: service_name.to_string(),
                service_name: instance,
                host_name,
                service_address,
                port,
                device_address: ip,
            };

            if matched_address.is_some() {
                return Ok(endpoint);
            }

            endpoints.push(endpoint);
        }

        let apple_tv_hosts = Self::discover_apple_tv_hosts_with_dns_sd();
        let mut apple_tv_candidates: Vec<_> = endpoints
            .iter()
            .filter(|endpoint| {
                apple_tv_hosts.contains(&Self::normalize_dns_sd_host(&endpoint.host_name))
            })
            .cloned()
            .collect();

        if let Some(endpoint) = apple_tv_candidates
            .iter()
            .find(|endpoint| Self::apple_tv_endpoint_matches_name(endpoint, &target_name))
            .cloned()
            .or_else(|| {
                if apple_tv_candidates.len() == 1 {
                    Some(apple_tv_candidates.remove(0))
                } else {
                    None
                }
            })
        {
            return Ok(endpoint);
        }

        if let Some(endpoint) = endpoints
            .iter()
            .find(|endpoint| Self::apple_tv_endpoint_matches_name(endpoint, &target_name))
            .cloned()
        {
            return Ok(endpoint);
        }

        if endpoints.len() == 1 {
            return Ok(endpoints.remove(0));
        }

        Err(Error::Other(format!(
            "dns-sd could not find a matching Apple TV {service_name} service for '{}'",
            device_name
        )))
    }

    #[cfg(target_vendor = "apple")]
    fn run_dns_sd_command(args: &[&str], capture_for: Duration) -> Result<String, Error> {
        let mut child = Command::new("/usr/bin/dns-sd")
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        thread::sleep(capture_for);
        let _ = child.kill();

        let output = child.wait_with_output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{stdout}\n{stderr}");

        if combined.trim().is_empty() {
            return Err(Error::Other(format!(
                "dns-sd returned no output for arguments {:?}",
                args
            )));
        }

        Ok(combined)
    }

    #[cfg(target_vendor = "apple")]
    fn parse_dns_sd_browse_instances(output: &str, service_type: &str) -> Vec<String> {
        let mut instances = Vec::new();
        let mut seen = HashSet::new();

        for line in output.lines() {
            if !line.contains(service_type) || !line.contains(" Add ") {
                continue;
            }

            let Some(instance) = line.split_whitespace().last() else {
                continue;
            };

            if seen.insert(instance.to_string()) {
                instances.push(instance.to_string());
            }
        }

        instances
    }

    #[cfg(target_vendor = "apple")]
    fn parse_dns_sd_lookup(output: &str) -> Option<(String, u16)> {
        for line in output.lines() {
            let marker = " can be reached at ";
            let Some((_, rest)) = line.split_once(marker) else {
                continue;
            };
            let endpoint = rest.split(" (interface ").next()?.trim();
            let Some((host_name, port)) = endpoint.rsplit_once(':') else {
                continue;
            };
            let port = port.parse().ok()?;
            return Some((Self::sanitize_remote_pairing_target(host_name), port));
        }

        None
    }

    #[cfg(target_vendor = "apple")]
    fn discover_apple_tv_hosts_with_dns_sd() -> HashSet<String> {
        let browse_output =
            match Self::run_dns_sd_command(&["-B", "_airplay._tcp"], Duration::from_secs(2)) {
                Ok(output) => output,
                Err(error) => {
                    log::debug!("Failed to browse AirPlay services via dns-sd: {}", error);
                    return HashSet::new();
                }
            };

        let instances = Self::parse_dns_sd_browse_instances(&browse_output, "_airplay._tcp.");
        let mut hosts = HashSet::new();

        for instance in instances {
            let lookup_output = match Self::run_dns_sd_command(
                &["-L", instance.as_str(), "_airplay._tcp", "local."],
                Duration::from_secs(2),
            ) {
                Ok(output) => output,
                Err(error) => {
                    log::debug!(
                        "Failed to resolve AirPlay service {} via dns-sd: {}",
                        instance,
                        error
                    );
                    continue;
                }
            };

            let Some(model) = Self::parse_dns_sd_txt_value(&lookup_output, "model") else {
                continue;
            };
            if !model.starts_with("AppleTV") {
                continue;
            }

            if let Some((host_name, _)) = Self::parse_dns_sd_lookup(&lookup_output) {
                hosts.insert(Self::normalize_dns_sd_host(&host_name));
            }
        }

        hosts
    }

    #[cfg(target_vendor = "apple")]
    fn parse_dns_sd_txt_value(output: &str, key: &str) -> Option<String> {
        for line in output.lines() {
            for token in line.split_whitespace() {
                let Some((candidate_key, value)) = token.split_once('=') else {
                    continue;
                };
                if candidate_key == key {
                    return Some(value.trim_matches('"').to_string());
                }
            }
        }

        None
    }

    #[cfg(target_vendor = "apple")]
    fn parse_dns_sd_addresses(output: &str) -> Vec<String> {
        let mut addresses = Vec::new();
        let mut seen = HashSet::new();

        for line in output.lines() {
            if !line.contains(" Add ") {
                continue;
            }

            let columns: Vec<&str> = line.split_whitespace().collect();
            if columns.len() < 2 {
                continue;
            }

            let address = Self::sanitize_remote_pairing_target(columns[columns.len() - 2]);
            if !address.is_empty() && seen.insert(address.clone()) {
                addresses.push(address);
            }
        }

        addresses
    }

    #[cfg(target_vendor = "apple")]
    fn normalize_dns_sd_host(host: &str) -> String {
        Self::sanitize_remote_pairing_target(host).to_ascii_lowercase()
    }

    pub async fn install_app<F, Fut>(
        &self,
        app_path: &PathBuf,
        progress_callback: F,
    ) -> Result<(), Error>
    where
        F: FnMut(i32) -> Fut + Send + Clone + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        if self.usbmuxd_device.is_none() {
            return Err(Error::Other("Device is not connected via USB".to_string()));
        }

        let provider = self.usbmuxd_device.clone().unwrap().to_provider(
            UsbmuxdAddr::from_env_var().unwrap_or_default(),
            INSTALLATION_LABEL,
        );

        let callback = move |(progress, _): (u64, ())| {
            let mut cb = progress_callback.clone();
            async move {
                cb(progress as i32).await;
            }
        };

        let state = ();

        installation::install_package_with_callback(&provider, app_path, None, callback, state)
            .await?;

        Ok(())
    }
}

fn get_app_name_from_info(info: &Value) -> Option<String> {
    let dict = info.as_dictionary()?;
    dict.get("CFBundleDisplayName")
        .and_then(|value| value.as_string())
        .or_else(|| dict.get("CFBundleName").and_then(|value| value.as_string()))
        .or_else(|| {
            dict.get("CFBundleExecutable")
                .and_then(|value| value.as_string())
        })
        .map(|value| value.to_string())
}

impl fmt::Display for Device {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {}",
            match &self.usbmuxd_device {
                Some(device) => match &device.connection_type {
                    Connection::Usb => "USB",
                    Connection::Network(_) => "WiFi",
                    Connection::Unknown(_) => "Unknown",
                },
                None => "LOCAL",
            },
            self.name
        )
    }
}

pub async fn get_device_for_id(device_id: &str) -> Result<Device, Error> {
    let mut usbmuxd = UsbmuxdConnection::default().await?;
    let usbmuxd_device = usbmuxd
        .get_devices()
        .await?
        .into_iter()
        .find(|d| d.device_id.to_string() == device_id)
        .ok_or_else(|| Error::Other(format!("Device ID {device_id} not found")))?;

    Ok(Device::new(usbmuxd_device).await)
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub async fn install_app_mac(app_path: &PathBuf) -> Result<(), Error> {
    use crate::copy_dir_recursively;
    use std::env;
    use tokio::fs;
    use uuid::Uuid;

    let stage_dir = env::temp_dir().join(format!(
        "plume_mac_stage_{}",
        Uuid::new_v4().to_string().to_uppercase()
    ));
    let app_name = app_path
        .file_name()
        .ok_or(Error::Other("Invalid app path".to_string()))?;

    // iOS Apps on macOS need to be wrapped in a special structure, more specifically
    // ```
    // LiveContainer.app
    // ├── WrappedBundle -> Wrapper/LiveContainer.app
    // └── Wrapper
    //     └── LiveContainer.app
    // ```
    // Then install to /Applications/...

    let outer_app_dir = stage_dir.join(app_name);
    let wrapper_dir = outer_app_dir.join("Wrapper");

    fs::create_dir_all(&wrapper_dir).await?;

    copy_dir_recursively(app_path, &wrapper_dir.join(app_name)).await?;

    let wrapped_bundle_path = outer_app_dir.join("WrappedBundle");
    fs::symlink(
        PathBuf::from("Wrapper").join(app_name),
        &wrapped_bundle_path,
    )
    .await?;

    let applications_dir = PathBuf::from("/Applications/iOS");
    fs::create_dir_all(&applications_dir).await?;

    let applications_dir = applications_dir.join(app_name);

    fs::remove_dir_all(&applications_dir).await.ok();

    fs::rename(&outer_app_dir, &applications_dir)
        .await
        .map_err(|_| Error::BundleFailedToCopy(applications_dir.to_string_lossy().into_owned()))?;

    Ok(())
}

#[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
pub async fn install_app_mac(_app_path: &PathBuf) -> Result<(), Error> {
    Ok(())
}
