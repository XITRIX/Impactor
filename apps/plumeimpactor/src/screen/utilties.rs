use iced::widget::{
    button, column, container, operation as widget_operation, row, rule, scrollable, stack, text,
    text_input, toggler,
};
use iced::{Center, Color, Element, Fill, Task};
use rust_i18n::t;

use crate::appearance;
use crate::defaults::get_data_path;
use futures::channel::oneshot;
use plume_utils::{Device, SignerAppReal};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};

const APPLE_TV_PIN_INPUT_ID: &str = "apple_tv_pin_input";

#[derive(Debug, Clone)]
struct StatusMessage {
    text: String,
    is_error: bool,
}

impl StatusMessage {
    fn success(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            is_error: false,
        }
    }

    fn error(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            is_error: true,
        }
    }

    fn color(&self) -> Color {
        if self.is_error {
            Color::from_rgb(0.9, 0.2, 0.2)
        } else {
            Color::from_rgb(0.2, 0.8, 0.4)
        }
    }
}

#[allow(unused)]
#[derive(Debug, Clone)]
pub enum Message {
    RefreshApps(bool),
    AppsLoaded(Result<Vec<SignerAppReal>, String>),
    InstallPairingFile(SignerAppReal),
    Trust,
    ExportAppleTvPairingFile,
    FocusAppleTvPin,
    AppleTvPinChanged(String),
    SubmitAppleTvPin,
    PairResult(Result<(), String>),
    ExportAppleTvPairingResult(Result<bool, String>),
    InstallPairingResult(String, Result<(), String>),
    ToggleRPPairing(bool),
}

#[derive(Debug, Clone)]
struct AppleTvPinPrompt {
    input: String,
    submitted: bool,
    error: Option<String>,
    pin_state: Arc<(Mutex<Option<String>>, Condvar)>,
}

impl AppleTvPinPrompt {
    fn new(pin_state: Arc<(Mutex<Option<String>>, Condvar)>) -> Self {
        Self {
            input: String::new(),
            submitted: false,
            error: None,
            pin_state,
        }
    }
}

#[derive(Debug, Clone)]
pub struct UtilitiesScreen {
    device: Option<Device>,
    installed_apps: Vec<SignerAppReal>,
    status_message: Option<StatusMessage>,
    app_statuses: HashMap<String, StatusMessage>,
    loading: bool,
    trust_loading: bool,
    pub rppairing_enabled: bool,
    apple_tv_pin_prompt: Option<AppleTvPinPrompt>,
}

impl UtilitiesScreen {
    pub fn new(device: Option<Device>) -> Self {
        let mut screen = Self {
            device,
            installed_apps: Vec::new(),
            status_message: None,
            app_statuses: HashMap::new(),
            loading: false,
            trust_loading: false,
            rppairing_enabled: false,
            apple_tv_pin_prompt: None,
        };

        if screen.device.as_ref().map(|d| d.is_mac).unwrap_or(false) {
            screen.status_message = Some(StatusMessage::error(t!(
                "utilities_mac_devices_not_supported"
            )));
        }

        screen
    }

    pub fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::RefreshApps(_) => {
                self.loading = true;
                self.status_message = None;
                self.app_statuses.clear();
                if let Some(device) = &self.device {
                    if device.is_mac {
                        self.loading = false;
                        self.installed_apps.clear();
                        return Task::none();
                    }

                    if device.supports_apple_tv_pairing() {
                        self.loading = false;
                        self.installed_apps.clear();
                        return Task::none();
                    }

                    let device = device.clone();
                    let rx = spawn_runtime_task(async move {
                        device
                            .installed_apps()
                            .await
                            .map_err(|e| format!("Failed to load apps: {}", e))
                    });

                    Task::perform(
                        async move {
                            rx.await
                                .unwrap_or_else(|_| Err("Failed to receive result".to_string()))
                        },
                        Message::AppsLoaded,
                    )
                } else {
                    Task::done(Message::AppsLoaded(Err("No device connected".to_string())))
                }
            }
            Message::AppsLoaded(result) => {
                self.loading = false;
                match result {
                    Ok(apps) => {
                        self.installed_apps = apps;
                        self.status_message = None;
                    }
                    Err(e) => {
                        self.status_message = Some(StatusMessage::error(e));
                        self.installed_apps.clear();
                    }
                }
                Task::none()
            }
            Message::InstallPairingFile(app) => {
                if let Some(device) = &self.device {
                    let device = device.clone();
                    let bundle_id = app.bundle_id.clone().unwrap_or_default();
                    let pairing_path = app.app.pairing_file_path().unwrap_or_default();
                    let app_key = Self::app_key(&app);

                    let rppairing_enabled = self.rppairing_enabled;

                    let rx = spawn_runtime_task(async move {
                        if rppairing_enabled {
                            device
                                .install_remote_pairing_record(
                                    &bundle_id,
                                    &pairing_path,
                                    get_data_path(),
                                )
                                .await
                                .map_err(|e| {
                                    format!("Failed to install remote pairing record: {}", e)
                                })
                        } else {
                            device
                                .install_pairing_record(&bundle_id, &pairing_path)
                                .await
                                .map_err(|e| format!("Failed to install pairing record: {}", e))
                        }
                    });

                    Task::perform(
                        async move {
                            rx.await
                                .unwrap_or_else(|_| Err("Failed to receive result".to_string()))
                        },
                        move |result| Message::InstallPairingResult(app_key, result),
                    )
                } else {
                    Task::none()
                }
            }
            Message::Trust => {
                self.trust_loading = true;
                self.status_message = None;
                if let Some(device) = &self.device {
                    let device = device.clone();

                    let pair_task = if device.can_attempt_remote_pairing() {
                        let pin_state = Arc::new((Mutex::new(None), Condvar::new()));
                        self.apple_tv_pin_prompt = Some(AppleTvPinPrompt::new(pin_state.clone()));

                        let rx = spawn_runtime_task(async move {
                            device
                                .pair_apple_tv(get_data_path(), move || {
                                    wait_for_pin_submission(pin_state.clone())
                                })
                                .await
                                .map_err(|e| format!("Failed to pair Apple TV: {}", e))
                        });

                        Task::perform(
                            async move {
                                rx.await.unwrap_or_else(|_| {
                                    Err("Failed to receive pairing result".to_string())
                                })
                            },
                            Message::PairResult,
                        )
                    } else {
                        self.apple_tv_pin_prompt = None;

                        let rx = spawn_runtime_task(async move {
                            device
                                .pair()
                                .await
                                .map_err(|e| format!("Failed to pair: {}", e))
                        });

                        Task::perform(
                            async move {
                                rx.await.unwrap_or_else(|_| {
                                    Err("Failed to receive pairing result".to_string())
                                })
                            },
                            Message::PairResult,
                        )
                    };

                    if self.apple_tv_pin_prompt.is_some() {
                        Task::batch([pair_task, Task::done(Message::FocusAppleTvPin)])
                    } else {
                        pair_task
                    }
                } else {
                    self.trust_loading = false;
                    Task::none()
                }
            }
            Message::ExportAppleTvPairingFile => {
                let Some(pairing_file_path) = self.apple_tv_pairing_file_path() else {
                    self.status_message = Some(StatusMessage::error(t!(
                        "utilities_apple_tv_pairing_file_missing"
                    )));
                    return Task::none();
                };

                if !pairing_file_path.is_file() {
                    self.status_message = Some(StatusMessage::error(t!(
                        "utilities_apple_tv_pairing_file_missing"
                    )));
                    return Task::none();
                }

                self.status_message = None;

                Task::perform(
                    async move {
                        let file = rfd::AsyncFileDialog::new()
                            .set_title(t!("utilities_export_pairing"))
                            .add_filter("Property List", &["plist"])
                            .set_file_name(
                                pairing_file_path
                                    .file_name()
                                    .and_then(|name| name.to_str())
                                    .unwrap_or("apple_tv_pairing.plist"),
                            )
                            .save_file()
                            .await;

                        if let Some(save_path) = file {
                            std::fs::copy(&pairing_file_path, save_path.path()).map_err(
                                |error| {
                                    format!("Failed to export Apple TV pairing file: {error}")
                                },
                            )?;
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    },
                    Message::ExportAppleTvPairingResult,
                )
            }
            Message::FocusAppleTvPin => {
                if self.apple_tv_pin_prompt.is_some() {
                    Task::batch([
                        widget_operation::focus(APPLE_TV_PIN_INPUT_ID),
                        widget_operation::move_cursor_to_end(APPLE_TV_PIN_INPUT_ID),
                    ])
                } else {
                    Task::none()
                }
            }
            Message::AppleTvPinChanged(pin) => {
                if let Some(prompt) = self.apple_tv_pin_prompt.as_mut()
                    && !prompt.submitted
                {
                    prompt.input = pin;
                    prompt.error = None;
                }
                Task::none()
            }
            Message::SubmitAppleTvPin => {
                if let Some(prompt) = self.apple_tv_pin_prompt.as_mut() {
                    let pin = prompt.input.trim().to_string();
                    if pin.is_empty() {
                        prompt.error = Some(t!("utilities_apple_tv_pin_required").to_string());
                    } else {
                        let (lock, cvar) = &*prompt.pin_state;
                        if let Ok(mut state) = lock.lock() {
                            *state = Some(pin);
                            cvar.notify_all();
                            prompt.submitted = true;
                            prompt.error = None;
                        } else {
                            prompt.error =
                                Some(t!("utilities_apple_tv_pin_submit_failed").to_string());
                        }
                    }
                }
                Task::none()
            }
            Message::PairResult(result) => {
                self.trust_loading = false;
                self.apple_tv_pin_prompt = None;
                match result {
                    Ok(_) => {
                        self.status_message =
                            Some(StatusMessage::success(t!("utilities_paired_success")));
                    }
                    Err(e) => {
                        self.status_message = Some(StatusMessage::error(e));
                    }
                }
                Task::none()
            }
            Message::ExportAppleTvPairingResult(result) => {
                match result {
                    Ok(true) => {
                        self.status_message = Some(StatusMessage::success(t!(
                            "utilities_apple_tv_pairing_exported"
                        )));
                    }
                    Ok(false) => {}
                    Err(error) => {
                        self.status_message = Some(StatusMessage::error(error));
                    }
                }
                Task::none()
            }
            Message::InstallPairingResult(app_key, result) => {
                let status = match result {
                    Ok(_) => StatusMessage::success(t!("utilities_device_paired_success")),
                    Err(e) => StatusMessage::error(e),
                };
                self.app_statuses.insert(app_key, status);
                Task::none()
            }
            Message::ToggleRPPairing(enabled) => {
                self.rppairing_enabled = enabled;
                Task::perform(async move { Message::RefreshApps(enabled) }, |msg| msg)
            }
        }
    }

    pub fn view(&self) -> Element<'_, Message> {
        let mut content = column![].spacing(appearance::THEME_PADDING);
        let is_apple_tv = self
            .device
            .as_ref()
            .is_some_and(Device::supports_apple_tv_pairing);

        if let Some(ref device) = self.device {
            let mut device_details = column![
                text(format!("Name: {}", device.name)),
                text(format!("UDID: {}", device.udid)),
            ]
            .spacing(4);

            if let Some(product_type) = &device.product_type {
                device_details = device_details.push(text(format!("Product Type: {product_type}")));
            }
            if let Some(ip) = device.network_address() {
                device_details = device_details.push(text(format!("IP: {ip}")));
            }

            content = content.push(device_details);
        } else {
            content = content.push(
                text(t!("utilities_no_device_connected")).color(Color::from_rgb(0.7, 0.7, 0.7)),
            );
        }

        if let Some(ref status) = self.status_message {
            content = content.push(text(&status.text).size(14).color(status.color()));
        }

        if self.device.is_some() && !self.device.as_ref().unwrap().is_mac {
            let refresh_button_text = if self.loading {
                t!("utilities_loading")
            } else {
                t!("utilities_refresh_installed_apps")
            };

            let trust_button_text = if self.trust_loading {
                t!("utilities_pairing")
            } else if is_apple_tv {
                t!("utilities_pair_apple_tv")
            } else {
                t!("utilities_trust_device")
            };

            if is_apple_tv {
                let pair_button = button(text(trust_button_text).align_x(Center))
                    .on_press_maybe(if self.trust_loading {
                        None
                    } else {
                        Some(Message::Trust)
                    })
                    .style(appearance::s_button)
                    .width(Fill);

                let pairing_actions = if self.has_apple_tv_pairing_file() {
                    row![
                        pair_button,
                        button(text(t!("utilities_export_pairing")).align_x(Center))
                            .on_press(Message::ExportAppleTvPairingFile)
                            .style(appearance::s_button)
                            .width(Fill),
                    ]
                    .spacing(appearance::THEME_PADDING)
                } else {
                    row![pair_button].spacing(appearance::THEME_PADDING)
                };

                content = content
                    .push(text(t!("utilities_apple_tv_pairing_notice")).size(13))
                    .push(pairing_actions);
            } else {
                content = content.push(
                    row![
                        button(text(trust_button_text).align_x(Center))
                            .on_press_maybe(if self.trust_loading {
                                None
                            } else {
                                Some(Message::Trust)
                            })
                            .style(appearance::s_button)
                            .width(Fill),
                        button(text(refresh_button_text).align_x(Center))
                            .on_press_maybe(if self.loading {
                                None
                            } else {
                                Some(Message::RefreshApps(self.rppairing_enabled))
                            })
                            .style(appearance::s_button)
                            .width(Fill),
                    ]
                    .spacing(appearance::THEME_PADDING),
                );
            }
        }

        if !is_apple_tv {
            let toggle = toggler(self.rppairing_enabled)
                .label(t!("utilities_use_remote_pairing"))
                .on_toggle(Message::ToggleRPPairing);

            content = content.push(toggle);
        }

        if !is_apple_tv && !self.installed_apps.is_empty() {
            content = content
                .push(container(rule::horizontal(1)).padding([appearance::THEME_PADDING, 0.0]));

            let mut apps_list = column![].spacing(4);

            for app in &self.installed_apps {
                let app_key = Self::app_key(app);
                let mut app_row = column![
                    row![
                        text(format!(
                            "{} ({})",
                            app.app.to_string(),
                            app.bundle_id.clone().unwrap_or("???".to_string())
                        ))
                        .size(14)
                        .width(iced::Length::Fill),
                        button(text(t!("utilities_install_pairing")).align_x(Center))
                            .on_press(Message::InstallPairingFile(app.clone()))
                            .style(appearance::s_button)
                    ]
                    .spacing(appearance::THEME_PADDING)
                    .align_y(Center)
                ]
                .spacing(4);

                if let Some(status) = self.app_statuses.get(&app_key) {
                    app_row = app_row.push(text(&status.text).size(13).color(status.color()));
                }

                apps_list = apps_list.push(app_row);
            }

            content = content.push(apps_list);
        }

        let base: Element<'_, Message> = container(scrollable(content)).into();

        if self.apple_tv_pin_prompt.is_some() {
            stack![base, self.view_apple_tv_pin_prompt()].into()
        } else {
            base
        }
    }

    fn app_key(app: &SignerAppReal) -> String {
        app.bundle_id.clone().unwrap_or_else(|| app.app.to_string())
    }

    pub fn set_device(&mut self, device: Option<Device>) {
        self.device = device;
        if self.device.as_ref().map(|d| d.is_mac).unwrap_or(false) {
            self.status_message = Some(StatusMessage::error(t!(
                "utilities_mac_devices_not_supported"
            )));
        }
    }

    pub fn pairing_in_progress(&self) -> bool {
        self.trust_loading || self.apple_tv_pin_prompt.is_some()
    }

    pub fn selected_device_id(&self) -> Option<u32> {
        self.device.as_ref().map(|device| device.device_id)
    }

    fn apple_tv_pairing_file_path(&self) -> Option<PathBuf> {
        let device = self.device.as_ref()?;
        if !device.supports_apple_tv_pairing() {
            return None;
        }

        Some(
            get_data_path()
                .join("appletv_pairing")
                .join(format!("plume_{}.plist", device.udid.replace(':', "_"))),
        )
    }

    fn has_apple_tv_pairing_file(&self) -> bool {
        self.apple_tv_pairing_file_path()
            .is_some_and(|path| path.is_file())
    }

    fn view_apple_tv_pin_prompt(&self) -> Element<'_, Message> {
        let Some(prompt) = &self.apple_tv_pin_prompt else {
            return container(text("")).into();
        };
        let pin_placeholder = t!("utilities_apple_tv_pin_placeholder");

        let mut pin_input = text_input(pin_placeholder.as_ref(), &prompt.input)
            .id(APPLE_TV_PIN_INPUT_ID)
            .on_input(Message::AppleTvPinChanged)
            .padding(8)
            .width(Fill);
        if !prompt.submitted {
            pin_input = pin_input.on_submit(Message::SubmitAppleTvPin);
        }

        let submit_label = if prompt.submitted {
            t!("utilities_apple_tv_waiting")
        } else {
            t!("utilities_apple_tv_submit_pin")
        };

        let mut dialog_content = column![
            text(t!("utilities_pair_apple_tv")).size(appearance::THEME_FONT_SIZE + 2.0),
            text(t!("utilities_apple_tv_pin_prompt")).size(14),
            pin_input,
            button(text(submit_label))
                .on_press_maybe(if prompt.submitted {
                    None
                } else {
                    Some(Message::SubmitAppleTvPin)
                })
                .style(appearance::p_button),
        ]
        .spacing(appearance::THEME_PADDING);

        if let Some(error) = &prompt.error {
            dialog_content = dialog_content.push(text(error).color(Color::from_rgb(0.9, 0.2, 0.2)));
        }

        let dialog = container(dialog_content)
            .padding(appearance::THEME_PADDING * 2.0)
            .max_width(420.0)
            .style(|theme: &iced::Theme| container::Style {
                background: Some(iced::Background::Color(theme.palette().background)),
                border: iced::Border {
                    width: 1.0,
                    color: theme.palette().primary,
                    radius: appearance::THEME_CORNER_RADIUS.into(),
                },
                ..Default::default()
            });

        container(dialog)
            .width(Fill)
            .height(Fill)
            .center(Fill)
            .style(|_theme: &iced::Theme| container::Style {
                background: Some(iced::Background::Color(Color {
                    r: 0.0,
                    g: 0.0,
                    b: 0.0,
                    a: 0.45,
                })),
                ..Default::default()
            })
            .into()
    }
}

fn spawn_runtime_task<T, Fut>(future: Fut) -> oneshot::Receiver<T>
where
    T: Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(future);
        let _ = tx.send(result);
    });

    rx
}

fn wait_for_pin_submission(pin_state: Arc<(Mutex<Option<String>>, Condvar)>) -> String {
    let (lock, cvar) = &*pin_state;
    let mut guard = lock.lock().unwrap();

    loop {
        if let Some(pin) = guard.take() {
            return pin;
        }

        guard = cvar.wait(guard).unwrap();
    }
}
