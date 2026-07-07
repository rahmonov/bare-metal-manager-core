/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;
use std::sync::Arc;

use askama::Template;
use axum::Form;
use axum::Json;
use axum::extract::{Query, State as AxumState};
use axum::response::{Html, IntoResponse, Redirect, Response};
use bmc_vendor::BMCVendor;
use carbide_api_core::Api;
use clap::ValueEnum;
use hyper::http::StatusCode;
use model::firmware::DesiredFirmwareVersions;
use rpc::forge as forgerpc;
use rpc::forge::forge_server::Forge;
use rpc::model::firmware::firmware_component_type_from_rpc;
use serde::Deserialize;
use sqlx::types::Json as SqlxJson;

use super::Base;
use crate::action_status::{self, ActionStatus};

const DESIRED_FIRMWARE_QUERY: &str = r#"
    SELECT vendor, model, versions, explicit_update_start_needed
    FROM desired_firmware
    ORDER BY vendor, model
"#;

#[derive(Template)]
#[template(path = "firmware_show.html")]
struct FirmwareShow<'a> {
    action_status: Option<ActionStatus<'a>>,
    vendor_options: Vec<String>,
    component_type_options: Vec<ComponentTypeOption>,
    host_firmware_configs: Vec<HostFirmwareConfigDisplay>,
    desired_firmware: Vec<DesiredFirmwareDisplay>,
}

struct ComponentTypeOption {
    value: i32,
    label: &'static str,
}

struct HostFirmwareConfigDisplay {
    vendor: String,
    model: String,
    explicit_start_needed: String,
    ordering: String,
    updated_at: String,
    components: Vec<HostFirmwareComponentDisplay>,
}

struct HostFirmwareComponentDisplay {
    component: String,
    versions: String,
    preingest_upgrade_when_below: String,
}

struct DesiredFirmwareDisplay {
    vendor: String,
    model: String,
    versions: Vec<FirmwareVersionDisplay>,
    explicit_update_start_needed: bool,
}

struct FirmwareVersionDisplay {
    component: String,
    version: String,
}

#[derive(serde::Serialize)]
struct DesiredFirmware {
    vendor: String,
    model: String,
    versions: DesiredFirmwareVersions,
    explicit_update_start_needed: bool,
}

#[derive(sqlx::FromRow)]
struct DesiredFirmwareRow {
    vendor: String,
    model: String,
    versions: SqlxJson<DesiredFirmwareVersions>,
    explicit_update_start_needed: bool,
}

#[derive(Deserialize)]
pub struct HostFirmwareConfigForm {
    vendor: String,
    model: String,
    component_type: i32,
    version: String,
    artifact_url: String,
    artifact_sha256: Option<String>,
    explicit_start_needed: String,
    preingest_upgrade_when_below: Option<String>,
    default: Option<String>,
    install_only_specified: Option<String>,
    pre_update_resets: Option<String>,
}

pub async fn show_html(
    AxumState(state): AxumState<Arc<Api>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let desired_firmware = match fetch_desired_firmware(&state).await {
        Ok(rows) => rows,
        Err(err) => {
            tracing::error!(%err, "fetch desired firmware");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error loading desired firmware",
            )
                .into_response();
        }
    };
    let host_firmware_configs = match fetch_host_firmware_configs(&state).await {
        Ok(rows) => rows,
        Err(err) => {
            tracing::error!(%err, "fetch host firmware config overrides");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error loading host firmware config overrides",
            )
                .into_response();
        }
    };

    let tmpl = FirmwareShow {
        action_status: ActionStatus::from_query(&params),
        vendor_options: host_firmware_vendor_options(),
        component_type_options: component_type_options(),
        host_firmware_configs,
        desired_firmware: desired_firmware.iter().map(Into::into).collect(),
    };
    (StatusCode::OK, Html(tmpl.render().unwrap())).into_response()
}

pub async fn upsert_host_firmware_config(
    AxumState(state): AxumState<Arc<Api>>,
    Form(form): Form<HostFirmwareConfigForm>,
) -> Response {
    let redirect_url = "/admin/firmware#host_firmware_config";
    let ordering = match ordering_for_upsert(&state, &form).await {
        Ok(ordering) => ordering,
        Err(message) => {
            return firmware_config_redirect(action_status::Class::Error, message, redirect_url);
        }
    };

    let request = forgerpc::UpsertHostFirmwareConfigRequest {
        vendor: form.vendor.trim().to_string(),
        model: form.model.trim().to_string(),
        components: vec![forgerpc::UpsertHostFirmwareComponentConfig {
            r#type: form.component_type,
            firmware: vec![forgerpc::HostFirmwareVersionConfig {
                version: form.version.trim().to_string(),
                default: form.default.as_deref() == Some("on"),
                artifacts: vec![forgerpc::HostFirmwareArtifact {
                    url: form.artifact_url.trim().to_string(),
                    sha256: optional_string(form.artifact_sha256.as_deref()),
                }],
                install_only_specified: form.install_only_specified.as_deref() == Some("on"),
                power_drains_needed: None,
                pre_update_resets: form.pre_update_resets.as_deref() == Some("on"),
                preingestion_exclusive_config: None,
            }],
            preingest_upgrade_when_below: optional_string(
                form.preingest_upgrade_when_below.as_deref(),
            ),
        }],
        explicit_start_needed: explicit_start_needed(&form.explicit_start_needed),
        ordering,
    };

    match state
        .upsert_host_firmware_config(tonic::Request::new(request))
        .await
    {
        Ok(response) => {
            let response = response.into_inner();
            firmware_config_redirect(
                action_status::Class::Success,
                format!(
                    "Updated host firmware config for {} {}",
                    response.vendor, response.model
                ),
                redirect_url,
            )
        }
        Err(err) => {
            tracing::error!(%err, "upsert host firmware config");
            firmware_config_redirect(
                action_status::Class::Error,
                err.message().to_string(),
                redirect_url,
            )
        }
    }
}

pub async fn show_json(AxumState(state): AxumState<Arc<Api>>) -> Response {
    let desired_firmware = match fetch_desired_firmware(&state).await {
        Ok(rows) => rows,
        Err(err) => {
            tracing::error!(%err, "fetch desired firmware");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error loading desired firmware",
            )
                .into_response();
        }
    };

    (StatusCode::OK, Json(desired_firmware)).into_response()
}

async fn fetch_desired_firmware(api: &Api) -> Result<Vec<DesiredFirmware>, sqlx::Error> {
    sqlx::query_as::<_, DesiredFirmwareRow>(DESIRED_FIRMWARE_QUERY)
        .fetch_all(&api.database_connection)
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

async fn fetch_host_firmware_configs(
    api: &Api,
) -> Result<Vec<HostFirmwareConfigDisplay>, db::DatabaseError> {
    db::host_firmware_config::list(&api.database_connection)
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

async fn ordering_for_upsert(api: &Api, form: &HostFirmwareConfigForm) -> Result<Vec<i32>, String> {
    let selected_component = firmware_component_type_from_rpc(form.component_type)
        .map_err(|err| format!("invalid component type: {err}"))?;
    let selected_component = forgerpc::HostFirmwareComponentType::from(selected_component) as i32;

    let mut ordering = db::host_firmware_config::list(&api.database_connection)
        .await
        .map_err(|err| format!("failed to load existing host firmware configs: {err}"))?
        .into_iter()
        .find(|row| {
            row.vendor.eq_ignore_ascii_case(form.vendor.trim())
                && row.model.eq_ignore_ascii_case(form.model.trim())
        })
        .map(|row| {
            row.config
                .0
                .ordering
                .into_iter()
                .map(|component_type| {
                    forgerpc::HostFirmwareComponentType::from(component_type) as i32
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if !ordering.contains(&selected_component) {
        ordering.push(selected_component);
    }

    Ok(ordering)
}

fn firmware_config_redirect(
    class: action_status::Class,
    message: impl Into<String>,
    redirect_url: &str,
) -> Response {
    let message = message.into();
    let redirect_url = ActionStatus {
        action: action_status::Type::HostFirmwareConfig,
        class,
        message: message.into(),
    }
    .update_redirect_url(redirect_url);
    Redirect::to(&redirect_url).into_response()
}

impl From<DesiredFirmwareRow> for DesiredFirmware {
    fn from(row: DesiredFirmwareRow) -> Self {
        Self {
            vendor: row.vendor,
            model: row.model,
            versions: row.versions.0,
            explicit_update_start_needed: row.explicit_update_start_needed,
        }
    }
}

impl From<db::host_firmware_config::HostFirmwareConfigRow> for HostFirmwareConfigDisplay {
    fn from(row: db::host_firmware_config::HostFirmwareConfigRow) -> Self {
        let updated_at = row.updated_at.format("%F %T %Z").to_string();
        let config = row.into_config();

        let mut components = config
            .components
            .into_iter()
            .map(|(component_type, component)| HostFirmwareComponentDisplay {
                component: component_type.to_string(),
                versions: component
                    .known_firmware
                    .into_iter()
                    .map(|firmware| {
                        if firmware.default {
                            format!("{} (default)", firmware.version)
                        } else {
                            firmware.version
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                preingest_upgrade_when_below: component
                    .preingest_upgrade_when_below
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();
        components.sort_unstable_by(|left, right| left.component.cmp(&right.component));

        Self {
            vendor: config.vendor.to_pascalcase(),
            model: config.model,
            explicit_start_needed: config
                .explicit_start_needed
                .map(|value| value.to_string())
                .unwrap_or_else(|| "Preserve".to_string()),
            ordering: config
                .ordering
                .into_iter()
                .map(|component_type| component_type.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            updated_at,
            components,
        }
    }
}

impl From<&DesiredFirmware> for DesiredFirmwareDisplay {
    fn from(row: &DesiredFirmware) -> Self {
        Self {
            vendor: row.vendor.clone(),
            model: row.model.clone(),
            versions: display_versions(&row.versions),
            explicit_update_start_needed: row.explicit_update_start_needed,
        }
    }
}

fn display_versions(versions: &DesiredFirmwareVersions) -> Vec<FirmwareVersionDisplay> {
    let mut versions = versions
        .versions
        .iter()
        .map(|(component_type, version)| FirmwareVersionDisplay {
            component: component_type.to_string(),
            version: version.clone(),
        })
        .collect::<Vec<_>>();
    versions.sort_unstable_by(|left, right| left.component.cmp(&right.component));
    versions
}

fn component_type_options() -> Vec<ComponentTypeOption> {
    vec![
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::Bmc as i32,
            label: "BMC",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::Uefi as i32,
            label: "UEFI",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::CombinedBmcUefi as i32,
            label: "BMC+UEFI",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::Nic as i32,
            label: "NIC",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::Cx7 as i32,
            label: "CX7",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::HgxBmc as i32,
            label: "HGX BMC",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::Gpu as i32,
            label: "GPU",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::Cec as i32,
            label: "CEC",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::CpldMb as i32,
            label: "CPLD MB",
        },
        ComponentTypeOption {
            value: forgerpc::HostFirmwareComponentType::CpldPdb as i32,
            label: "CPLD PDB",
        },
    ]
}

fn host_firmware_vendor_options() -> Vec<String> {
    let mut vendors = BMCVendor::value_variants()
        .iter()
        .copied()
        .filter(|vendor| *vendor != BMCVendor::Unknown)
        .map(BMCVendor::to_pascalcase)
        .collect::<Vec<_>>();
    vendors.sort_unstable();
    vendors
}

fn optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn explicit_start_needed(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

impl Base for FirmwareShow<'_> {}
