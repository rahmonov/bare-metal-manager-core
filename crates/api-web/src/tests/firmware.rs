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

use axum::body::Body;
use axum::response::Response;
use http_body_util::BodyExt;
use hyper::http::{Method, StatusCode};
use model::firmware::FirmwareComponentType;
use sqlx::types::Json;
use tower::ServiceExt;

use crate::tests::env::TestEnv;
use crate::tests::{make_test_app, web_request_builder};

async fn response_body(response: Response) -> String {
    let body_bytes = response
        .into_body()
        .collect()
        .await
        .expect("empty response body")
        .to_bytes();

    String::from_utf8(body_bytes.to_vec()).expect("invalid UTF-8 in response body")
}

async fn insert_desired_firmware(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"
            INSERT INTO desired_firmware (
                vendor,
                model,
                versions,
                explicit_update_start_needed
            )
            VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind("Dell")
    .bind("PowerEdge R760")
    .bind(Json(serde_json::json!({
        "Versions": {
            "bmc": "1.2.3",
            "uefi": "4.5.6"
        }
    })))
    .bind(true)
    .execute(pool)
    .await
    .expect("insert desired firmware row");
}

#[crate::sqlx_test]
async fn firmware_page_shows_desired_firmware_table(pool: sqlx::PgPool) {
    let env = TestEnv::new(pool.clone()).await;
    insert_desired_firmware(&pool).await;
    let app = make_test_app(&env.test_harness);

    let response = app
        .oneshot(
            web_request_builder()
                .uri("/admin/firmware")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body(response).await;
    assert!(body.contains("Desired Firmware"));
    assert!(body.contains("Host Firmware Config Overrides"));
    assert!(body.find("Desired Firmware") < body.find("Host Firmware Config Overrides"));
    assert!(body.contains(r#"action="/admin/firmware#host_firmware_config""#));
    assert!(body.contains(r#"Vendor <span aria-label="required">*</span>"#));
    assert!(body.contains(r#"Model <span aria-label="required">*</span>"#));
    assert!(body.contains(r#"<select id="host-firmware-vendor" name="vendor" required>"#));
    for vendor in [
        "Dell",
        "Lenovo",
        "LenovoAMI",
        "Supermicro",
        "Hpe",
        "Nvidia",
        "Liteon",
        "Delta",
    ] {
        assert!(
            body.contains(&format!(r#"<option value="{vendor}">{vendor}</option>"#)),
            "missing vendor option {vendor}"
        );
    }
    assert!(!body.contains(r#"<option value="Unknown">Unknown</option>"#));
    assert!(body.contains(
        r#"<input id="host-firmware-model" type="text" name="model" list="host-firmware-model-options" autocomplete="off" required>"#
    ));
    assert!(body.contains(r#"<option value="PowerEdge R760" data-vendor="Dell"></option>"#));
    assert!(body.contains(r#"Component <span aria-label="required">*</span>"#));
    assert!(body.contains(r#"Version <span aria-label="required">*</span>"#));
    assert!(body.contains(r#"Artifact URL <span aria-label="required">*</span>"#));
    assert!(body.contains("Optional 64-character hex SHA-256 digest"));
    assert!(body.contains("multi-component packages install only that component"));
    assert!(body.contains("Run the required reset sequence"));
    assert!(body.contains(r#"value="Upsert""#));
    assert!(!body.contains("Upsert Host Firmware Config"));
    assert!(body.contains("Dell"));
    assert!(body.contains("PowerEdge R760"));
    assert!(body.contains("true"));
    assert!(body.contains("<b>BMC</b>: 1.2.3"));
    assert!(body.contains("<b>UEFI</b>: 4.5.6"));
    assert!(body.contains("1.2.3"));
    assert!(body.contains("4.5.6"));
}

#[crate::sqlx_test]
async fn firmware_json_preserves_versions_as_json(pool: sqlx::PgPool) {
    let env = TestEnv::new(pool.clone()).await;
    insert_desired_firmware(&pool).await;
    let app = make_test_app(&env.test_harness);

    let response = app
        .oneshot(
            web_request_builder()
                .uri("/admin/firmware.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&response_body(response).await).expect("valid JSON response");
    let row = rows
        .iter()
        .find(|row| row["vendor"] == "Dell" && row["model"] == "PowerEdge R760")
        .expect("inserted desired firmware row");

    assert_eq!(row["explicit_update_start_needed"], true);
    assert_eq!(row["versions"]["Versions"]["bmc"], "1.2.3");
    assert_eq!(row["versions"]["Versions"]["uefi"], "4.5.6");
}

#[crate::sqlx_test]
async fn firmware_form_upserts_host_firmware_config(pool: sqlx::PgPool) {
    let env = TestEnv::new(pool.clone()).await;
    let app = make_test_app(&env.test_harness);

    let response = app
        .oneshot(
            web_request_builder()
                .method(Method::POST)
                .uri("/admin/firmware")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "vendor=Dell&model=PowerEdge%20R760&component_type=1&version=7.10.30.00&artifact_url=https%3A%2F%2Ffirmware.example.invalid%2Fidrac.exe&artifact_sha256=&explicit_start_needed=true&preingest_upgrade_when_below=&default=on",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    let mut txn = pool.begin().await.unwrap();
    let row = db::host_firmware_config::get(&mut txn, "Dell", "PowerEdge R760")
        .await
        .unwrap()
        .expect("host firmware config row");
    let config = row.into_config();

    assert_eq!(config.explicit_start_needed, Some(true));
    assert_eq!(config.ordering, vec![FirmwareComponentType::Bmc]);
    let bmc = config
        .components
        .get(&FirmwareComponentType::Bmc)
        .expect("BMC component");
    let firmware = bmc
        .known_firmware
        .iter()
        .find(|firmware| firmware.version == "7.10.30.00")
        .expect("firmware version");
    assert!(firmware.default);
    assert_eq!(
        firmware.files.first().and_then(|file| file.url.as_deref()),
        Some("https://firmware.example.invalid/idrac.exe")
    );
    txn.commit().await.unwrap();
}
