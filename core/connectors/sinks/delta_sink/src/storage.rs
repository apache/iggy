/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::{DeltaSinkConfig, StorageBackendType};
use iggy_connector_sdk::Error;
use secrecy::ExposeSecret;
use std::collections::HashMap;

pub(crate) fn build_storage_options(
    config: &DeltaSinkConfig,
) -> Result<HashMap<String, String>, Error> {
    let mut opts = HashMap::new();

    match config.storage_backend_type {
        Some(StorageBackendType::S3) => {
            let access_key = config.aws_s3_access_key.as_ref().ok_or_else(|| {
                Error::InitError("S3 backend requires 'aws_s3_access_key'".into())
            })?;
            let secret_key = config.aws_s3_secret_key.as_ref().ok_or_else(|| {
                Error::InitError("S3 backend requires 'aws_s3_secret_key'".into())
            })?;
            let region = config
                .aws_s3_region
                .as_ref()
                .ok_or_else(|| Error::InitError("S3 backend requires 'aws_s3_region'".into()))?;

            opts.insert("AWS_ACCESS_KEY_ID".into(), access_key.clone());
            opts.insert(
                "AWS_SECRET_ACCESS_KEY".into(),
                secret_key.expose_secret().to_owned(),
            );
            opts.insert("AWS_REGION".into(), region.clone());

            if let Some(endpoint_url) = config.aws_s3_endpoint_url.as_ref() {
                opts.insert("AWS_ENDPOINT_URL".into(), endpoint_url.clone());
            }
            if let Some(allow_http) = config.aws_s3_allow_http {
                opts.insert("AWS_ALLOW_HTTP".into(), allow_http.to_string());
                opts.insert("AWS_S3_ALLOW_HTTP".into(), allow_http.to_string());
            }
        }
        Some(StorageBackendType::Azure) => {
            let account_name = config.azure_storage_account_name.as_ref().ok_or_else(|| {
                Error::InitError("Azure backend requires 'azure_storage_account_name'".into())
            })?;
            let container_name = config.azure_container_name.as_ref().ok_or_else(|| {
                Error::InitError("Azure backend requires 'azure_container_name'".into())
            })?;

            opts.insert("AZURE_STORAGE_ACCOUNT_NAME".into(), account_name.clone());
            opts.insert("AZURE_CONTAINER_NAME".into(), container_name.clone());

            match (
                config.azure_storage_account_key.as_ref(),
                config.azure_storage_sas_token.as_ref(),
            ) {
                (Some(key), None) => {
                    opts.insert(
                        "AZURE_STORAGE_ACCOUNT_KEY".into(),
                        key.expose_secret().to_owned(),
                    );
                }
                (None, Some(sas)) => {
                    opts.insert(
                        "AZURE_STORAGE_SAS_TOKEN".into(),
                        sas.expose_secret().to_owned(),
                    );
                }
                (Some(_), Some(_)) => {
                    return Err(Error::InitError("Azure backend requires exactly one of 'azure_storage_account_key' or 'azure_storage_sas_token', but both were provided".into()));
                }
                (None, None) => {
                    return Err(Error::InitError("Azure backend requires one of 'azure_storage_account_key' or 'azure_storage_sas_token'".into()));
                }
            }
        }
        Some(StorageBackendType::Gcs) => {
            let service_account_key = config.gcs_service_account_key.as_ref().ok_or_else(|| {
                Error::InitError("GCS backend requires 'gcs_service_account_key'".into())
            })?;

            opts.insert(
                "GOOGLE_SERVICE_ACCOUNT_KEY".into(),
                service_account_key.expose_secret().to_owned(),
            );
        }
        None => {}
    }

    Ok(opts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeltaSinkConfig, StorageBackendType};

    fn default_config() -> DeltaSinkConfig {
        DeltaSinkConfig {
            table_uri: "file:///tmp/test".into(),
            storage_backend_type: None,
            aws_s3_access_key: None,
            aws_s3_secret_key: None,
            aws_s3_region: None,
            aws_s3_endpoint_url: None,
            aws_s3_allow_http: None,
            azure_storage_account_name: None,
            azure_storage_account_key: None,
            azure_storage_sas_token: None,
            azure_container_name: None,
            gcs_service_account_key: None,
        }
    }

    #[test]
    fn no_backend_type_returns_empty() {
        let config = default_config();
        let opts = build_storage_options(&config).unwrap();
        assert!(opts.is_empty());
    }

    fn s3_config() -> DeltaSinkConfig {
        DeltaSinkConfig {
            storage_backend_type: Some(StorageBackendType::S3),
            aws_s3_access_key: Some("AKID".into()),
            aws_s3_secret_key: Some("SECRET".to_string().into()),
            aws_s3_region: Some("us-east-1".into()),
            aws_s3_endpoint_url: Some("http://localhost:9000".into()),
            aws_s3_allow_http: Some(true),
            ..default_config()
        }
    }

    fn azure_config_with_key() -> DeltaSinkConfig {
        DeltaSinkConfig {
            storage_backend_type: Some(StorageBackendType::Azure),
            azure_storage_account_name: Some("myaccount".into()),
            azure_storage_account_key: Some("mykey".to_string().into()),
            azure_storage_sas_token: None,
            azure_container_name: Some("mycontainer".into()),
            ..default_config()
        }
    }

    fn azure_config_with_sas() -> DeltaSinkConfig {
        DeltaSinkConfig {
            storage_backend_type: Some(StorageBackendType::Azure),
            azure_storage_account_name: Some("myaccount".into()),
            azure_storage_account_key: None,
            azure_storage_sas_token: Some("mysas".to_string().into()),
            azure_container_name: Some("mycontainer".into()),
            ..default_config()
        }
    }

    fn gcs_config() -> DeltaSinkConfig {
        DeltaSinkConfig {
            storage_backend_type: Some(StorageBackendType::Gcs),
            gcs_service_account_key: Some("{\"key\": \"value\"}".to_string().into()),
            ..default_config()
        }
    }

    #[test]
    fn s3_backend_maps_all_fields() {
        let opts = build_storage_options(&s3_config()).unwrap();
        assert_eq!(opts.get("AWS_ACCESS_KEY_ID").unwrap(), "AKID");
        assert_eq!(opts.get("AWS_SECRET_ACCESS_KEY").unwrap(), "SECRET");
        assert_eq!(opts.get("AWS_REGION").unwrap(), "us-east-1");
        assert_eq!(
            opts.get("AWS_ENDPOINT_URL").unwrap(),
            "http://localhost:9000"
        );
        assert_eq!(opts.get("AWS_ALLOW_HTTP").unwrap(), "true");
        assert_eq!(opts.get("AWS_S3_ALLOW_HTTP").unwrap(), "true");
    }

    #[test]
    fn s3_backend_missing_access_key_errors() {
        let mut config = s3_config();
        config.aws_s3_access_key = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn s3_backend_missing_secret_key_errors() {
        let mut config = s3_config();
        config.aws_s3_secret_key = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn s3_backend_missing_region_errors() {
        let mut config = s3_config();
        config.aws_s3_region = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn s3_backend_without_endpoint_url_succeeds() {
        let mut config = s3_config();
        config.aws_s3_endpoint_url = None;
        let opts = build_storage_options(&config).unwrap();
        assert!(!opts.contains_key("AWS_ENDPOINT_URL"));
    }

    #[test]
    fn s3_backend_without_allow_http_succeeds() {
        let mut config = s3_config();
        config.aws_s3_allow_http = None;
        let opts = build_storage_options(&config).unwrap();
        assert!(!opts.contains_key("AWS_ALLOW_HTTP"));
        assert!(!opts.contains_key("AWS_S3_ALLOW_HTTP"));
    }

    #[test]
    fn azure_backend_maps_account_key() {
        let opts = build_storage_options(&azure_config_with_key()).unwrap();
        assert_eq!(opts.get("AZURE_STORAGE_ACCOUNT_NAME").unwrap(), "myaccount");
        assert_eq!(opts.get("AZURE_STORAGE_ACCOUNT_KEY").unwrap(), "mykey");
        assert!(!opts.contains_key("AZURE_STORAGE_SAS_TOKEN"));
        assert_eq!(opts.get("AZURE_CONTAINER_NAME").unwrap(), "mycontainer");
    }

    #[test]
    fn azure_backend_maps_sas_token() {
        let opts = build_storage_options(&azure_config_with_sas()).unwrap();
        assert_eq!(opts.get("AZURE_STORAGE_ACCOUNT_NAME").unwrap(), "myaccount");
        assert_eq!(opts.get("AZURE_STORAGE_SAS_TOKEN").unwrap(), "mysas");
        assert!(!opts.contains_key("AZURE_STORAGE_ACCOUNT_KEY"));
        assert_eq!(opts.get("AZURE_CONTAINER_NAME").unwrap(), "mycontainer");
    }

    #[test]
    fn azure_backend_both_auth_methods_errors() {
        let config = DeltaSinkConfig {
            azure_storage_account_key: Some("mykey".to_string().into()),
            azure_storage_sas_token: Some("mysas".to_string().into()),
            ..azure_config_with_key()
        };
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn azure_backend_no_auth_method_errors() {
        let config = DeltaSinkConfig {
            azure_storage_account_key: None,
            azure_storage_sas_token: None,
            ..azure_config_with_key()
        };
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn azure_backend_missing_account_name_errors() {
        let mut config = azure_config_with_key();
        config.azure_storage_account_name = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn azure_backend_missing_container_name_errors() {
        let mut config = azure_config_with_key();
        config.azure_container_name = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn gcs_backend_maps_service_account_key() {
        let opts = build_storage_options(&gcs_config()).unwrap();
        assert_eq!(
            opts.get("GOOGLE_SERVICE_ACCOUNT_KEY").unwrap(),
            "{\"key\": \"value\"}"
        );
    }

    #[test]
    fn gcs_backend_missing_service_account_key_errors() {
        let mut config = gcs_config();
        config.gcs_service_account_key = None;
        assert!(build_storage_options(&config).is_err());
    }
}
