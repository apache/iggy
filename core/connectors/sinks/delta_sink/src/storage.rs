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

use crate::DeltaSinkConfig;
use iggy_connector_sdk::Error;
use std::collections::HashMap;

pub(crate) fn build_storage_options(
    config: &DeltaSinkConfig,
) -> Result<HashMap<String, String>, Error> {
    let mut opts = HashMap::new();

    match config.storage_backend_type.as_deref() {
        Some("s3") => {
            let access_key = config.aws_s3_access_key.as_ref().ok_or(Error::InvalidConfig)?;
            let secret_key = config.aws_s3_secret_key.as_ref().ok_or(Error::InvalidConfig)?;
            let region = config.aws_s3_region.as_ref().ok_or(Error::InvalidConfig)?;
            let endpoint_url = config.aws_s3_endpoint_url.as_ref().ok_or(Error::InvalidConfig)?;
            let allow_http = config.aws_s3_allow_http.ok_or(Error::InvalidConfig)?;

            opts.insert("AWS_ACCESS_KEY_ID".into(), access_key.clone());
            opts.insert("AWS_SECRET_ACCESS_KEY".into(), secret_key.clone());
            opts.insert("AWS_REGION".into(), region.clone());
            opts.insert("AWS_ENDPOINT_URL".into(), endpoint_url.clone());
            opts.insert("AWS_ALLOW_HTTP".into(), allow_http.to_string());
            opts.insert("AWS_S3_ALLOW_HTTP".into(), allow_http.to_string());
        }
        Some("azure") => {
            let account_name = config.azure_storage_account_name.as_ref().ok_or(Error::InvalidConfig)?;
            let account_key = config.azure_storage_account_key.as_ref().ok_or(Error::InvalidConfig)?;
            let sas_token = config.azure_storage_sas_token.as_ref().ok_or(Error::InvalidConfig)?;
            let container_name = config.azure_container_name.as_ref().ok_or(Error::InvalidConfig)?;

            opts.insert("AZURE_STORAGE_ACCOUNT_NAME".into(), account_name.clone());
            opts.insert("AZURE_STORAGE_ACCOUNT_KEY".into(), account_key.clone());
            opts.insert("AZURE_STORAGE_SAS_TOKEN".into(), sas_token.clone());
            opts.insert("AZURE_CONTAINER_NAME".into(), container_name.clone());
        }
        Some("gcs") => {
            let service_account_key = config.gcs_service_account_key.as_ref().ok_or(Error::InvalidConfig)?;
            let bucket = config.gcs_bucket.as_ref().ok_or(Error::InvalidConfig)?;

            opts.insert(
                "GOOGLE_SERVICE_ACCOUNT_KEY".into(),
                service_account_key.clone(),
            );
            opts.insert("GCS_BUCKET".into(), bucket.clone());
        }
        Some(_) => {
            return Err(Error::InvalidConfig);
        }
        None => {}
    }

    Ok(opts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DeltaSinkConfig;

    fn default_config() -> DeltaSinkConfig {
        DeltaSinkConfig {
            table_uri: "file:///tmp/test".into(),
            schema: vec![],
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
            gcs_bucket: None,
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
            storage_backend_type: Some("s3".into()),
            aws_s3_access_key: Some("AKID".into()),
            aws_s3_secret_key: Some("SECRET".into()),
            aws_s3_region: Some("us-east-1".into()),
            aws_s3_endpoint_url: Some("http://localhost:9000".into()),
            aws_s3_allow_http: Some(true),
            ..default_config()
        }
    }

    fn azure_config() -> DeltaSinkConfig {
        DeltaSinkConfig {
            storage_backend_type: Some("azure".into()),
            azure_storage_account_name: Some("myaccount".into()),
            azure_storage_account_key: Some("mykey".into()),
            azure_storage_sas_token: Some("mysas".into()),
            azure_container_name: Some("mycontainer".into()),
            ..default_config()
        }
    }

    fn gcs_config() -> DeltaSinkConfig {
        DeltaSinkConfig {
            storage_backend_type: Some("gcs".into()),
            gcs_service_account_key: Some("{\"key\": \"value\"}".into()),
            gcs_bucket: Some("mybucket".into()),
            ..default_config()
        }
    }

    #[test]
    fn s3_backend_maps_all_fields() {
        let opts = build_storage_options(&s3_config()).unwrap();
        assert_eq!(opts.get("AWS_ACCESS_KEY_ID").unwrap(), "AKID");
        assert_eq!(opts.get("AWS_SECRET_ACCESS_KEY").unwrap(), "SECRET");
        assert_eq!(opts.get("AWS_REGION").unwrap(), "us-east-1");
        assert_eq!(opts.get("AWS_ENDPOINT_URL").unwrap(), "http://localhost:9000");
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
    fn s3_backend_missing_endpoint_url_errors() {
        let mut config = s3_config();
        config.aws_s3_endpoint_url = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn s3_backend_missing_allow_http_errors() {
        let mut config = s3_config();
        config.aws_s3_allow_http = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn azure_backend_maps_all_fields() {
        let opts = build_storage_options(&azure_config()).unwrap();
        assert_eq!(opts.get("AZURE_STORAGE_ACCOUNT_NAME").unwrap(), "myaccount");
        assert_eq!(opts.get("AZURE_STORAGE_ACCOUNT_KEY").unwrap(), "mykey");
        assert_eq!(opts.get("AZURE_STORAGE_SAS_TOKEN").unwrap(), "mysas");
        assert_eq!(opts.get("AZURE_CONTAINER_NAME").unwrap(), "mycontainer");
    }

    #[test]
    fn azure_backend_missing_account_name_errors() {
        let mut config = azure_config();
        config.azure_storage_account_name = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn azure_backend_missing_account_key_errors() {
        let mut config = azure_config();
        config.azure_storage_account_key = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn azure_backend_missing_sas_token_errors() {
        let mut config = azure_config();
        config.azure_storage_sas_token = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn azure_backend_missing_container_name_errors() {
        let mut config = azure_config();
        config.azure_container_name = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn gcs_backend_maps_all_fields() {
        let opts = build_storage_options(&gcs_config()).unwrap();
        assert_eq!(
            opts.get("GOOGLE_SERVICE_ACCOUNT_KEY").unwrap(),
            "{\"key\": \"value\"}"
        );
        assert_eq!(opts.get("GCS_BUCKET").unwrap(), "mybucket");
    }

    #[test]
    fn gcs_backend_missing_service_account_key_errors() {
        let mut config = gcs_config();
        config.gcs_service_account_key = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn gcs_backend_missing_bucket_errors() {
        let mut config = gcs_config();
        config.gcs_bucket = None;
        assert!(build_storage_options(&config).is_err());
    }

    #[test]
    fn unknown_backend_type_errors() {
        let config = DeltaSinkConfig {
            storage_backend_type: Some("unknown".into()),
            ..default_config()
        };
        assert!(build_storage_options(&config).is_err());
    }
}
