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

use super::{Error, IcebergSinkConfig, IcebergSinkStoreClass};
use std::collections::HashMap;

pub fn init_props(config: &IcebergSinkConfig) -> Result<HashMap<String, String>, Error> {
    match config.store_class {
        IcebergSinkStoreClass::S3 => Ok(get_props_s3(config)?),
        _ => Err(Error::InvalidConfig),
    }
}

#[inline(always)]
fn get_props_s3(config: &IcebergSinkConfig) -> Result<HashMap<String, String>, Error> {
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("s3.region".to_string(), config.store_region.clone());
    props.insert("s3.endpoint".to_string(), config.store_url.clone());
    match (&config.store_access_key_id, &config.store_secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            props.insert("s3.access-key-id".to_string(), access_key_id.clone());
            props.insert(
                "s3.secret-access-key".to_string(),
                secret_access_key.clone(),
            );
        }
        (None, None) => {}
        _ => {
            return Err(Error::InvalidConfigValue("Partially configured credentials. You must provide both store_access_key_id and store_secret_access_key, or omit both.".to_owned()));
        }
    }
    Ok(props)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IcebergSinkConfig, IcebergSinkStoreClass, IcebergSinkTypes};

    fn base_config() -> IcebergSinkConfig {
        IcebergSinkConfig {
            tables: vec![],
            catalog_type: IcebergSinkTypes::REST,
            warehouse: "warehouse".to_string(),
            uri: "http://localhost:8181".to_string(),
            dynamic_routing: false,
            dynamic_route_field: "".to_string(),
            store_url: "http://localhost:9000".to_string(),
            store_access_key_id: None,
            store_secret_access_key: None,
            store_region: "us-east-1".to_string(),
            store_class: IcebergSinkStoreClass::S3,
        }
    }

    #[test]
    fn test_get_props_s3_no_credentials() {
        let config = base_config();
        let props = get_props_s3(&config).expect("Should succeed without credentials");
        assert_eq!(props.get("s3.region").unwrap(), "us-east-1");
        assert_eq!(props.get("s3.endpoint").unwrap(), "http://localhost:9000");
        assert!(!props.contains_key("s3.access-key-id"));
        assert!(!props.contains_key("s3.secret-access-key"));
    }

    #[test]
    fn test_get_props_s3_full_credentials() {
        let config = IcebergSinkConfig {
            store_access_key_id: Some("admin".to_string()),
            store_secret_access_key: Some("password".to_string()),
            ..base_config()
        };
        let props = get_props_s3(&config).expect("Should succeed with full credentials");
        assert_eq!(props.get("s3.region").unwrap(), "us-east-1");
        assert_eq!(props.get("s3.endpoint").unwrap(), "http://localhost:9000");
        assert_eq!(props.get("s3.access-key-id").unwrap(), "admin");
        assert_eq!(props.get("s3.secret-access-key").unwrap(), "password");
    }

    #[test]
    fn test_get_props_s3_partial_access_key() {
        let config = IcebergSinkConfig {
            store_access_key_id: Some("admin".to_string()),
            store_secret_access_key: None,
            ..base_config()
        };
        assert!(
            get_props_s3(&config).is_err(),
            "Partial credentials (only access_key_id) should be rejected"
        );
    }

    #[test]
    fn test_get_props_s3_partial_secret_key() {
        let config = IcebergSinkConfig {
            store_access_key_id: None,
            store_secret_access_key: Some("password".to_string()),
            ..base_config()
        };
        assert!(
            get_props_s3(&config).is_err(),
            "Partial credentials (only secret_access_key) should be rejected"
        );
    }
}
