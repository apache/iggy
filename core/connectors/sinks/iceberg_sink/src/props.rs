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
    props.insert(
        "s3.access-key-id".to_string(),
        config.store_access_key_id.clone(),
    );
    props.insert(
        "s3.secret-access-key".to_string(),
        config.store_secret_access_key.clone(),
    );
    props.insert("s3.endpoint".to_string(), config.store_url.clone());
    Ok(props)
}
