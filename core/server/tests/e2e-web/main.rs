// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod utils;

use crate::utils::TestCtx;
use fantoccini::Locator;

async fn test_redirects_unauthenticated_user_to_sign_in(TestCtx { client, url }: TestCtx) {
    // we are going to the Iggy Web UI dashboard, but ...
    client.goto(url.as_str()).await.unwrap();
    let _sign_in_form = client.wait_for_element(Locator::Css("form")).await.unwrap();
    // ... get redirected (client-side) to the sign-in page
    let current_url = client.current_url().await.unwrap();
    let auth_url = url.join("ui/auth/sign-in").unwrap();
    assert_eq!(current_url, auth_url);
}

mod tests {
    crate::test!(test_redirects_unauthenticated_user_to_sign_in);
}
