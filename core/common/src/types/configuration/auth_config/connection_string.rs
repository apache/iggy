use crate::{AutoLogin, ConnectionStringOptions};

const CONNECTION_STRING_PREFIX: &str = "iggy://";

#[derive(Debug)]
pub struct ConnectionString {
    server_address: String,
    auto_login: AutoLogin,
    options: ConnectionStringOptions,
}

impl ConnectionString {
    pub fn new(
        server_address: String,
        auto_login: AutoLogin,
        options: ConnectionStringOptions,
    ) -> Self {
        Self {
            server_address,
            auto_login,
            options,
        }
    }
}

impl ConnectionString {
    pub fn server_address(&self) -> &str {
        &self.server_address
    }

    pub fn auto_login(&self) -> &AutoLogin {
        &self.auto_login
    }

    pub fn options(&self) -> &ConnectionStringOptions {
        &self.options
    }
}
