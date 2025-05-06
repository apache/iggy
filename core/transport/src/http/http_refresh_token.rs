use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct RefreshToken {
    token: String,
}

impl RefreshToken {
    pub fn new(token: String) -> Self {
        Self { token }
    }
}
