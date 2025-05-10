use crate::Credentials;

#[derive(Debug, Clone, PartialEq)]
pub enum AutoLogin {
    Disabled,
    Enabled(Credentials),
}
