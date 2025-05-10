#[derive(Debug, Clone, PartialEq)]
pub enum Credentials {
    UsernamePassword(String, String),
    PersonalAccessToken(String),
}
