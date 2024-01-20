use crate::bytes_serializable::BytesSerializable;
use crate::command::CommandPayload;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::users::defaults::*;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::from_utf8;

/// `ChangePassword` command is used to change a user's password.
/// It has additional payload:
/// - `user_id` - unique user ID (numeric or name).
/// - `current_password` - current password, must be between 3 and 100 characters long.
/// - `new_password` - new password, must be between 3 and 100 characters long.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ChangePassword {
    /// Unique user ID (numeric or name).
    #[serde(skip)]
    pub user_id: Identifier,
    /// Current password, must be between 3 and 100 characters long.
    pub current_password: String,
    /// New password, must be between 3 and 100 characters long.
    pub new_password: String,
}

impl CommandPayload for ChangePassword {}

impl Default for ChangePassword {
    fn default() -> Self {
        ChangePassword {
            user_id: Identifier::default(),
            current_password: "secret".to_string(),
            new_password: "topsecret".to_string(),
        }
    }
}

impl Validatable<IggyError> for ChangePassword {
    fn validate(&self) -> Result<(), IggyError> {
        if self.current_password.is_empty()
            || self.current_password.len() > MAX_PASSWORD_LENGTH
            || self.current_password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        if self.new_password.is_empty()
            || self.new_password.len() > MAX_PASSWORD_LENGTH
            || self.new_password.len() < MIN_PASSWORD_LENGTH
        {
            return Err(IggyError::InvalidPassword);
        }

        Ok(())
    }
}

impl BytesSerializable for ChangePassword {
    fn as_bytes(&self) -> Vec<u8> {
        let user_id_bytes = self.user_id.as_bytes();
        let mut bytes = Vec::new();
        bytes.extend(user_id_bytes);
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.current_password.len() as u8);
        bytes.extend(self.current_password.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(self.new_password.len() as u8);
        bytes.extend(self.new_password.as_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<ChangePassword, IggyError> {
        if bytes.len() < 9 {
            return Err(IggyError::InvalidCommand);
        }

        let user_id = Identifier::from_bytes(bytes)?;
        let mut position = user_id.get_size_bytes() as usize;
        let current_password_length = bytes[position];
        position += 1;
        let current_password =
            from_utf8(&bytes[position..position + current_password_length as usize])?.to_string();
        position += current_password_length as usize;
        let new_password_length = bytes[position];
        position += 1;
        let new_password =
            from_utf8(&bytes[position..position + new_password_length as usize])?.to_string();

        let command = ChangePassword {
            user_id,
            current_password,
            new_password,
        };
        command.validate()?;
        Ok(command)
    }
}

impl Display for ChangePassword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}",
            self.user_id, self.current_password, self.new_password
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = ChangePassword {
            user_id: Identifier::numeric(1).unwrap(),
            current_password: "user".to_string(),
            new_password: "secret".to_string(),
        };

        let bytes = command.as_bytes();
        let user_id = Identifier::from_bytes(&bytes).unwrap();
        let mut position = user_id.get_size_bytes() as usize;
        let current_password_length = bytes[position];
        position += 1;
        let current_password =
            from_utf8(&bytes[position..position + current_password_length as usize]).unwrap();
        position += current_password_length as usize;
        let new_password_length = bytes[position];
        position += 1;
        let new_password =
            from_utf8(&bytes[position..position + new_password_length as usize]).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(user_id, command.user_id);
        assert_eq!(current_password, command.current_password);
        assert_eq!(new_password, command.new_password);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let user_id = Identifier::numeric(1).unwrap();
        let current_password = "secret";
        let new_password = "topsecret";
        let mut bytes = Vec::new();
        bytes.extend(user_id.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(current_password.len() as u8);
        bytes.extend(current_password.as_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u8(new_password.len() as u8);
        bytes.extend(new_password.as_bytes());

        let command = ChangePassword::from_bytes(&bytes);
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(command.user_id, user_id);
        assert_eq!(command.current_password, current_password);
        assert_eq!(command.new_password, new_password);
    }
}
