use crate::streaming::users::user::User;
use anyhow::Context;
use iggy::error::IggyError;
use iggy::models::user_info::UserId;
use sled::Db;
use std::sync::Arc;

const KEY_PREFIX: &str = "users";

#[derive(Debug)]
pub struct FileUserStorage {
    db: Arc<Db>,
}

impl FileUserStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

unsafe impl Send for FileUserStorage {}
unsafe impl Sync for FileUserStorage {}

impl FileUserStorage {
    pub async fn load_by_id(&self, id: UserId) -> Result<User, IggyError> {
        let mut user = User::empty(id);
        self.load(&mut user).await?;
        Ok(user)
    }

    pub async fn load_by_username(&self, username: &str) -> Result<User, IggyError> {
        let user_id_key = get_id_key(username);
        let user_id = self.db.get(&user_id_key).with_context(|| {
            format!(
                "Failed to load user with key: {}, username: {}",
                user_id_key, username
            )
        });
        match user_id {
            Ok(user_id) => {
                if let Some(user_id) = user_id {
                    let user_id = u32::from_le_bytes(user_id.as_ref().try_into()?);
                    let mut user = User::empty(user_id);
                    self.load(&mut user).await?;
                    Ok(user)
                } else {
                    Err(IggyError::ResourceNotFound(user_id_key))
                }
            }
            Err(err) => Err(IggyError::CannotLoadResource(err)),
        }
    }

    pub async fn load_all(&self) -> Result<Vec<User>, IggyError> {
        let mut users = Vec::new();
        for data in self.db.scan_prefix(format!("{}:", KEY_PREFIX)) {
            let user = match data.with_context(|| {
                format!(
                    "Failed to load user, when searching for key: {}",
                    KEY_PREFIX
                )
            }) {
                Ok((_, value)) => match rmp_serde::from_slice::<User>(&value).with_context(|| {
                    format!(
                        "Failed to deserialize user, when searching for key: {}",
                        KEY_PREFIX
                    )
                }) {
                    Ok(user) => user,
                    Err(err) => {
                        return Err(IggyError::CannotDeserializeResource(err));
                    }
                },
                Err(err) => {
                    return Err(IggyError::CannotLoadResource(err));
                }
            };
            users.push(user);
        }

        Ok(users)
    }

    pub async fn load(&self, user: &mut User) -> Result<(), IggyError> {
        let key = get_key(user.id);
        let user_data = match self.db.get(&key).with_context(|| {
            format!(
                "Failed to load user with key: {}, username: {}",
                key, user.username
            )
        }) {
            Ok(data) => {
                if let Some(user_data) = data {
                    user_data
                } else {
                    return Err(IggyError::ResourceNotFound(key));
                }
            }
            Err(err) => {
                return Err(IggyError::CannotLoadResource(err));
            }
        };

        let user_data = rmp_serde::from_slice::<User>(&user_data)
            .with_context(|| format!("Failed to deserialize user with key: {}", key));
        match user_data {
            Ok(user_data) => {
                user.status = user_data.status;
                user.username = user_data.username;
                user.password = user_data.password;
                user.created_at = user_data.created_at;
                user.permissions = user_data.permissions;
                Ok(())
            }
            Err(err) => Err(IggyError::CannotDeserializeResource(err)),
        }
    }
}

fn get_key(user_id: UserId) -> String {
    format!("{}:{}", KEY_PREFIX, user_id)
}

fn get_id_key(username: &str) -> String {
    format!("{}_id:{}", KEY_PREFIX, username)
}
