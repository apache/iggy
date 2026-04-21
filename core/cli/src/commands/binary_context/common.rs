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

use anyhow::{Context, Result, bail};
use dirs::home_dir;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::{env::var, path};
use tokio::join;

use iggy_common::ArgsOptional;

static ENV_IGGY_HOME: &str = "IGGY_HOME";
static DEFAULT_IGGY_HOME_VALUE: &str = ".iggy";
static ACTIVE_CONTEXT_FILE_NAME: &str = ".active_context";
static CONTEXTS_FILE_NAME: &str = "contexts.toml";
pub(crate) static DEFAULT_CONTEXT_NAME: &str = "default";

pub type ContextsConfigMap = BTreeMap<String, ContextConfig>;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ContextConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_name: Option<String>,

    #[serde(flatten)]
    pub iggy: ArgsOptional,

    #[serde(flatten)]
    pub extra: BTreeMap<String, toml::Value>,
}

struct ContextState {
    active_context: String,
    contexts: ContextsConfigMap,
}

impl Default for ContextState {
    fn default() -> Self {
        let mut contexts = ContextsConfigMap::new();
        contexts.insert(DEFAULT_CONTEXT_NAME.to_string(), ContextConfig::default());
        Self {
            active_context: DEFAULT_CONTEXT_NAME.to_string(),
            contexts,
        }
    }
}

pub struct ContextManager {
    context_rw: ContextReaderWriter,
    context_state: Option<ContextState>,
}

impl Default for ContextManager {
    fn default() -> Self {
        Self::new(ContextReaderWriter::default())
    }
}

impl ContextManager {
    pub fn new(context_rw: ContextReaderWriter) -> Self {
        Self {
            context_rw,
            context_state: None,
        }
    }

    pub async fn get_active_context(&mut self) -> Result<ContextConfig> {
        let active_context_key = self.get_active_context_key().await?;
        let contexts = self.get_contexts().await?;

        let active_context = contexts
            .get(&active_context_key)
            .ok_or_else(|| anyhow::anyhow!("active context key not found in contexts"))?;

        Ok(active_context.clone())
    }

    pub async fn set_active_context_key(&mut self, context_name: &str) -> Result<()> {
        self.get_context_state().await?;
        let cs = self.context_state.take().unwrap();

        if !cs.contexts.contains_key(context_name) {
            bail!("context key '{context_name}' is missing from {CONTEXTS_FILE_NAME}")
        }

        self.context_rw
            .write_active_context(context_name)
            .await
            .context(format!("failed writing active context '{context_name}'"))?;

        self.context_state.replace(ContextState {
            active_context: context_name.to_string(),
            contexts: cs.contexts,
        });

        Ok(())
    }

    pub async fn get_active_context_key(&mut self) -> Result<String> {
        let context_state = self.get_context_state().await?;
        Ok(context_state.active_context.clone())
    }

    pub async fn get_contexts(&mut self) -> Result<ContextsConfigMap> {
        let context_state = self.get_context_state().await?;
        Ok(context_state.contexts.clone())
    }

    pub async fn create_context(&mut self, name: &str, config: ContextConfig) -> Result<()> {
        validate_context_name(name)?;

        if name == DEFAULT_CONTEXT_NAME {
            bail!("cannot create a context named '{DEFAULT_CONTEXT_NAME}' - it is reserved")
        }

        self.get_context_state().await?;
        let cs = self.context_state.as_ref().unwrap();

        if cs.contexts.contains_key(name) {
            bail!("context '{name}' already exists in {CONTEXTS_FILE_NAME}")
        }

        let mut new_contexts = cs.contexts.clone();
        new_contexts.insert(name.to_string(), config);

        self.context_rw
            .write_contexts(new_contexts.clone())
            .await
            .context(format!("failed writing contexts after creating '{name}'"))?;

        self.context_state.replace(ContextState {
            active_context: cs.active_context.clone(),
            contexts: new_contexts,
        });

        Ok(())
    }

    pub async fn delete_context(&mut self, name: &str) -> Result<()> {
        if name == DEFAULT_CONTEXT_NAME {
            bail!("cannot delete the '{DEFAULT_CONTEXT_NAME}' context")
        }

        self.get_context_state().await?;
        let cs = self.context_state.as_ref().unwrap();

        if !cs.contexts.contains_key(name) {
            bail!("context '{name}' not found in {CONTEXTS_FILE_NAME}")
        }

        let mut new_contexts = cs.contexts.clone();
        new_contexts.remove(name);

        let active_context = if cs.active_context == name {
            self.context_rw
                .write_active_context(DEFAULT_CONTEXT_NAME)
                .await
                .context("failed resetting active context to default")?;
            DEFAULT_CONTEXT_NAME.to_string()
        } else {
            cs.active_context.clone()
        };

        self.context_rw
            .write_contexts(new_contexts.clone())
            .await
            .context(format!("failed writing contexts after deleting '{name}'"))?;

        self.context_state.replace(ContextState {
            active_context,
            contexts: new_contexts,
        });

        Ok(())
    }

    async fn get_context_state(&mut self) -> Result<&ContextState> {
        if self.context_state.is_none() {
            let (active_context_res, contexts_res) = join!(
                self.context_rw.read_active_context(),
                self.context_rw.read_contexts()
            );

            let (maybe_active_context, maybe_contexts) = active_context_res
                .and_then(|a| contexts_res.map(|b| (a, b)))
                .context("could not read context state")?;

            let mut context_state = ContextState::default();

            if let Some(contexts) = maybe_contexts {
                context_state.contexts.extend(contexts)
            }

            if let Some(active_context) = maybe_active_context {
                if !context_state.contexts.contains_key(&active_context) {
                    bail!("context key '{active_context}' is missing from {CONTEXTS_FILE_NAME}")
                }
                context_state.active_context = active_context;
            }

            self.context_state.replace(context_state);
        }

        Ok(self.context_state.as_ref().unwrap())
    }
}

pub struct ContextReaderWriter {
    iggy_home: Option<PathBuf>,
}

impl ContextReaderWriter {
    pub fn from_env() -> Self {
        Self::new(iggy_home())
    }

    pub fn new(iggy_home: Option<PathBuf>) -> Self {
        Self { iggy_home }
    }

    pub async fn read_contexts(&self) -> Result<Option<ContextsConfigMap>> {
        let maybe_contexts_path = &self.contexts_path();

        if let Some(contexts_path) = maybe_contexts_path {
            let maybe_contents = tokio::fs::read_to_string(contexts_path)
                .await
                .map(Some)
                .or_else(|err| {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        Ok(None)
                    } else {
                        Err(err)
                    }
                })
                .context(format!(
                    "failed reading contexts file {}",
                    contexts_path.display()
                ))?;

            if let Some(contents) = maybe_contents {
                let contexts: ContextsConfigMap =
                    toml::from_str(contents.as_str()).context(format!(
                        "failed deserializing contexts file {}",
                        contexts_path.display()
                    ))?;

                Ok(Some(contexts))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn write_contexts(&self, contexts: ContextsConfigMap) -> Result<()> {
        let maybe_contexts_path = self.contexts_path();

        if let Some(contexts_path) = maybe_contexts_path {
            let contents = toml::to_string(&contexts).context(format!(
                "failed serializing contexts file {}",
                contexts_path.display()
            ))?;

            self.ensure_iggy_home_exists().await?;
            tokio::fs::write(&contexts_path, contents).await?;
            Self::set_owner_only_permissions(&contexts_path).await?;
        }

        Ok(())
    }

    pub async fn read_active_context(&self) -> Result<Option<String>> {
        let maybe_active_context_path = self.active_context_path();

        if let Some(active_context_path) = maybe_active_context_path {
            tokio::fs::read_to_string(active_context_path.clone())
                .await
                .map(|s| Some(s.trim().to_string()))
                .or_else(|err| {
                    if err.kind() == std::io::ErrorKind::NotFound {
                        Ok(None)
                    } else {
                        Err(err)
                    }
                })
                .context(format!(
                    "failed reading active context file {}",
                    active_context_path.display()
                ))
        } else {
            Ok(None)
        }
    }

    pub async fn write_active_context(&self, context_name: &str) -> Result<()> {
        self.ensure_iggy_home_exists().await?;
        let maybe_active_context_path = self.active_context_path();

        if let Some(active_context_path) = maybe_active_context_path {
            tokio::fs::write(active_context_path.clone(), context_name)
                .await
                .context(format!(
                    "failed writing active context file {}",
                    active_context_path.to_string_lossy()
                ))?;
        }

        Ok(())
    }

    pub async fn ensure_iggy_home_exists(&self) -> Result<()> {
        if let Some(ref iggy_home) = self.iggy_home
            && !tokio::fs::try_exists(iggy_home).await.unwrap_or(false)
        {
            tokio::fs::create_dir_all(iggy_home).await.context(format!(
                "failed creating iggy home directory {}",
                iggy_home.display()
            ))?;
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn set_owner_only_permissions(path: &PathBuf) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(path, perms)
            .await
            .context(format!("failed setting permissions on {}", path.display()))
    }

    #[cfg(not(unix))]
    async fn set_owner_only_permissions(_path: &PathBuf) -> Result<()> {
        Ok(())
    }

    fn active_context_path(&self) -> Option<PathBuf> {
        self.iggy_home
            .clone()
            .map(|pb| pb.join(ACTIVE_CONTEXT_FILE_NAME))
    }

    fn contexts_path(&self) -> Option<PathBuf> {
        self.iggy_home.clone().map(|pb| pb.join(CONTEXTS_FILE_NAME))
    }
}

impl Default for ContextReaderWriter {
    fn default() -> Self {
        ContextReaderWriter::new(iggy_home())
    }
}

pub fn iggy_home() -> Option<PathBuf> {
    match var(ENV_IGGY_HOME) {
        Ok(home) => Some(PathBuf::from(home)),
        Err(_) => home_dir().map(|dir| dir.join(path::Path::new(DEFAULT_IGGY_HOME_VALUE))),
    }
}

fn validate_context_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        bail!("context name cannot be empty or whitespace-only")
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        bail!("context name must contain only alphanumeric characters, hyphens, and underscores")
    }
    Ok(())
}

pub fn validate_transport(transport: &str) -> Result<()> {
    use std::str::FromStr;
    iggy_common::TransportProtocol::from_str(transport).map_err(|_| {
        anyhow::anyhow!(
            "invalid transport '{}' - valid values are: tcp, quic, http, ws",
            transport
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_manager(iggy_home: PathBuf) -> ContextManager {
        ContextManager::new(ContextReaderWriter::new(Some(iggy_home)))
    }

    #[tokio::test]
    async fn should_create_context() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        let config = ContextConfig {
            username: Some("admin".to_string()),
            iggy: ArgsOptional {
                transport: Some("tcp".to_string()),
                tcp_server_address: Some("10.0.0.1:8090".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        mgr.create_context("production", config).await.unwrap();

        let contexts = mgr.get_contexts().await.unwrap();
        assert!(contexts.contains_key("production"));
        assert!(contexts.contains_key("default"));
        assert_eq!(contexts.len(), 2);
    }

    #[tokio::test]
    async fn should_reject_duplicate_context() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        mgr.create_context("test", ContextConfig::default())
            .await
            .unwrap();

        let result = mgr.create_context("test", ContextConfig::default()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn should_reject_creating_default_context() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        let result = mgr
            .create_context("default", ContextConfig::default())
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("reserved"));
    }

    #[tokio::test]
    async fn should_reject_empty_context_name() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        let result = mgr.create_context("", ContextConfig::default()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[tokio::test]
    async fn should_reject_whitespace_only_context_name() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        let result = mgr.create_context("  ", ContextConfig::default()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[tokio::test]
    async fn should_reject_context_name_with_special_chars() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        let result = mgr
            .create_context("my context!", ContextConfig::default())
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("alphanumeric"));
    }

    #[tokio::test]
    async fn should_accept_context_name_with_hyphens_and_underscores() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        mgr.create_context("my-context_01", ContextConfig::default())
            .await
            .unwrap();

        let contexts = mgr.get_contexts().await.unwrap();
        assert!(contexts.contains_key("my-context_01"));
    }

    #[tokio::test]
    async fn should_delete_context() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        mgr.create_context("staging", ContextConfig::default())
            .await
            .unwrap();

        mgr.delete_context("staging").await.unwrap();

        let contexts = mgr.get_contexts().await.unwrap();
        assert!(!contexts.contains_key("staging"));
    }

    #[tokio::test]
    async fn should_reject_deleting_default_context() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        let result = mgr.delete_context("default").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot delete"));
    }

    #[tokio::test]
    async fn should_reject_deleting_nonexistent_context() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        let result = mgr.delete_context("nope").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn should_reset_active_to_default_when_deleting_active_context() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        mgr.create_context("dev", ContextConfig::default())
            .await
            .unwrap();
        mgr.set_active_context_key("dev").await.unwrap();
        assert_eq!(mgr.get_active_context_key().await.unwrap(), "dev");

        mgr.delete_context("dev").await.unwrap();
        assert_eq!(mgr.get_active_context_key().await.unwrap(), "default");
    }

    #[tokio::test]
    async fn should_create_iggy_home_if_missing() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("sub").join("dir");
        let mut mgr = test_manager(nested.clone());

        assert!(!nested.exists());
        mgr.create_context("test", ContextConfig::default())
            .await
            .unwrap();
        assert!(nested.exists());
    }

    #[tokio::test]
    async fn should_persist_context_config_fields() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        let config = ContextConfig {
            username: Some("user1".to_string()),
            password: Some("pass1".to_string()),
            iggy: ArgsOptional {
                transport: Some("http".to_string()),
                http_api_url: Some("http://localhost:3000".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        mgr.create_context("myctx", config).await.unwrap();

        let rw = ContextReaderWriter::new(Some(dir.path().to_path_buf()));
        let saved = rw.read_contexts().await.unwrap().unwrap();
        let ctx = saved.get("myctx").unwrap();
        assert_eq!(ctx.username.as_deref(), Some("user1"));
        assert_eq!(ctx.password.as_deref(), Some("pass1"));
        assert_eq!(ctx.iggy.transport.as_deref(), Some("http"));
        assert_eq!(
            ctx.iggy.http_api_url.as_deref(),
            Some("http://localhost:3000")
        );
    }

    #[tokio::test]
    async fn reader_writer_with_none_iggy_home_is_noop_for_paths() {
        let rw = ContextReaderWriter::new(None);
        assert!(rw.read_contexts().await.unwrap().is_none());
        assert!(rw.read_active_context().await.unwrap().is_none());
        rw.write_contexts(ContextsConfigMap::new()).await.unwrap();
        rw.write_active_context("any").await.unwrap();
        rw.ensure_iggy_home_exists().await.unwrap();
    }

    #[tokio::test]
    async fn read_contexts_returns_none_when_file_missing() {
        let dir = tempdir().unwrap();
        let rw = ContextReaderWriter::new(Some(dir.path().to_path_buf()));
        assert!(rw.read_contexts().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_active_context_returns_none_when_file_missing() {
        let dir = tempdir().unwrap();
        let rw = ContextReaderWriter::new(Some(dir.path().to_path_buf()));
        assert!(rw.read_active_context().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ensure_iggy_home_exists_skips_when_directory_already_exists() {
        let dir = tempdir().unwrap();
        let rw = ContextReaderWriter::new(Some(dir.path().to_path_buf()));
        assert!(dir.path().exists());
        rw.ensure_iggy_home_exists().await.unwrap();
    }

    #[tokio::test]
    async fn should_fail_when_active_context_file_points_to_unknown_context() {
        let dir = tempdir().unwrap();
        let iggy_home = dir.path().to_path_buf();
        tokio::fs::create_dir_all(&iggy_home).await.unwrap();
        tokio::fs::write(iggy_home.join(ACTIVE_CONTEXT_FILE_NAME), "ghost")
            .await
            .unwrap();
        let mut only_default = ContextsConfigMap::new();
        only_default.insert(DEFAULT_CONTEXT_NAME.to_string(), ContextConfig::default());
        let contents = toml::to_string(&only_default).unwrap();
        tokio::fs::write(iggy_home.join(CONTEXTS_FILE_NAME), contents)
            .await
            .unwrap();

        let mut mgr = test_manager(iggy_home);
        let err = mgr.get_contexts().await.unwrap_err();
        assert!(err.to_string().contains("missing"));
    }

    #[tokio::test]
    async fn should_reject_set_active_for_unknown_context() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());
        let err = mgr.set_active_context_key("nonexistent").await.unwrap_err();
        assert!(err.to_string().contains("missing"));
    }

    #[tokio::test]
    async fn should_get_active_context_config() {
        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());
        let ctx = mgr.get_active_context().await.unwrap();
        assert!(ctx.username.is_none());
        assert!(ctx.password.is_none());
        assert_eq!(
            mgr.get_active_context_key().await.unwrap(),
            DEFAULT_CONTEXT_NAME
        );
    }

    #[tokio::test]
    async fn should_trim_whitespace_from_active_context() {
        let dir = tempdir().unwrap();
        let iggy_home = dir.path().to_path_buf();
        let rw = ContextReaderWriter::new(Some(iggy_home.clone()));
        rw.write_active_context("dev").await.unwrap();
        tokio::fs::write(iggy_home.join(ACTIVE_CONTEXT_FILE_NAME), "dev\n")
            .await
            .unwrap();
        let result = rw.read_active_context().await.unwrap();
        assert_eq!(result.as_deref(), Some("dev"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn should_set_restrictive_permissions_on_contexts_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let mut mgr = test_manager(dir.path().to_path_buf());

        mgr.create_context("secure-ctx", ContextConfig::default())
            .await
            .unwrap();

        let contexts_path = dir.path().join(CONTEXTS_FILE_NAME);
        let metadata = tokio::fs::metadata(&contexts_path).await.unwrap();
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn should_validate_transport() {
        assert!(validate_transport("tcp").is_ok());
        assert!(validate_transport("quic").is_ok());
        assert!(validate_transport("http").is_ok());
        assert!(validate_transport("ws").is_ok());
        assert!(validate_transport("foobar").is_err());
        assert!(validate_transport("websocket").is_err());
    }

    #[test]
    fn should_validate_context_name() {
        assert!(validate_context_name("production").is_ok());
        assert!(validate_context_name("my-ctx").is_ok());
        assert!(validate_context_name("my_ctx_01").is_ok());
        assert!(validate_context_name("").is_err());
        assert!(validate_context_name("  ").is_err());
        assert!(validate_context_name("my ctx").is_err());
        assert!(validate_context_name("ctx!").is_err());
        assert!(validate_context_name("a/b").is_err());
    }

    #[tokio::test]
    async fn should_preserve_unknown_fields_through_round_trip() {
        let dir = tempdir().unwrap();
        let iggy_home = dir.path().to_path_buf();

        let toml_with_extra = r#"[myctx]
username = "admin"
future_field = "preserved"
"#;
        tokio::fs::write(iggy_home.join(CONTEXTS_FILE_NAME), toml_with_extra)
            .await
            .unwrap();

        let rw = ContextReaderWriter::new(Some(iggy_home.clone()));
        let mut contexts = rw.read_contexts().await.unwrap().unwrap();
        contexts.insert("newctx".to_string(), ContextConfig::default());
        rw.write_contexts(contexts).await.unwrap();

        let reloaded = rw.read_contexts().await.unwrap().unwrap();
        let myctx = reloaded.get("myctx").unwrap();
        assert_eq!(myctx.username.as_deref(), Some("admin"));
        assert!(myctx.extra.contains_key("future_field"));
    }
}
