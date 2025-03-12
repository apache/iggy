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

use anyhow::{bail, Context, Result};
use dirs::home_dir;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::{collections::HashMap, env::var, path};
use tokio::join;

use crate::args::ArgsOptional;

static ENV_IGGY_HOME: &str = "IGGY_HOME";
static DEFAULT_IGGY_HOME_VALUE: &str = ".iggy";
static ACTIVE_CONTEXT_FILE_NAME: &str = ".active_context";
static CONTEXTS_FILE_NAME: &str = "contexts.toml";
pub(crate) static DEFAULT_CONTEXT_NAME: &str = "default";

pub type ContextsConfigMap = HashMap<String, ContextConfig>;

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
}

struct ContextState {
    active_context: String,
    contexts: ContextsConfigMap,
}

impl Default for ContextState {
    fn default() -> Self {
        Self {
            active_context: DEFAULT_CONTEXT_NAME.to_string(),
            contexts: HashMap::from([(DEFAULT_CONTEXT_NAME.to_string(), ContextConfig::default())]),
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

            tokio::fs::write(contexts_path, contents).await?;
        }

        Ok(())
    }

    pub async fn read_active_context(&self) -> Result<Option<String>> {
        let maybe_active_context_path = self.active_context_path();

        if let Some(active_context_path) = maybe_active_context_path {
            tokio::fs::read_to_string(active_context_path.clone())
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
                    "failed reading active context file {}",
                    active_context_path.display()
                ))
        } else {
            Ok(None)
        }
    }

    pub async fn write_active_context(&self, context_name: &str) -> Result<()> {
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
    let iggy_home = match var(ENV_IGGY_HOME) {
        Ok(home) => Some(PathBuf::from(home)),
        Err(_) => home_dir().map(|dir| dir.join(path::Path::new(DEFAULT_IGGY_HOME_VALUE))),
    };
    iggy_home
}
