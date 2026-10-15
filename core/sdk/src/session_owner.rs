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

use crate::session::ConsensusSession;
use iggy_binary_protocol::HEADER_SIZE;
use iggy_binary_protocol::requests::system::SessionIdentity;
use iggy_binary_protocol::requests::users::login_register::BindSecret;
use iggy_common::{AutoLogin, Credentials, IdKind, Identifier, IggyError};
use lanes::Lanes;
use secrecy::SecretString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use tokio::sync::watch;

mod lanes;

pub(crate) use lanes::{LaneKey, LaneTicket, RetainedOperation};

/// The session state that every link of one client shares: the consensus
/// session, its epoch, the lanes, the metadata watermark and the sign-in.
///
/// Only [`Self::reset`] holds two of these locks, `session` and then `lanes`.
/// The epoch is read and written under either of them, never the other way
/// round. No lock is held across an await.
#[derive(Debug)]
pub(crate) struct SessionOwner {
    session: Mutex<ConsensusSession>,
    /// Changed only under the `session` lock. Every reset raises the fence,
    /// so a route, a link binding or a frame made under an older fence
    /// belongs to a replaced session.
    epoch: watch::Sender<SessionEpoch>,
    lanes: Mutex<Lanes>,
    /// The highest metadata commit a reply showed. A reset keeps it.
    metadata_watermark: AtomicU64,
    sign_in: Mutex<SignIn>,
    auto_login: AutoLogin,
}

/// The fence of the current session, and whether the session is bound.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionEpoch {
    pub(crate) fence: u64,
    pub(crate) bound: bool,
}

#[derive(Debug, Default)]
struct SignIn {
    /// Lets a reconnect, to this node or after a failover to another one,
    /// sign in again instead of surfacing `Unauthenticated`. A logout and an
    /// explicit disconnect clear it.
    remembered: Option<RememberedSignIn>,
    /// A committed change for the user of a configured `AutoLogin`. The
    /// configuration cannot be rewritten, and every login replaces
    /// `remembered`, so only here does the change reach every later sign-in.
    configured_password: Option<SecretString>,
    configured_username: Option<String>,
}

/// The sign-in a login on this client succeeded with. A password change can
/// target any user, so the id decides whether a change applies here.
#[derive(Debug)]
struct RememberedSignIn {
    credentials: Credentials,
    user_id: u32,
}

impl SessionOwner {
    pub(crate) fn new(auto_login: AutoLogin) -> Self {
        Self {
            session: Mutex::new(ConsensusSession::new()),
            epoch: watch::Sender::new(SessionEpoch::default()),
            lanes: Mutex::default(),
            metadata_watermark: AtomicU64::new(0),
            sign_in: Mutex::default(),
            auto_login,
        }
    }

    /// The caller must not replace the session through the guard, because
    /// only [`Self::reset`] raises the fence.
    pub(crate) fn session(&self) -> Result<MutexGuard<'_, ConsensusSession>, IggyError> {
        self.session
            .lock()
            .map_err(|_| IggyError::InvalidConfiguration)
    }

    pub(crate) fn identity(&self) -> Result<SessionIdentity, IggyError> {
        let session = self.session()?;
        Ok(SessionIdentity {
            client_id: session.client_id(),
            session: session.session().ok_or(IggyError::Unauthenticated)?,
            metadata_watermark: self.metadata_watermark(),
        })
    }

    pub(crate) fn bind_secret(&self) -> Result<BindSecret, IggyError> {
        Ok(self.session()?.bind_secret())
    }

    /// [`Self::bind_at`] at the current fence.
    #[cfg(test)]
    pub(crate) fn bind(&self, session: u64) -> Result<bool, IggyError> {
        self.bind_at(self.fence(), session)
    }

    /// Binds `session`, but only while the fence is `fence`. So the reply to
    /// a Register of a replaced session cannot bind the new session. Returns
    /// true on the first bind of the session. A resume binds the session
    /// again and keeps the fence.
    pub(crate) fn bind_at(&self, fence: u64, session: u64) -> Result<bool, IggyError> {
        let mut consensus_session = self.session()?;
        if self.fence() != fence {
            return Err(IggyError::InvalidSession(session));
        }
        let first_bind = !consensus_session.is_bound();
        consensus_session.bind(session)?;
        self.epoch
            .send_if_modified(|epoch| !std::mem::replace(&mut epoch.bound, true));
        Ok(first_bind)
    }

    /// Replaces the session, raises the fence and drops every retained
    /// operation. The request counter, the watermark and the sign-in stay.
    pub(crate) fn reset(&self) -> Result<(), IggyError> {
        let mut session = self.session()?;
        self.replace(&mut session);
        Ok(())
    }

    /// [`Self::reset`], but only while the fence is `fence`. So when every
    /// link finds the session gone, only one of them resets it. Returns the
    /// new fence if this call reset the session.
    pub(crate) fn reset_if_fence(&self, fence: u64) -> Result<Option<u64>, IggyError> {
        let mut session = self.session()?;
        if self.fence() != fence {
            return Ok(None);
        }
        self.replace(&mut session);
        Ok(Some(self.fence()))
    }

    pub(crate) fn fence(&self) -> u64 {
        self.epoch.borrow().fence
    }

    pub(crate) fn epoch(&self) -> SessionEpoch {
        *self.epoch.borrow()
    }

    pub(crate) fn subscribe_epoch(&self) -> watch::Receiver<SessionEpoch> {
        self.epoch.subscribe()
    }

    pub(crate) fn metadata_watermark(&self) -> u64 {
        self.metadata_watermark.load(Ordering::Acquire)
    }

    pub(crate) fn observe_metadata_reply(&self, header: &[u8; HEADER_SIZE]) {
        crate::vsr::observe_metadata_reply(&self.metadata_watermark, header);
    }

    pub(crate) fn remember(&self, credentials: Credentials, user_id: u32) {
        self.remember_unless(|| false, credentials, user_id);
    }

    /// [`Self::remember`], unless `withdrawn` returns true. It runs under the
    /// sign-in lock, so a caller that marks the sign-in withdrawn and then
    /// calls [`Self::forget`] cannot have a later sign-in undo that forget.
    /// Returns whether the credentials were kept.
    pub(crate) fn remember_unless(
        &self,
        withdrawn: impl FnOnce() -> bool,
        credentials: Credentials,
        user_id: u32,
    ) -> bool {
        let mut sign_in = self.sign_in();
        if withdrawn() {
            return false;
        }
        sign_in.remembered = Some(RememberedSignIn {
            credentials,
            user_id,
        });
        true
    }

    pub(crate) fn forget(&self) {
        self.sign_in().remembered = None;
    }

    /// The last successful sign-in, else the configured `AutoLogin` with the
    /// changes this client committed for its user.
    pub(crate) fn sign_in_credentials(&self) -> Option<Credentials> {
        let sign_in = self.sign_in();
        // A client is whoever it last signed in as, so a reconnect restores
        // the sign-in the caller last asked for. A configured `AutoLogin`
        // signs in through this path too.
        if let Some(remembered) = &sign_in.remembered {
            return Some(remembered.credentials.clone());
        }
        match &self.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(username, password)) => {
                Some(Credentials::UsernamePassword(
                    sign_in
                        .configured_username
                        .clone()
                        .unwrap_or_else(|| username.clone()),
                    sign_in
                        .configured_password
                        .clone()
                        .unwrap_or_else(|| password.clone()),
                ))
            }
            AutoLogin::Enabled(credentials) => Some(credentials.clone()),
            AutoLogin::Disabled => None,
        }
    }

    /// Applies a committed password change to the remembered sign-in and to
    /// the configured `AutoLogin`, each only if the change targets its user.
    pub(crate) fn refresh_password(&self, user: &Identifier, new_password: &str) {
        let mut sign_in = self.sign_in();
        let SignIn {
            remembered,
            configured_password,
            configured_username,
        } = &mut *sign_in;
        // A personal access token is not derived from the password.
        let session_username = match remembered {
            Some(RememberedSignIn {
                credentials: Credentials::UsernamePassword(username, password),
                user_id,
            }) if matches_session_user(user, *user_id, username) => {
                *password = SecretString::from(new_password.to_owned());
                Some(username.as_str())
            }
            _ => None,
        };
        let Some(configured) = configured_user(&self.auto_login, configured_username) else {
            return;
        };
        // Only the remembered login reports a user id, so a numeric id names
        // the configured user only through the signed-in user. A change made
        // from another user's session has to name the configured user.
        let targets_configured_user = match user.kind {
            IdKind::String => user
                .get_cow_str_value()
                .is_ok_and(|name| name.as_ref() == configured),
            IdKind::Numeric => session_username == Some(configured),
        };
        if targets_configured_user {
            *configured_password = Some(SecretString::from(new_password.to_owned()));
        }
    }

    pub(crate) fn refresh_username(&self, user: &Identifier, new_username: &str) {
        let mut sign_in = self.sign_in();
        let SignIn {
            remembered,
            configured_username,
            ..
        } = &mut *sign_in;
        let renamed = match remembered {
            Some(RememberedSignIn {
                credentials: Credentials::UsernamePassword(username, _),
                user_id,
            }) if matches_session_user(user, *user_id, username) => {
                Some(std::mem::replace(username, new_username.to_owned()))
            }
            _ => None,
        };
        let Some(configured) = configured_user(&self.auto_login, configured_username) else {
            return;
        };
        if renamed.as_deref() == Some(configured)
            || user
                .get_cow_str_value()
                .is_ok_and(|name| name.as_ref() == configured)
        {
            *configured_username = Some(new_username.to_owned());
        }
    }

    /// The session of a poisoned lock, for tests that poison it.
    #[cfg(test)]
    pub(crate) fn session_ignoring_poison(&self) -> MutexGuard<'_, ConsensusSession> {
        self.session.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn sign_in(&self) -> MutexGuard<'_, SignIn> {
        self.sign_in.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lanes(&self) -> MutexGuard<'_, Lanes> {
        self.lanes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn replace(&self, session: &mut ConsensusSession) {
        let mut lanes = self.lanes();
        *session = ConsensusSession::with_next_request_id(session.current_request_id());
        self.epoch.send_modify(|epoch| {
            epoch.fence += 1;
            epoch.bound = false;
        });
        lanes::drop_retained(&mut lanes);
    }
}

/// The username a configured `AutoLogin` signs in as, after the renames this
/// client committed.
fn configured_user<'a>(auto_login: &'a AutoLogin, renamed: &'a Option<String>) -> Option<&'a str> {
    let AutoLogin::Enabled(Credentials::UsernamePassword(configured, _)) = auto_login else {
        return None;
    };
    Some(renamed.as_deref().unwrap_or(configured))
}

fn matches_session_user(user: &Identifier, user_id: u32, username: &str) -> bool {
    match user.kind {
        IdKind::Numeric => user.get_u32_value().is_ok_and(|id| id == user_id),
        IdKind::String => user
            .get_cow_str_value()
            .is_ok_and(|name| name.as_ref() == username),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vsr::metadata_reply_header;
    use secrecy::ExposeSecret;

    const SESSION_USER_ID: u32 = 7;
    const BOUND_SESSION: u64 = 3;
    const NEXT_SESSION: u64 = 4;
    const METADATA_COMMIT: u64 = 11;

    #[test]
    fn given_a_reset_when_the_session_is_replaced_should_raise_the_fence_and_keep_the_watermark_and_sign_in()
     {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        owner.bind(BOUND_SESSION).unwrap();
        owner.observe_metadata_reply(&metadata_reply_header(METADATA_COMMIT));
        owner.remember(user_password("iggy", "iggy"), SESSION_USER_ID);
        let replaced = owner.identity().unwrap();
        let fence = owner.fence();

        owner.reset().unwrap();

        assert_eq!(owner.fence(), fence + 1);
        assert_eq!(owner.metadata_watermark(), METADATA_COMMIT);
        assert_eq!(
            username_and_password(owner.sign_in_credentials()),
            ("iggy".to_owned(), "iggy".to_owned())
        );
        let session = owner.session().unwrap();
        assert!(!session.is_bound());
        assert_ne!(session.client_id(), replaced.client_id);
    }

    #[test]
    fn given_a_session_reset_when_old_replies_arrive_should_not_complete_new_calls() {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        owner.bind(BOUND_SESSION).unwrap();
        let old_request = owner.session().unwrap().next_request_id().unwrap();

        owner.reset().unwrap();
        owner.bind(NEXT_SESSION).unwrap();

        let new_request = owner.session().unwrap().next_request_id().unwrap();
        assert!(
            new_request > old_request,
            "request {new_request} after a reset can take the reply of request {old_request}"
        );
    }

    #[test]
    fn given_a_resumed_session_when_bound_again_should_keep_the_fence() {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        assert!(owner.bind(BOUND_SESSION).unwrap(), "the first bind");
        let fence = owner.fence();

        assert!(!owner.bind(BOUND_SESSION).unwrap(), "a resume");

        assert_eq!(owner.fence(), fence);
    }

    #[test]
    fn given_a_lost_session_when_two_links_reset_it_should_reset_it_once() {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        owner.bind(BOUND_SESSION).unwrap();
        let fence = owner.fence();

        let first = owner.reset_if_fence(fence).unwrap();
        let second = owner.reset_if_fence(fence).unwrap();

        assert_eq!(first, Some(owner.fence()));
        assert_eq!(second, None);
        assert_eq!(owner.fence(), fence + 1);
    }

    #[test]
    fn given_a_reset_during_a_register_when_its_reply_binds_should_leave_the_new_session_unbound() {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        let fence = owner.fence();
        owner.reset().unwrap();

        let bound = owner.bind_at(fence, BOUND_SESSION);

        assert_eq!(bound.unwrap_err(), IggyError::InvalidSession(BOUND_SESSION));
        assert!(!owner.epoch().bound);
    }

    #[test]
    fn given_no_auto_login_when_a_sign_in_is_remembered_should_offer_it() {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        assert!(owner.sign_in_credentials().is_none());

        owner.remember(user_password("iggy", "iggy"), SESSION_USER_ID);
        assert!(owner.sign_in_credentials().is_some());

        // An explicit logout leaves no session to restore, and a reconnect
        // must not resurrect one.
        owner.forget();
        assert!(owner.sign_in_credentials().is_none());
    }

    // Otherwise the next reconnect replays the old password and fails an
    // unrelated request with `InvalidCredentials`.
    #[test]
    fn given_a_remembered_sign_in_when_its_user_changes_password_should_use_the_new_password() {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        for user in [
            Identifier::numeric(SESSION_USER_ID).unwrap(),
            Identifier::named("iggy").unwrap(),
        ] {
            owner.remember(user_password("iggy", "old"), SESSION_USER_ID);

            owner.refresh_password(&user, "new");

            assert_eq!(
                username_and_password(owner.sign_in_credentials()).1,
                "new",
                "for user: {user}"
            );
        }
    }

    #[test]
    fn given_a_remembered_sign_in_when_another_user_changes_password_should_keep_it() {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        owner.remember(user_password("iggy", "old"), SESSION_USER_ID);

        for user in [
            Identifier::numeric(SESSION_USER_ID + 1).unwrap(),
            Identifier::named("someone-else").unwrap(),
        ] {
            owner.refresh_password(&user, "new");

            assert_eq!(
                username_and_password(owner.sign_in_credentials()).1,
                "old",
                "for user: {user}"
            );
        }
    }

    #[test]
    fn given_a_remembered_token_when_a_password_changes_should_keep_the_token() {
        let owner = SessionOwner::new(AutoLogin::Disabled);
        owner.remember(
            Credentials::PersonalAccessToken("token".into()),
            SESSION_USER_ID,
        );

        owner.refresh_password(&Identifier::numeric(SESSION_USER_ID).unwrap(), "new");

        match owner.sign_in_credentials() {
            Some(Credentials::PersonalAccessToken(token)) => {
                assert_eq!(token.expose_secret(), "token");
            }
            other => panic!("expected the remembered token, got {other:?}"),
        }
    }

    #[test]
    fn given_a_configured_auto_login_when_its_user_changes_password_should_use_the_new_password() {
        let owner = owner_with_auto_login("iggy", "old");
        owner.remember(user_password("iggy", "old"), SESSION_USER_ID);

        owner.refresh_password(&Identifier::numeric(SESSION_USER_ID).unwrap(), "new");

        // Each reconnect signs in and remembers that sign-in again, and the
        // configured password is dead for good.
        for reconnect in 0..3 {
            let (username, password) = username_and_password(owner.sign_in_credentials());
            assert_eq!(
                (username.as_str(), password.as_str()),
                ("iggy", "new"),
                "reconnect {reconnect}"
            );
            owner.remember(user_password(&username, &password), SESSION_USER_ID);
        }
    }

    #[test]
    fn given_a_configured_auto_login_when_another_user_changes_password_should_keep_it() {
        let owner = owner_with_auto_login("configured", "old");
        owner.remember(user_password("signed-in", "old"), SESSION_USER_ID);

        owner.refresh_password(&Identifier::numeric(SESSION_USER_ID).unwrap(), "new");

        owner.forget();
        assert_eq!(
            username_and_password(owner.sign_in_credentials()),
            ("configured".to_owned(), "old".to_owned())
        );
    }

    #[test]
    fn given_another_session_when_a_change_names_the_configured_user_should_refresh_it() {
        let owner = owner_with_auto_login("configured", "old");
        owner.remember(
            Credentials::PersonalAccessToken("token".into()),
            SESSION_USER_ID,
        );

        owner.refresh_password(&Identifier::named("configured").unwrap(), "new");

        owner.forget();
        assert_eq!(
            username_and_password(owner.sign_in_credentials()),
            ("configured".to_owned(), "new".to_owned())
        );
    }

    #[test]
    fn given_a_remembered_sign_in_and_auto_login_when_choosing_should_prefer_the_remembered_one() {
        let owner = owner_with_auto_login("configured", "iggy");
        owner.remember(user_password("signed-in", "iggy"), SESSION_USER_ID);

        assert_eq!(
            username_and_password(owner.sign_in_credentials()).0,
            "signed-in"
        );
        owner.forget();
        assert_eq!(
            username_and_password(owner.sign_in_credentials()).0,
            "configured"
        );
    }

    #[test]
    fn given_a_renamed_session_user_when_signing_in_again_should_use_the_new_username() {
        let owner = owner_with_auto_login("old-name", "password");
        owner.remember(user_password("old-name", "password"), SESSION_USER_ID);

        owner.refresh_username(&Identifier::numeric(SESSION_USER_ID).unwrap(), "new-name");

        assert_eq!(
            username_and_password(owner.sign_in_credentials()).0,
            "new-name"
        );
        owner.forget();
        assert_eq!(
            username_and_password(owner.sign_in_credentials()).0,
            "new-name"
        );
    }

    fn owner_with_auto_login(username: &str, password: &str) -> SessionOwner {
        SessionOwner::new(AutoLogin::Enabled(user_password(username, password)))
    }

    fn user_password(username: &str, password: &str) -> Credentials {
        Credentials::UsernamePassword(username.to_owned(), password.into())
    }

    fn username_and_password(credentials: Option<Credentials>) -> (String, String) {
        match credentials {
            Some(Credentials::UsernamePassword(username, password)) => {
                (username, password.expose_secret().to_owned())
            }
            other => panic!("expected a username and a password, got {other:?}"),
        }
    }
}
