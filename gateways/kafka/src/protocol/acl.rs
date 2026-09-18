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

//! Rendering an Iggy principal's permissions as Kafka ACL bindings.
//!
//! Pure and synchronous, and deliberately free of any Iggy type: the permissions arrive as
//! [`PrincipalPermissions`], which `auth` fills in. That keeps the protocol layer independent of
//! the SDK and makes the mapping unit-testable without a server.
//!
//! See `docs/ACL_MAPPING.md` for the decisions this implements, including what is not mapped.

/// Kafka resource types (`org.apache.kafka.common.resource.ResourceType`). `kafka_protocol` carries
/// these as bare `i8` with no enum, so the values live here.
pub mod resource_type {
    pub const ANY: i8 = 1;
    pub const TOPIC: i8 = 2;
    pub const GROUP: i8 = 3;
    pub const CLUSTER: i8 = 4;
}

/// Kafka pattern types (`org.apache.kafka.common.resource.PatternType`).
pub mod pattern_type {
    pub const ANY: i8 = 1;
    /// Kafka's own "match anything of this type" lookup, which a filter may ask for.
    pub const MATCH: i8 = 2;
    pub const LITERAL: i8 = 3;
}

/// Kafka ACL operations (`org.apache.kafka.common.acl.AclOperation`).
pub mod operation {
    pub const ANY: i8 = 1;
    pub const ALL: i8 = 2;
    pub const READ: i8 = 3;
    pub const WRITE: i8 = 4;
    pub const CREATE: i8 = 5;
    pub const DELETE: i8 = 6;
    pub const ALTER: i8 = 7;
    pub const DESCRIBE: i8 = 8;
}

/// Kafka ACL permission types (`org.apache.kafka.common.acl.AclPermissionType`).
pub mod permission_type {
    pub const ANY: i8 = 1;
    pub const ALLOW: i8 = 3;
}

/// Name Kafka gives the cluster resource. There is exactly one, and it is always called this.
pub const CLUSTER_NAME: &str = "kafka-cluster";

/// How Kafka spells "every resource of this type": a literal pattern named `*`.
pub const WILDCARD: &str = "*";

/// Host scope on every binding this gateway renders. Iggy has no host-scoped permissions.
pub const ANY_HOST: &str = "*";

/// The subset of an Iggy principal's global permissions that has a Kafka meaning.
///
/// Stream-level flags are folded into the topic ones by the caller: Kafka has no resource above a
/// topic, and every Kafka topic lives inside one Iggy stream, so a stream grant is in practice a
/// grant over the topics a Kafka client can reach.
///
/// The boolean count mirrors Iggy's own `GlobalPermissions`, which is a flat set of independent
/// grants. Collapsing them into a bitfield would hide which grant is which at every call site for
/// no gain, so the lint is allowed here the way it is elsewhere in this repository.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PrincipalPermissions {
    /// True when the principal holds either of Iggy's server flags. `manage_servers` is not
    /// carried separately: Iggy reads it in exactly one rule, as an alias for this one, so it
    /// gates no mutation anywhere and renders nothing of its own. `docs/ACL_MAPPING.md` has the
    /// argument, which is the same one that keeps `manage_users` out of the table.
    pub read_servers: bool,
    pub read_topics: bool,
    pub manage_topics: bool,
    pub poll_messages: bool,
    pub send_messages: bool,
}

/// One rendered binding. Principal, host and permission type are constant for every binding this
/// gateway produces, so they are not carried here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AclBinding {
    pub resource_type: i8,
    pub resource_name: &'static str,
    pub operation: i8,
}

impl AclBinding {
    const fn new(resource_type: i8, resource_name: &'static str, operation: i8) -> Self {
        Self {
            resource_type,
            resource_name,
            operation,
        }
    }
}

/// Renders a principal's permissions as Kafka ACL bindings.
///
/// Only global permissions are rendered, as wildcard bindings. Iggy keys its per-stream and
/// per-topic permissions by numeric id, and the topic mapping is one-way, so a named Kafka binding
/// cannot be reconstructed from them without a reverse index that does not exist. The result
/// under-reports rather than over-reports, which is the safe direction for an authorization view.
#[must_use]
pub fn bindings_for(permissions: &PrincipalPermissions) -> Vec<AclBinding> {
    let mut bindings = Vec::new();

    if permissions.read_servers {
        bindings.push(AclBinding::new(
            resource_type::CLUSTER,
            CLUSTER_NAME,
            operation::DESCRIBE,
        ));
    }
    if permissions.read_topics {
        bindings.push(AclBinding::new(
            resource_type::TOPIC,
            WILDCARD,
            operation::DESCRIBE,
        ));
    }
    if permissions.manage_topics {
        for op in [operation::CREATE, operation::DELETE, operation::ALTER] {
            bindings.push(AclBinding::new(resource_type::TOPIC, WILDCARD, op));
        }
    }
    if permissions.poll_messages {
        bindings.push(AclBinding::new(
            resource_type::TOPIC,
            WILDCARD,
            operation::READ,
        ));
    }
    // Derived, not stored: Iggy has no group-level permission. Group *membership* operations
    // (create, delete, get, join, leave) route through `Permissioner::get_topic`
    // (`permissioner_rules/consumer_groups.rs`), which admits on the read and manage flags and
    // never consults `poll_messages`. Deriving this from polling instead granted a group to
    // principals Iggy denies, which is the over-report the design commits against.
    //
    // Not every group-shaped operation goes that way: offset commit and fetch route through
    // `poll_messages` (`permissioner_rules/consumer_offsets.rs`), while Kafka gates them on this
    // same GROUP READ. A principal with polling but no read grant is therefore shown no group
    // binding while Iggy would let it commit an offset. That under-reports, which is the safe
    // direction, and the alternative over-reports membership.
    if permissions.read_topics {
        bindings.push(AclBinding::new(
            resource_type::GROUP,
            WILDCARD,
            operation::READ,
        ));
    }
    if permissions.send_messages {
        bindings.push(AclBinding::new(
            resource_type::TOPIC,
            WILDCARD,
            operation::WRITE,
        ));
    }

    bindings
}

/// The filter carried by a `DescribeAcls` request, already decoded.
#[derive(Debug, Clone)]
pub struct AclFilter {
    pub resource_type: i8,
    pub resource_name: Option<String>,
    pub pattern_type: i8,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: i8,
    pub permission_type: i8,
}

impl AclFilter {
    /// Whether this filter selects `binding`, belonging to `principal`.
    ///
    /// Kafka's `ANY` sentinel matches everything, and an absent string field is the same as `ANY`.
    /// A principal filter naming anyone else matches nothing: the gateway holds no administrative
    /// credentials and can only ever read the caller's own record, so claiming an empty result for
    /// another user is the only honest answer it can give.
    #[must_use]
    pub fn matches(&self, binding: &AclBinding, principal: &str) -> bool {
        if self.resource_type != resource_type::ANY && self.resource_type != binding.resource_type {
            return false;
        }
        // PREFIXED selects nothing, because nothing here is prefix-scoped.
        if !matches!(
            self.pattern_type,
            pattern_type::ANY | pattern_type::MATCH | pattern_type::LITERAL
        ) {
            return false;
        }
        if !self.matches_resource_name(binding.resource_name) {
            return false;
        }
        if !matches_name(self.principal.as_deref(), &format!("User:{principal}")) {
            return false;
        }
        if !matches_name(self.host.as_deref(), ANY_HOST) {
            return false;
        }
        // Only `ANY` is a wildcard. `AccessControlEntryFilter.matches` compares everything else by
        // equality, so `ALL` selects bindings whose operation is literally `ALL`. Nothing here
        // renders one, so a filter asking for it is correctly empty. Treating it as a wildcard
        // would return every binding, which over-reports a principal's access.
        if self.operation != operation::ANY && self.operation != binding.operation {
            return false;
        }
        if self.permission_type != permission_type::ANY
            && self.permission_type != permission_type::ALLOW
        {
            return false;
        }
        true
    }
}

/// An absent filter field means `ANY`, so it matches. Present fields compare exactly.
fn matches_name(filter: Option<&str>, value: &str) -> bool {
    filter.is_none_or(|wanted| wanted == value)
}

impl AclFilter {
    /// Name matching, which `MATCH` widens.
    ///
    /// `ResourcePatternFilter.matches` gives `MATCH` a second branch: a named filter also selects a
    /// literal pattern named `*`. That is how `kafka-acls.sh --topic foo --resource-pattern-type
    /// match` asks "what affects topic foo", and since every binding here is a wildcard, exact
    /// comparison alone would answer nothing to the one query that should find them all.
    fn matches_resource_name(&self, name: &str) -> bool {
        let Some(wanted) = self.resource_name.as_deref() else {
            return true;
        };
        if wanted == name {
            return true;
        }
        self.pattern_type == pattern_type::MATCH && name == WILDCARD
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_permissions() -> PrincipalPermissions {
        PrincipalPermissions {
            read_servers: true,
            read_topics: true,
            manage_topics: true,
            poll_messages: true,
            send_messages: true,
        }
    }

    fn any_filter() -> AclFilter {
        AclFilter {
            resource_type: resource_type::ANY,
            resource_name: None,
            pattern_type: pattern_type::ANY,
            principal: None,
            host: None,
            operation: operation::ANY,
            permission_type: permission_type::ANY,
        }
    }

    #[test]
    fn given_no_permissions_when_rendered_should_produce_no_bindings() {
        assert!(bindings_for(&PrincipalPermissions::default()).is_empty());
    }

    #[test]
    fn given_only_poll_when_rendered_should_grant_the_topic_read_but_no_group() {
        // Consumer-group operations route through Iggy's topic rule, which never consults
        // `poll_messages`. Deriving the group binding from polling advertised access Iggy denies.
        let permissions = PrincipalPermissions {
            poll_messages: true,
            ..PrincipalPermissions::default()
        };
        let bindings = bindings_for(&permissions);
        assert_eq!(
            bindings,
            vec![AclBinding::new(
                resource_type::TOPIC,
                WILDCARD,
                operation::READ
            )]
        );
    }

    #[test]
    fn given_a_topic_read_grant_should_render_the_derived_group_binding() {
        // The grant Iggy actually admits consumer-group operations on.
        let permissions = PrincipalPermissions {
            read_topics: true,
            ..PrincipalPermissions::default()
        };
        let bindings = bindings_for(&permissions);
        assert!(bindings.contains(&AclBinding::new(
            resource_type::GROUP,
            WILDCARD,
            operation::READ
        )));
    }

    #[test]
    fn given_only_send_when_rendered_should_grant_write_and_no_group() {
        let permissions = PrincipalPermissions {
            send_messages: true,
            ..PrincipalPermissions::default()
        };
        let bindings = bindings_for(&permissions);
        assert_eq!(
            bindings,
            vec![AclBinding::new(
                resource_type::TOPIC,
                WILDCARD,
                operation::WRITE
            )],
            "a producer has no group to read"
        );
    }

    #[test]
    fn given_manage_topics_when_rendered_should_grant_the_three_admin_operations() {
        let permissions = PrincipalPermissions {
            manage_topics: true,
            ..PrincipalPermissions::default()
        };
        let operations: Vec<i8> = bindings_for(&permissions)
            .iter()
            .map(|binding| binding.operation)
            .collect();
        assert_eq!(
            operations,
            vec![operation::CREATE, operation::DELETE, operation::ALTER]
        );
    }

    #[test]
    fn given_server_permissions_when_rendered_should_scope_them_to_the_cluster() {
        let permissions = PrincipalPermissions {
            read_servers: true,
            ..PrincipalPermissions::default()
        };
        let bindings = bindings_for(&permissions);
        assert!(
            bindings
                .iter()
                .all(|binding| binding.resource_type == resource_type::CLUSTER
                    && binding.resource_name == CLUSTER_NAME)
        );
    }

    #[test]
    fn given_an_any_filter_when_matching_should_select_every_binding() {
        let filter = any_filter();
        let bindings = bindings_for(&all_permissions());
        assert!(
            bindings
                .iter()
                .all(|binding| filter.matches(binding, "alice"))
        );
    }

    #[test]
    fn given_a_resource_type_filter_when_matching_should_select_only_that_type() {
        let filter = AclFilter {
            resource_type: resource_type::GROUP,
            ..any_filter()
        };
        let selected: Vec<AclBinding> = bindings_for(&all_permissions())
            .into_iter()
            .filter(|binding| filter.matches(binding, "alice"))
            .collect();
        assert_eq!(
            selected,
            vec![AclBinding::new(
                resource_type::GROUP,
                WILDCARD,
                operation::READ
            )]
        );
    }

    #[test]
    fn given_a_filter_naming_another_principal_should_select_nothing() {
        // The gateway can only ever read the caller's own record, so anything else is empty.
        let filter = AclFilter {
            principal: Some("User:someone-else".to_string()),
            ..any_filter()
        };
        let bindings = bindings_for(&all_permissions());
        assert!(
            !bindings
                .iter()
                .any(|binding| filter.matches(binding, "alice"))
        );
    }

    #[test]
    fn given_a_filter_naming_the_caller_should_select_their_bindings() {
        let filter = AclFilter {
            principal: Some("User:alice".to_string()),
            ..any_filter()
        };
        let bindings = bindings_for(&all_permissions());
        assert!(
            bindings
                .iter()
                .all(|binding| filter.matches(binding, "alice"))
        );
    }

    #[test]
    fn given_a_deny_filter_when_matching_should_select_nothing() {
        // Iggy has no deny rules, so a filter asking for them is correctly empty rather than an
        // error.
        let filter = AclFilter {
            permission_type: 2,
            ..any_filter()
        };
        let bindings = bindings_for(&all_permissions());
        assert!(!bindings.iter().any(|b| filter.matches(b, "alice")));
    }

    #[test]
    fn given_a_prefixed_pattern_filter_should_select_nothing() {
        // Nothing here is prefix-scoped, so this is empty rather than a wrong match.
        let filter = AclFilter {
            pattern_type: 4,
            ..any_filter()
        };
        let bindings = bindings_for(&all_permissions());
        assert!(!bindings.iter().any(|b| filter.matches(b, "alice")));
    }

    #[test]
    fn given_an_operation_filter_of_all_should_select_nothing() {
        // `ALL` is a concrete operation in Kafka, not a filter wildcard: only `ANY` is one, and
        // `AccessControlEntryFilter.matches` compares everything else by equality. Nothing here
        // renders an `ALL` binding, so this is correctly empty. Treating it as a wildcard returned
        // every binding, which over-reports a principal's access.
        let filter = AclFilter {
            operation: operation::ALL,
            ..any_filter()
        };
        let bindings = bindings_for(&all_permissions());
        assert!(!bindings.iter().any(|b| filter.matches(b, "alice")));
    }

    #[test]
    fn given_a_match_pattern_filter_with_a_name_should_select_the_wildcard_bindings() {
        // `kafka-acls.sh --topic orders --resource-pattern-type match` asks "what affects orders".
        // Every binding here is a wildcard, so exact comparison alone answered nothing to the one
        // query that should find them all.
        let filter = AclFilter {
            resource_type: resource_type::TOPIC,
            resource_name: Some("orders".to_string()),
            pattern_type: pattern_type::MATCH,
            ..any_filter()
        };
        let selected: Vec<AclBinding> = bindings_for(&all_permissions())
            .into_iter()
            .filter(|binding| filter.matches(binding, "alice"))
            .collect();
        assert!(
            !selected.is_empty(),
            "a MATCH filter must find the wildcard bindings that cover the named topic"
        );
        assert!(
            selected
                .iter()
                .all(|binding| binding.resource_type == resource_type::TOPIC)
        );
    }

    #[test]
    fn given_a_literal_pattern_filter_with_a_name_should_not_select_the_wildcard() {
        // The widening belongs to MATCH alone. A LITERAL filter means the name exactly.
        let filter = AclFilter {
            resource_name: Some("orders".to_string()),
            pattern_type: pattern_type::LITERAL,
            ..any_filter()
        };
        let bindings = bindings_for(&all_permissions());
        assert!(!bindings.iter().any(|b| filter.matches(b, "alice")));
    }

    #[test]
    fn given_a_resource_name_filter_should_distinguish_wildcard_from_cluster() {
        let filter = AclFilter {
            resource_name: Some(CLUSTER_NAME.to_string()),
            ..any_filter()
        };
        let selected: Vec<AclBinding> = bindings_for(&all_permissions())
            .into_iter()
            .filter(|binding| filter.matches(binding, "alice"))
            .collect();
        assert!(
            selected
                .iter()
                .all(|binding| binding.resource_type == resource_type::CLUSTER)
        );
        assert!(!selected.is_empty());
    }
}
