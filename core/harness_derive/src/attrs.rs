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

//! Attribute parsing for `#[iggy_harness(...)]`.
//!
//! Supports:
//! - `test_client_transport = Tcp` (single)
//! - `test_client_transport = [Tcp, Http]` (matrix)
//! - `server(path.to.field = "value")` (static)
//! - `server(path.to.field = ["v1", "v2"])` (matrix)

use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Ident, LitStr, Token, bracketed, parenthesized};

/// Cluster node configuration.
#[derive(Debug, Clone, Default)]
pub enum ClusterNodesValue {
    #[default]
    None,
    Single(usize),
    Matrix(Vec<usize>),
}

impl ClusterNodesValue {
    pub fn variants(&self) -> Vec<Option<usize>> {
        match self {
            ClusterNodesValue::None => vec![None],
            ClusterNodesValue::Single(n) => vec![Some(*n)],
            ClusterNodesValue::Matrix(v) => v.iter().map(|n| Some(*n)).collect(),
        }
    }
}

/// Parsed `#[iggy_harness(...)]` attributes.
#[derive(Debug, Default)]
pub struct IggyTestAttrs {
    pub transports: Vec<Transport>,
    /// True if `test_client_transport` was explicitly provided in attributes.
    pub transport_explicit: bool,
    pub server: ServerAttrs,
    pub seed_fn: Option<syn::Path>,
    pub cluster_nodes: ClusterNodesValue,
}

/// MCP configuration attributes.
#[derive(Debug, Default, Clone)]
pub struct McpAttrs {
    pub consumer_name: Option<String>,
    pub http_path: Option<String>,
}

/// Connectors runtime configuration attributes.
#[derive(Debug, Default, Clone)]
pub struct ConnectorsRuntimeAttrs {
    pub config_path: Option<String>,
}

#[cfg(test)]
impl IggyTestAttrs {
    /// Create attrs with specified transports and default server config.
    pub fn with_transports(transports: Vec<Transport>) -> Self {
        Self {
            transports,
            transport_explicit: true,
            server: ServerAttrs::default(),
            seed_fn: None,
            cluster_nodes: ClusterNodesValue::None,
        }
    }
}

/// Transport protocol variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Tcp,
    TcpTlsSelfSigned,
    TcpTlsGenerated,
    Http,
    Quic,
    WebSocket,
    WebSocketTlsSelfSigned,
    WebSocketTlsGenerated,
}

impl Transport {
    pub fn as_str(&self) -> &'static str {
        match self {
            Transport::Tcp => "tcp",
            Transport::TcpTlsSelfSigned => "tcp_tls_self_signed",
            Transport::TcpTlsGenerated => "tcp_tls_generated",
            Transport::Http => "http",
            Transport::Quic => "quic",
            Transport::WebSocket => "websocket",
            Transport::WebSocketTlsSelfSigned => "websocket_tls_self_signed",
            Transport::WebSocketTlsGenerated => "websocket_tls_generated",
        }
    }

    pub fn variant_ident(&self) -> Ident {
        let name = match self {
            Transport::Tcp | Transport::TcpTlsSelfSigned | Transport::TcpTlsGenerated => "Tcp",
            Transport::Http => "Http",
            Transport::Quic => "Quic",
            Transport::WebSocket
            | Transport::WebSocketTlsSelfSigned
            | Transport::WebSocketTlsGenerated => "WebSocket",
        };
        Ident::new(name, Span::call_site())
    }

    pub fn client_config_method(&self) -> &'static str {
        match self {
            Transport::Tcp | Transport::TcpTlsSelfSigned | Transport::TcpTlsGenerated => "root_tcp",
            Transport::Http => "root_http",
            Transport::Quic => "root_quic",
            Transport::WebSocket
            | Transport::WebSocketTlsSelfSigned
            | Transport::WebSocketTlsGenerated => "root_websocket",
        }
    }

    /// Returns the TLS mode if this transport uses TLS.
    pub fn tls_mode(&self) -> Option<TlsMode> {
        match self {
            Transport::TcpTlsSelfSigned | Transport::WebSocketTlsSelfSigned => {
                Some(TlsMode::SelfSigned)
            }
            Transport::TcpTlsGenerated | Transport::WebSocketTlsGenerated => {
                Some(TlsMode::Generated)
            }
            _ => None,
        }
    }

    /// Returns true if this transport uses WebSocket protocol.
    pub fn is_websocket(&self) -> bool {
        matches!(
            self,
            Transport::WebSocket
                | Transport::WebSocketTlsSelfSigned
                | Transport::WebSocketTlsGenerated
        )
    }
}

/// A single config override with dot-notation path.
#[derive(Debug, Clone)]
pub struct ConfigOverride {
    pub path: String,
    pub value: ConfigValue,
}

/// Server configuration attributes.
#[derive(Debug, Default)]
pub struct ServerAttrs {
    /// Dynamic config overrides using dot-notation paths.
    pub config_overrides: Vec<ConfigOverride>,

    /// Special cases requiring custom codegen.
    pub mcp: Option<McpAttrs>,
    pub connectors_runtime: Option<ConnectorsRuntimeAttrs>,
    pub tls: Option<TlsConfig>,
    pub websocket_tls: Option<TlsConfig>,
}

impl ServerAttrs {
    /// Find a config override by its path. Used in tests.
    #[cfg(test)]
    pub fn find_override(&self, path: &str) -> Option<&ConfigOverride> {
        self.config_overrides.iter().find(|o| o.path == path)
    }
}

/// TLS configuration mode for server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// Server generates its own self-signed certs. Client cannot validate.
    SelfSigned,
    /// Harness generates test certs. Client can validate using CA cert.
    Generated,
}

/// Parsed TLS configuration from attributes.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub mode: TlsMode,
}

/// A configuration value that can be static or a matrix.
#[derive(Debug, Default, Clone)]
pub enum ConfigValue {
    #[default]
    Unset,
    Static(String),
    Matrix(Vec<String>),
}

impl ConfigValue {
    pub fn variants(&self) -> Vec<Option<&str>> {
        match self {
            ConfigValue::Unset => vec![None],
            ConfigValue::Static(s) => vec![Some(s.as_str())],
            ConfigValue::Matrix(v) => v.iter().map(|s| Some(s.as_str())).collect(),
        }
    }
}

impl Parse for IggyTestAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut attrs = IggyTestAttrs::default();

        if input.is_empty() {
            attrs.transports.push(Transport::Tcp);
            return Ok(attrs);
        }

        let items: Punctuated<AttrItem, Token![,]> = Punctuated::parse_terminated(input)?;

        for item in items {
            match item {
                AttrItem::Transport(transports) => {
                    attrs.transports = transports;
                    attrs.transport_explicit = true;
                }
                AttrItem::Server(server) => {
                    attrs.server = *server;
                }
                AttrItem::Seed(path) => {
                    attrs.seed_fn = Some(path);
                }
                AttrItem::ClusterNodes(cluster) => {
                    attrs.cluster_nodes = cluster;
                }
            }
        }

        if attrs.transports.is_empty() {
            attrs.transports.push(Transport::Tcp);
        }

        Ok(attrs)
    }
}

enum AttrItem {
    Transport(Vec<Transport>),
    Server(Box<ServerAttrs>),
    Seed(syn::Path),
    ClusterNodes(ClusterNodesValue),
}

impl Parse for AttrItem {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;
        let ident_str = ident.to_string();

        match ident_str.as_str() {
            "test_client_transport" => {
                input.parse::<Token![=]>()?;
                let transports = parse_transport_value(input)?;
                Ok(AttrItem::Transport(transports))
            }
            "cluster_nodes" => {
                input.parse::<Token![=]>()?;
                let cluster = parse_cluster_nodes_value(input)?;
                Ok(AttrItem::ClusterNodes(cluster))
            }
            "server" => {
                let content;
                parenthesized!(content in input);
                let server = parse_server_attrs(&content)?;
                Ok(AttrItem::Server(Box::new(server)))
            }
            "seed" => {
                input.parse::<Token![=]>()?;
                let path: syn::Path = input.parse()?;
                Ok(AttrItem::Seed(path))
            }
            _ => Err(syn::Error::new(
                ident.span(),
                format!("unknown attribute: {ident_str}"),
            )),
        }
    }
}

fn parse_transport_value(input: ParseStream) -> syn::Result<Vec<Transport>> {
    if input.peek(syn::token::Bracket) {
        let content;
        bracketed!(content in input);
        let idents: Punctuated<Ident, Token![,]> = Punctuated::parse_terminated(&content)?;
        idents.into_iter().map(parse_transport_ident).collect()
    } else {
        let ident: Ident = input.parse()?;
        Ok(vec![parse_transport_ident(ident)?])
    }
}

fn parse_cluster_nodes_value(input: ParseStream) -> syn::Result<ClusterNodesValue> {
    if input.peek(syn::token::Bracket) {
        let content;
        bracketed!(content in input);
        let values: Punctuated<syn::LitInt, Token![,]> = Punctuated::parse_terminated(&content)?;
        let nodes: Result<Vec<usize>, _> = values
            .into_iter()
            .map(|lit| lit.base10_parse::<usize>())
            .collect();
        Ok(ClusterNodesValue::Matrix(nodes?))
    } else {
        let lit: syn::LitInt = input.parse()?;
        let n = lit.base10_parse::<usize>()?;
        Ok(ClusterNodesValue::Single(n))
    }
}

fn parse_transport_ident(ident: Ident) -> syn::Result<Transport> {
    match ident.to_string().as_str() {
        "Tcp" => Ok(Transport::Tcp),
        "TcpTlsSelfSigned" => Ok(Transport::TcpTlsSelfSigned),
        "TcpTlsGenerated" => Ok(Transport::TcpTlsGenerated),
        "Http" => Ok(Transport::Http),
        "Quic" => Ok(Transport::Quic),
        "WebSocket" => Ok(Transport::WebSocket),
        "WebSocketTlsSelfSigned" => Ok(Transport::WebSocketTlsSelfSigned),
        "WebSocketTlsGenerated" => Ok(Transport::WebSocketTlsGenerated),
        other => Err(syn::Error::new(
            ident.span(),
            format!("unknown transport: {other}"),
        )),
    }
}

/// Parses a dot-notation config key like `segment.size` or `partition.messages_required_to_save`.
fn parse_config_key(input: ParseStream) -> syn::Result<(String, Span)> {
    let first: Ident = input.parse()?;
    let span = first.span();
    let mut path = first.to_string();

    while input.peek(Token![.]) {
        input.parse::<Token![.]>()?;
        let next: Ident = input.parse()?;
        path.push('.');
        path.push_str(&next.to_string());
    }

    Ok((path, span))
}

fn parse_tls_value(input: ParseStream, span: Span) -> syn::Result<TlsConfig> {
    let lit: Ident = input.parse()?;
    let mode = match lit.to_string().as_str() {
        "self_signed" | "SelfSigned" => TlsMode::SelfSigned,
        "generated" | "Generated" => TlsMode::Generated,
        other => {
            return Err(syn::Error::new(
                span,
                format!("unknown tls mode: {other}, expected 'self_signed' or 'generated'"),
            ));
        }
    };
    Ok(TlsConfig { mode })
}

fn parse_server_attrs(input: ParseStream) -> syn::Result<ServerAttrs> {
    let mut server = ServerAttrs::default();

    while !input.is_empty() {
        let (key, span) = parse_config_key(input)?;

        match key.as_str() {
            "mcp" => {
                let mcp = if input.peek(syn::token::Paren) {
                    let content;
                    parenthesized!(content in input);
                    parse_mcp_attrs(&content)?
                } else {
                    McpAttrs::default()
                };
                server.mcp = Some(mcp);
            }
            "connectors_runtime" => {
                let attrs = if input.peek(syn::token::Paren) {
                    let content;
                    parenthesized!(content in input);
                    parse_connectors_runtime_attrs(&content)?
                } else {
                    ConnectorsRuntimeAttrs::default()
                };
                server.connectors_runtime = Some(attrs);
            }
            "tls" => {
                input.parse::<Token![=]>()?;
                server.tls = Some(parse_tls_value(input, span)?);
            }
            "websocket_tls" => {
                input.parse::<Token![=]>()?;
                server.websocket_tls = Some(parse_tls_value(input, span)?);
            }
            _ => {
                input.parse::<Token![=]>()?;
                let value = parse_config_value(input)?;
                server
                    .config_overrides
                    .push(ConfigOverride { path: key, value });
            }
        }

        if !input.is_empty() {
            input.parse::<Token![,]>()?;
        }
    }

    Ok(server)
}

fn parse_config_value(input: ParseStream) -> syn::Result<ConfigValue> {
    if input.peek(syn::token::Bracket) {
        let content;
        bracketed!(content in input);
        let values: Punctuated<ArrayLiteral, Token![,]> = Punctuated::parse_terminated(&content)?;
        Ok(ConfigValue::Matrix(
            values.into_iter().map(|v| v.to_string_value()).collect(),
        ))
    } else if input.peek(LitStr) {
        let lit: LitStr = input.parse()?;
        Ok(ConfigValue::Static(lit.value()))
    } else if input.peek(syn::LitBool) {
        let lit: syn::LitBool = input.parse()?;
        Ok(ConfigValue::Static(lit.value.to_string()))
    } else if input.peek(syn::LitInt) {
        let lit: syn::LitInt = input.parse()?;
        Ok(ConfigValue::Static(lit.base10_digits().to_string()))
    } else {
        Err(input.error("expected string literal, bool, int, or array"))
    }
}

fn parse_mcp_attrs(input: ParseStream) -> syn::Result<McpAttrs> {
    let mut mcp = McpAttrs::default();

    let items: Punctuated<KeyValueAttrItem, Token![,]> = Punctuated::parse_terminated(input)?;

    for item in items {
        match item.key.as_str() {
            "consumer_name" => mcp.consumer_name = Some(item.value),
            "http_path" => mcp.http_path = Some(item.value),
            other => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("unknown mcp attribute: {other}"),
                ));
            }
        }
    }

    Ok(mcp)
}

fn parse_connectors_runtime_attrs(input: ParseStream) -> syn::Result<ConnectorsRuntimeAttrs> {
    let mut attrs = ConnectorsRuntimeAttrs::default();

    let items: Punctuated<KeyValueAttrItem, Token![,]> = Punctuated::parse_terminated(input)?;

    for item in items {
        match item.key.as_str() {
            "config_path" => attrs.config_path = Some(item.value),
            other => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("unknown connectors_runtime attribute: {other}"),
                ));
            }
        }
    }

    Ok(attrs)
}

struct KeyValueAttrItem {
    key: String,
    value: String,
}

impl Parse for KeyValueAttrItem {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let lit: LitStr = input.parse()?;

        Ok(KeyValueAttrItem {
            key: ident.to_string(),
            value: lit.value(),
        })
    }
}

/// A literal that can be a string or an integer.
enum ArrayLiteral {
    Str(LitStr),
    Int(syn::LitInt),
}

impl Parse for ArrayLiteral {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(LitStr) {
            Ok(ArrayLiteral::Str(input.parse()?))
        } else if input.peek(syn::LitInt) {
            Ok(ArrayLiteral::Int(input.parse()?))
        } else {
            Err(input.error("expected string or integer literal"))
        }
    }
}

impl ArrayLiteral {
    fn to_string_value(&self) -> String {
        match self {
            ArrayLiteral::Str(s) => s.value(),
            ArrayLiteral::Int(i) => i.base10_digits().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty() {
        let attrs: IggyTestAttrs = syn::parse_quote!();
        assert_eq!(attrs.transports.len(), 1);
        assert_eq!(attrs.transports[0], Transport::Tcp);
    }

    #[test]
    fn parse_single_transport() {
        let attrs: IggyTestAttrs = syn::parse_quote!(test_client_transport = Http);
        assert_eq!(attrs.transports.len(), 1);
        assert_eq!(attrs.transports[0], Transport::Http);
    }

    #[test]
    fn parse_transport_array() {
        let attrs: IggyTestAttrs = syn::parse_quote!(test_client_transport = [Tcp, Http, Quic]);
        assert_eq!(attrs.transports.len(), 3);
        assert_eq!(attrs.transports[0], Transport::Tcp);
        assert_eq!(attrs.transports[1], Transport::Http);
        assert_eq!(attrs.transports[2], Transport::Quic);
    }

    #[test]
    fn parse_server_static() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(segment.size = "1MiB"));
        let segment_size = attrs.server.find_override("segment.size").unwrap();
        assert!(matches!(&segment_size.value, ConfigValue::Static(s) if s == "1MiB"));
    }

    #[test]
    fn parse_server_matrix() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(segment.size = ["512B", "1MiB"]));
        let segment_size = attrs.server.find_override("segment.size").unwrap();
        assert!(matches!(&segment_size.value, ConfigValue::Matrix(v) if v.len() == 2));
    }

    #[test]
    fn parse_full() {
        let attrs: IggyTestAttrs = syn::parse_quote!(
            test_client_transport = [Tcp, Http],
            server(
                segment.size = ["512B", "1MiB"],
                segment.cache_indexes = "none",
                tcp.socket.nodelay = true
            )
        );
        assert_eq!(attrs.transports.len(), 2);
        let segment_size = attrs.server.find_override("segment.size").unwrap();
        let cache_indexes = attrs.server.find_override("segment.cache_indexes").unwrap();
        let tcp_nodelay = attrs.server.find_override("tcp.socket.nodelay").unwrap();
        assert!(matches!(&segment_size.value, ConfigValue::Matrix(v) if v.len() == 2));
        assert!(matches!(&cache_indexes.value, ConfigValue::Static(s) if s == "none"));
        assert!(matches!(&tcp_nodelay.value, ConfigValue::Static(s) if s == "true"));
    }

    #[test]
    fn parse_tls_transports() {
        let attrs: IggyTestAttrs =
            syn::parse_quote!(test_client_transport = [TcpTlsSelfSigned, TcpTlsGenerated]);
        assert_eq!(attrs.transports.len(), 2);
        assert_eq!(attrs.transports[0], Transport::TcpTlsSelfSigned);
        assert_eq!(attrs.transports[1], Transport::TcpTlsGenerated);
    }

    #[test]
    fn parse_mcp_empty() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(mcp));
        assert!(attrs.server.mcp.is_some());
        let mcp = attrs.server.mcp.unwrap();
        assert!(mcp.consumer_name.is_none());
        assert!(mcp.http_path.is_none());
    }

    #[test]
    fn parse_mcp_with_consumer() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(mcp(consumer_name = "test-consumer")));
        assert!(attrs.server.mcp.is_some());
        let mcp = attrs.server.mcp.unwrap();
        assert_eq!(mcp.consumer_name, Some("test-consumer".to_string()));
    }

    #[test]
    fn parse_mcp_with_http_path() {
        let attrs: IggyTestAttrs =
            syn::parse_quote!(server(mcp(consumer_name = "test", http_path = "/custom")));
        assert!(attrs.server.mcp.is_some());
        let mcp = attrs.server.mcp.unwrap();
        assert_eq!(mcp.consumer_name, Some("test".to_string()));
        assert_eq!(mcp.http_path, Some("/custom".to_string()));
    }

    #[test]
    fn parse_seed() {
        let attrs: IggyTestAttrs = syn::parse_quote!(seed = my_seed_fn);
        assert!(attrs.seed_fn.is_some());
    }

    #[test]
    fn parse_mcp_with_seed() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(mcp), seed = crate::seeds::standard);
        assert!(attrs.server.mcp.is_some());
        assert!(attrs.seed_fn.is_some());
    }

    #[test]
    fn parse_mcp_combined() {
        let attrs: IggyTestAttrs =
            syn::parse_quote!(seed = my_seed, server(mcp, segment.size = "1MiB"));
        assert!(attrs.server.mcp.is_some());
        assert!(attrs.seed_fn.is_some());
        let segment_size = attrs.server.find_override("segment.size").unwrap();
        assert!(matches!(&segment_size.value, ConfigValue::Static(s) if s == "1MiB"));
    }

    #[test]
    fn parse_cluster_enabled() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(cluster.enabled = true));
        let cluster_enabled = attrs.server.find_override("cluster.enabled").unwrap();
        assert!(matches!(&cluster_enabled.value, ConfigValue::Static(s) if s == "true"));
    }

    #[test]
    fn parse_cluster_enabled_false() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(cluster.enabled = false));
        let cluster_enabled = attrs.server.find_override("cluster.enabled").unwrap();
        assert!(matches!(&cluster_enabled.value, ConfigValue::Static(s) if s == "false"));
    }

    #[test]
    fn parse_cluster_enabled_with_mcp() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(cluster.enabled = true, mcp));
        let cluster_enabled = attrs.server.find_override("cluster.enabled").unwrap();
        assert!(matches!(&cluster_enabled.value, ConfigValue::Static(s) if s == "true"));
        assert!(attrs.server.mcp.is_some());
    }

    #[test]
    fn parse_dot_notation_deep() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(
            partition.messages_required_to_save = [32, 64],
            system.encryption.enabled = true
        ));
        assert_eq!(attrs.server.config_overrides.len(), 2);
        let msgs = attrs
            .server
            .find_override("partition.messages_required_to_save")
            .unwrap();
        assert!(matches!(&msgs.value, ConfigValue::Matrix(v) if v.len() == 2));
    }

    #[test]
    fn parse_tls_self_signed() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(tls = self_signed));
        assert!(attrs.server.tls.is_some());
        assert_eq!(attrs.server.tls.unwrap().mode, TlsMode::SelfSigned);
    }

    #[test]
    fn parse_tls_generated() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(tls = generated));
        assert!(attrs.server.tls.is_some());
        assert_eq!(attrs.server.tls.unwrap().mode, TlsMode::Generated);
    }

    #[test]
    fn parse_websocket_tls() {
        let attrs: IggyTestAttrs = syn::parse_quote!(server(websocket_tls = generated));
        assert!(attrs.server.websocket_tls.is_some());
        assert_eq!(attrs.server.websocket_tls.unwrap().mode, TlsMode::Generated);
    }

    #[test]
    fn parse_cluster_nodes_single() {
        let attrs: IggyTestAttrs = syn::parse_quote!(cluster_nodes = 3);
        assert!(matches!(attrs.cluster_nodes, ClusterNodesValue::Single(3)));
    }

    #[test]
    fn parse_cluster_nodes_matrix() {
        let attrs: IggyTestAttrs = syn::parse_quote!(cluster_nodes = [3, 5]);
        assert!(matches!(&attrs.cluster_nodes, ClusterNodesValue::Matrix(v) if v == &[3, 5]));
    }

    #[test]
    fn parse_cluster_nodes_with_transport() {
        let attrs: IggyTestAttrs =
            syn::parse_quote!(cluster_nodes = 3, test_client_transport = Tcp);
        assert!(matches!(attrs.cluster_nodes, ClusterNodesValue::Single(3)));
        assert_eq!(attrs.transports.len(), 1);
        assert_eq!(attrs.transports[0], Transport::Tcp);
    }

    #[test]
    fn parse_cluster_nodes_full_matrix() {
        let attrs: IggyTestAttrs = syn::parse_quote!(
            cluster_nodes = [3, 5],
            test_client_transport = [Tcp, Http],
            server(segment.size = ["512B", "1MiB"])
        );
        assert!(matches!(&attrs.cluster_nodes, ClusterNodesValue::Matrix(v) if v == &[3, 5]));
        assert_eq!(attrs.transports.len(), 2);
        let segment_size = attrs.server.find_override("segment.size").unwrap();
        assert!(matches!(&segment_size.value, ConfigValue::Matrix(v) if v.len() == 2));
    }
}
