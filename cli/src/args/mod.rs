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

use std::path::PathBuf;

use clap::{Args, Command as ClapCommand};
use clap::{Parser, Subcommand};
use clap_complete::{generate, Generator, Shell};
use figlet_rs::FIGfont;

use iggy::args::{Args as IggyArgs, ArgsOptional as IggyArgsOptional};
use iggy::cli::context::common::ContextConfig;
use system::SnapshotArgs;

use crate::args::{
    client::ClientAction,
    consumer_group::ConsumerGroupAction,
    consumer_offset::ConsumerOffsetAction,
    context::ContextAction,
    message::MessageAction,
    partition::PartitionAction,
    personal_access_token::PersonalAccessTokenAction,
    stream::StreamAction,
    system::{PingArgs, StatsArgs},
    topic::TopicAction,
};

#[cfg(feature = "login-session")]
use crate::args::system::LoginArgs;

use self::user::UserAction;

pub(crate) mod client;
pub(crate) mod common;
pub(crate) mod consumer_group;
pub(crate) mod consumer_offset;
pub(crate) mod context;
pub(crate) mod message;
pub(crate) mod partition;
pub(crate) mod permissions;
pub(crate) mod personal_access_token;
pub(crate) mod stream;
pub(crate) mod system;
pub(crate) mod topic;
pub(crate) mod user;

static CARGO_BIN_NAME: &str = env!("CARGO_BIN_NAME");
static CARGO_PKG_HOMEPAGE: &str = env!("CARGO_PKG_HOMEPAGE");

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub(crate) struct IggyConsoleArgs {
    #[clap(flatten, verbatim_doc_comment)]
    pub(crate) iggy: IggyArgsOptional,

    #[clap(flatten, verbatim_doc_comment)]
    pub(crate) cli: CliOptions,

    #[clap(subcommand)]
    pub(crate) command: Option<Command>,
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub(crate) struct CliOptions {
    /// Quiet mode (disabled stdout printing)
    #[clap(short, long, default_value_t = false)]
    pub(crate) quiet: bool,

    /// Debug mode (verbose printing to given file)
    #[clap(short, long)]
    pub(crate) debug: Option<PathBuf>,

    /// Iggy server username
    #[clap(short, long, group = "credentials")]
    pub(crate) username: Option<String>,

    /// Iggy server password
    ///
    /// An optional parameter to specify the password for authentication.
    /// If not provided, user will be prompted interactively to enter the
    /// password securely.
    #[clap(short, long, verbatim_doc_comment)]
    pub(crate) password: Option<String>,

    /// Iggy server personal access token
    #[clap(short, long, group = "credentials")]
    pub(crate) token: Option<String>,

    #[cfg(feature = "login-session")]
    /// Iggy server personal access token name
    ///
    /// When personal access token is created using command line tool and stored
    /// inside platform-specific secure storage its name can be used as a value
    /// for this option without revealing the token value.
    #[clap(short = 'n', long, group = "credentials", verbatim_doc_comment)]
    pub(crate) token_name: Option<String>,

    /// Shell completion generator for iggy command
    ///
    /// Option prints shell completion code on standard output for selected shell.
    /// Redirect standard output to file and follow and use selected shell means
    /// to enable completion for iggy command.
    /// Option cannot be combined with other options.
    ///
    /// Example:
    ///  source <(iggy --generate bash)
    /// or
    ///  iggy --generate bash > iggy_completion.bash
    ///  source iggy_completion.bash
    #[clap(verbatim_doc_comment)]
    #[clap(long = "generate", value_enum)]
    pub(crate) generator: Option<Shell>,
}

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum Command {
    /// stream operations
    #[command(subcommand, visible_alias = "s")]
    Stream(StreamAction),
    /// topic operations
    #[command(subcommand, visible_alias = "t")]
    Topic(TopicAction),
    /// partition operations
    #[command(subcommand, visible_alias = "p")]
    Partition(PartitionAction),
    /// ping iggy server
    ///
    /// Check if iggy server is up and running and what's the response ping response time
    #[clap(verbatim_doc_comment)]
    Ping(PingArgs),
    /// get current client info
    ///
    /// Command connects to Iggy server and collects client info like client ID, user ID
    /// server address and protocol type.
    #[clap(verbatim_doc_comment)]
    Me,
    /// get iggy server statistics
    ///
    /// Collect basic Iggy server statistics like number of streams, topics, partitions, etc.
    /// Server OS name, version, etc. are also collected.
    #[clap(verbatim_doc_comment)]
    Stats(StatsArgs),
    /// collect iggy server troubleshooting data
    #[clap(verbatim_doc_comment)]
    Snapshot(SnapshotArgs),
    /// personal access token operations
    #[command(subcommand)]
    Pat(PersonalAccessTokenAction),
    /// user operations
    #[command(subcommand, visible_alias = "u")]
    User(UserAction),
    /// client operations
    #[command(subcommand, visible_alias = "c")]
    Client(ClientAction),
    /// consumer group operations
    #[command(subcommand, visible_alias = "g")]
    ConsumerGroup(ConsumerGroupAction),
    /// consumer offset operations
    #[command(subcommand, visible_alias = "o")]
    ConsumerOffset(ConsumerOffsetAction),
    /// message operations
    #[command(subcommand, visible_alias = "m")]
    Message(MessageAction),
    /// context operations
    #[command(subcommand, visible_alias = "ctx")]
    Context(ContextAction),
    #[cfg(feature = "login-session")]
    /// login to Iggy server
    ///
    /// Command logs in to Iggy server using provided credentials and stores session token
    /// in platform-specific secure storage. Session token is used for authentication in
    /// subsequent commands until logout command is executed.
    #[clap(verbatim_doc_comment, visible_alias = "li")]
    Login(LoginArgs),
    #[cfg(feature = "login-session")]
    /// logout from Iggy server
    ///
    /// Command logs out from Iggy server and removes session token from platform-specific
    /// secure storage. After logout command is executed, user needs to log in again to
    /// execute any command that requires authentication.
    #[clap(verbatim_doc_comment, visible_alias = "lo")]
    Logout,
}

impl IggyConsoleArgs {
    pub(crate) fn generate_completion<G: Generator>(&self, generator: G) {
        generate(
            generator,
            &mut IggyConsoleArgs::augment_args_for_update(
                ClapCommand::new(CARGO_BIN_NAME).bin_name(CARGO_BIN_NAME),
            ),
            CARGO_BIN_NAME,
            &mut std::io::stdout(),
        );
    }

    pub(crate) fn print_overview() {
        let mut cli = IggyConsoleArgs::augment_args_for_update(
            ClapCommand::new(CARGO_BIN_NAME).bin_name(CARGO_BIN_NAME),
        );

        let full_help = cli.render_help().to_string();
        let help = full_help.replace(
            &full_help[full_help.find("Options:").unwrap()..full_help.len()],
            "",
        );

        let standard_font = FIGfont::standard().unwrap();
        let figure = standard_font.convert("Iggy CLI").unwrap();

        println!("{figure}");
        println!("{help}");
        println!("Run '{CARGO_BIN_NAME} --help' for full help message.");
        println!("Run '{CARGO_BIN_NAME} COMMAND --help' for more information on a command.");
        println!();
        println!("For more help on what's Iggy and how to use it, head to {CARGO_PKG_HOMEPAGE}");
    }
}

pub struct IggyMergedConsoleArgs {
    pub iggy: IggyArgs,
    pub cli: CliOptions,
}

impl IggyMergedConsoleArgs {
    pub fn from_context(context: ContextConfig, args: IggyConsoleArgs) -> Self {
        let merged_cli_options = CliOptions {
            quiet: args.cli.quiet,
            debug: args.cli.debug,
            username: args.cli.username.or(context.username),
            password: args.cli.password.or(context.password),
            token: args.cli.token.or(context.token),
            #[cfg(feature = "login-session")]
            token_name: args.cli.token_name.or(context.token_name),
            generator: args.cli.generator,
        };

        Self {
            iggy: IggyArgs::from(vec![context.iggy, args.iggy]),
            cli: merged_cli_options,
        }
    }
}
