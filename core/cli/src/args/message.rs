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

use clap::builder::NonEmptyStringValueParser;
use clap::{ArgGroup, Args, Subcommand};
use iggy::prelude::*;
use std::str::FromStr;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum MessageAction {
    /// Send messages to given topic ID and given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy message send 1 2 message
    ///  iggy message send stream 2 "long message"
    ///  iggy message send 1 topic message1 message2 message3
    ///  iggy message send stream topic "long message with spaces"
    #[clap(verbatim_doc_comment, visible_alias = "s")]
    Send(SendMessagesArgs),
    /// Poll messages from given topic ID and given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples:
    ///  iggy message poll --offset 0 1 2 1
    ///  iggy message poll --offset 0 stream 2 1
    ///  iggy message poll --offset 0 1 topic 1
    ///  iggy message poll --offset 0 stream topic 1
    #[clap(verbatim_doc_comment, visible_alias = "p")]
    Poll(PollMessagesArgs),
    /// Flush messages from given topic ID and given stream ID
    ///
    /// Command is used to force a flush of unsaved_buffer to disk
    /// for specific stream, topic and partition. If fsync is enabled
    /// then the data is flushed to disk and fsynced, otherwise the
    /// data is only flushed to disk.
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples:
    ///  iggy message flush 1 2 1
    ///  iggy message flush stream 2 1
    ///  iggy message flush 1 topic 1
    ///  iggy message flush stream topic 1
    #[clap(verbatim_doc_comment, visible_alias = "f")]
    Flush(FlushMessagesArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct SendMessagesArgs {
    /// ID of the stream to which the message will be sent
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// ID of the topic to which the message will be sent
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// ID of the partition to which the message will be sent
    #[clap(short, long, group = "partitioning")]
    pub(crate) partition_id: Option<u32>,
    /// Messages key which will be used to partition the messages
    ///
    /// Value of the key will be used by the server to calculate the partition ID
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, group = "partitioning")]
    pub(crate) message_key: Option<String>,
    /// Messages to be sent
    ///
    /// If no messages are provided, the command will read the messages from the
    /// standard input and each line will be sent as a separate message.
    /// If messages are provided, they will be sent as is. If message contains
    /// spaces, it should be enclosed in quotes. Limit of the messages and size
    /// of each message is defined by the used shell.
    #[clap(verbatim_doc_comment)]
    #[clap(group = "input_messages")]
    pub(crate) messages: Option<Vec<String>>,
    /// Comma separated list of key:kind:value, sent as header with the message
    ///
    /// Headers are comma separated key-value pairs that can be sent with the message.
    /// Kind can be one of the following: raw, string, bool, int8, int16, int32, int64,
    /// int128, uint8, uint16, uint32, uint64, uint128, float32, float64
    #[clap(verbatim_doc_comment)]
    #[clap(short = 'H', long, value_parser = parse_key_val, value_delimiter = ',')]
    pub(crate) headers: Vec<(HeaderKey, HeaderValue)>,
    /// Input file with messages to be sent
    ///
    /// File should contain messages stored in binary format. If the file does
    /// not exist, the command will fail. If the file is not specified, the command
    /// will read the messages from the standard input and each line will
    /// be sent as a separate message. If the file is specified, the messages
    /// will be read from the file and sent as is. Option cannot be used
    /// with the messages option (messages given as command line arguments).
    #[clap(verbatim_doc_comment)]
    #[clap(long, value_parser = NonEmptyStringValueParser::new(), group = "input_messages")]
    pub(crate) input_file: Option<String>,
}

/// Parse Header Key, Kind and Value from the string separated by a ':'
fn parse_key_val(s: &str) -> Result<(HeaderKey, HeaderValue), IggyError> {
    let lower = s.to_lowercase();
    let parts = lower.split(':').collect::<Vec<_>>();

    if parts.len() != 3 {
        return Err(IggyError::InvalidFormat);
    }

    let key = HeaderKey::from_str(parts[0])?;
    let value = HeaderValue::from_kind_str_and_value_str(parts[1], parts[2])?;
    Ok((key, value))
}

#[derive(Debug, Clone, Args)]
#[command(group = ArgGroup::new("polling_strategy").required(true))]
pub(crate) struct PollMessagesArgs {
    /// ID of the stream from which message will be polled
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// ID of the topic from which message will be polled
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Partition ID from which message will be polled
    #[arg(value_parser = clap::value_parser!(u32).range(0..))]
    pub(crate) partition_id: u32,
    /// Number of messages to poll
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    pub(crate) message_count: u32,
    /// Auto commit offset
    ///
    /// Flag indicates whether to commit offset on the server automatically
    /// after polling the messages.
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false)]
    pub(crate) auto_commit: bool,
    /// Polling strategy - offset to start polling messages from
    ///
    /// Offset must be specified as a number
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, group = "polling_strategy")]
    pub(crate) offset: Option<u64>,
    /// Polling strategy - start polling from the first message in the partition
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false, group = "polling_strategy")]
    pub(crate) first: bool,
    /// Polling strategy - start polling from the last message in the partition
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false, group = "polling_strategy")]
    pub(crate) last: bool,
    /// Polling strategy - start polling from the next message
    ///
    /// Start polling after the last polled message based
    /// on the stored consumer offset
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false, group = "polling_strategy")]
    pub(crate) next: bool,
    /// Regular consumer which will poll messages
    ///
    /// Consumer ID can be specified as a consumer name or ID
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = Identifier::default(), value_parser = clap::value_parser!(Identifier))]
    pub(crate) consumer: Identifier,
    /// Include the message headers in the output
    ///
    /// Flag indicates whether to include headers in the output
    /// after polling the messages.
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false)]
    pub(crate) show_headers: bool,
    /// Store polled message into file in binary format
    ///
    /// Polled messages will be stored in the file in binary format.
    /// File can be used to replay the messages later. If the file
    /// already exists, the messages will be appended to the file.
    /// If the file does not exist, it will be created.
    /// If the file is not specified, the messages will be printed
    /// to the standard output.
    #[clap(verbatim_doc_comment)]
    #[clap(long, value_parser = NonEmptyStringValueParser::new())]
    pub(crate) output_file: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct FlushMessagesArgs {
    /// ID of the stream for which messages will be flushed
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// ID of the topic for which messages will be flushed
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// Partition ID for which messages will be flushed
    #[arg(value_parser = clap::value_parser!(u32).range(0..))]
    pub(crate) partition_id: u32,
    /// fsync flushed data to disk
    ///
    /// If option is enabled then the data is flushed to disk and fsynced,
    /// otherwise the data is only flushed to disk. Default is false.
    #[clap(verbatim_doc_comment)]
    #[clap(short, long, default_value_t = false)]
    pub(crate) fsync: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn parse_key_val_should_parse_string() {
        let expected_value: &str = "value";
        let result = parse_key_val(&format!("key:String:{expected_value}"));
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key, HeaderKey::from_str("key").unwrap());
        assert_eq!(value.as_str().unwrap(), expected_value);
    }

    #[test]
    fn parse_key_val_should_parse_uint8() {
        let expected_value: u8 = 4;
        let result = parse_key_val(&format!("key:uint8:{expected_value}"));
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key, HeaderKey::from_str("key").unwrap());
        assert_eq!(value.as_uint8().unwrap(), expected_value);
    }

    #[test]
    fn parse_key_val_should_parse_float64() {
        let expected_value: f64 = 42.0;
        let result = parse_key_val(&format!("key:float64:{expected_value}"));
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key, HeaderKey::from_str("key").unwrap());
        assert_eq!(value.as_float64().unwrap(), expected_value);
    }

    #[test]
    fn parse_key_val_should_parse_bool() {
        let expected_value = true;
        let result = parse_key_val(&format!("key:bool:{expected_value}"));
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key, HeaderKey::from_str("key").unwrap());
        assert_eq!(value.as_bool().unwrap(), expected_value);
    }

    #[test]
    fn parse_key_val_to_less_params_should_return_err() {
        let result = parse_key_val("key:string");
        assert!(result.is_err());
    }

    #[test]
    fn parse_key_val_wrong_kind_should_return_err() {
        let result = parse_key_val("key:strin:value");
        assert!(result.is_err());
    }

    #[test]
    fn parse_key_val_no_matching_value_should_return_err() {
        let result = parse_key_val("key:uint8:69.42");
        assert!(result.is_err());
    }
}
