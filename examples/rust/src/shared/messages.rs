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

use iggy::prelude::IggyTimestamp;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};

pub const ORDER_CREATED_TYPE: &str = "order_created";
pub const ORDER_CONFIRMED_TYPE: &str = "order_confirmed";
pub const ORDER_REJECTED_TYPE: &str = "order_rejected";

pub trait SerializableMessage: Debug {
    fn get_message_type(&self) -> &str;
    fn to_json(&self) -> String;
    fn to_json_envelope(&self) -> String;
}

// The message envelope can be used to send the different types of messages to the same topic.
#[derive(Debug, Deserialize, Serialize)]
pub struct Envelope {
    pub message_type: String,
    pub payload: String,
}

impl Envelope {
    pub fn new<T>(message_type: &str, payload: &T) -> Envelope
    where
        T: Serialize,
    {
        Envelope {
            message_type: message_type.to_string(),
            payload: serde_json::to_string(payload).unwrap(),
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

#[derive(Deserialize, Serialize)]
pub struct OrderCreated {
    pub order_id: u64,
    pub currency_pair: String,
    pub price: f64,
    pub quantity: f64,
    pub side: String,
    pub timestamp: IggyTimestamp,
}

impl Debug for OrderCreated {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderCreated")
            .field("order_id", &self.order_id)
            .field("currency_pair", &self.currency_pair)
            .field("price", &format!("{:.2}", self.price))
            .field("quantity", &format!("{:.2}", self.quantity))
            .field("side", &self.side)
            .field("timestamp", &self.timestamp.as_micros())
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
pub struct OrderConfirmed {
    pub order_id: u64,
    pub price: f64,
    pub timestamp: IggyTimestamp,
}

impl Debug for OrderConfirmed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderConfirmed")
            .field("order_id", &self.order_id)
            .field("price", &format!("{:.2}", self.price))
            .field("timestamp", &self.timestamp.as_micros())
            .finish()
    }
}

#[derive(Deserialize, Serialize)]
pub struct OrderRejected {
    pub order_id: u64,
    pub timestamp: IggyTimestamp,
    pub reason: String,
}

impl Debug for OrderRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderRejected")
            .field("order_id", &self.order_id)
            .field("timestamp", &self.timestamp.as_micros())
            .field("reason", &self.reason)
            .finish()
    }
}

impl SerializableMessage for OrderCreated {
    fn get_message_type(&self) -> &str {
        ORDER_CREATED_TYPE
    }

    fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    fn to_json_envelope(&self) -> String {
        Envelope::new(ORDER_CREATED_TYPE, self).to_json()
    }
}

impl SerializableMessage for OrderConfirmed {
    fn get_message_type(&self) -> &str {
        ORDER_CONFIRMED_TYPE
    }

    fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    fn to_json_envelope(&self) -> String {
        Envelope::new(ORDER_CONFIRMED_TYPE, self).to_json()
    }
}

impl SerializableMessage for OrderRejected {
    fn get_message_type(&self) -> &str {
        ORDER_REJECTED_TYPE
    }

    fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    fn to_json_envelope(&self) -> String {
        Envelope::new(ORDER_REJECTED_TYPE, self).to_json()
    }
}
