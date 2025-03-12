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

use crate::shared::messages::{OrderConfirmed, OrderCreated, OrderRejected, SerializableMessage};
use iggy::utils::timestamp::IggyTimestamp;
use rand::rngs::ThreadRng;
use rand::Rng;

const CURRENCY_PAIRS: &[&str] = &["EUR/USD", "EUR/GBP", "USD/GBP", "EUR/PLN", "USD/PLN"];

#[derive(Debug, Default)]
pub struct MessagesGenerator {
    order_id: u64,
    rng: ThreadRng,
}

impl MessagesGenerator {
    pub fn new() -> MessagesGenerator {
        MessagesGenerator {
            order_id: 0,
            rng: rand::rng(),
        }
    }

    pub fn generate(&mut self) -> Box<dyn SerializableMessage> {
        match self.rng.random_range(0..=2) {
            0 => self.generate_order_created(),
            1 => self.generate_order_confirmed(),
            2 => self.generate_order_rejected(),
            _ => panic!("Unexpected message type"),
        }
    }

    fn generate_order_created(&mut self) -> Box<dyn SerializableMessage> {
        self.order_id += 1;
        Box::new(OrderCreated {
            order_id: self.order_id,
            timestamp: IggyTimestamp::now(),
            currency_pair: CURRENCY_PAIRS[self.rng.random_range(0..CURRENCY_PAIRS.len())]
                .to_string(),
            price: self.rng.random_range(10.0..=1000.0),
            quantity: self.rng.random_range(0.1..=1.0),
            side: match self.rng.random_range(0..=1) {
                0 => "buy",
                _ => "sell",
            }
            .to_string(),
        })
    }

    fn generate_order_confirmed(&mut self) -> Box<dyn SerializableMessage> {
        Box::new(OrderConfirmed {
            order_id: self.order_id,
            timestamp: IggyTimestamp::now(),
            price: self.rng.random_range(10.0..=1000.0),
        })
    }

    fn generate_order_rejected(&mut self) -> Box<dyn SerializableMessage> {
        Box::new(OrderRejected {
            order_id: self.order_id,
            timestamp: IggyTimestamp::now(),
            reason: match self.rng.random_range(0..=1) {
                0 => "cancelled_by_user",
                _ => "other",
            }
            .to_string(),
        })
    }
}
