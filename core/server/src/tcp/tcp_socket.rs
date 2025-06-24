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

use std::num::TryFromIntError;


use crate::configs::tcp::TcpSocketConfig;

pub fn build(ipv6: bool, config: TcpSocketConfig) -> TcpSocket {
    let socket = if ipv6 {
        TcpSocket::new_v6().expect("Unable to create an ipv6 socket")
    } else {
        TcpSocket::new_v4().expect("Unable to create an ipv4 socket")
    };

    if config.override_defaults {
        config
            .recv_buffer_size
            .as_bytes_u64()
            .try_into()
            .map_err(|e: TryFromIntError| std::io::Error::other(e.to_string()))
            .and_then(|size: u32| socket.set_recv_buffer_size(size))
            .expect("Unable to set SO_RCVBUF on socket");
        config
            .send_buffer_size
            .as_bytes_u64()
            .try_into()
            .map_err(|e: TryFromIntError| std::io::Error::other(e.to_string()))
            .and_then(|size| socket.set_send_buffer_size(size))
            .expect("Unable to set SO_SNDBUF on socket");
        socket
            .set_keepalive(config.keepalive)
            .expect("Unable to set SO_KEEPALIVE on socket");
        socket
            .set_nodelay(config.nodelay)
            .expect("Unable to set TCP_NODELAY on socket");
        socket
            .set_linger(Some(config.linger.get_duration()))
            .expect("Unable to set SO_LINGER on socket");
    }

    socket
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iggy_common::{IggyByteSize, IggyDuration};

    use super::*;

    #[test]
    fn given_override_defaults_socket_should_be_configured() {
        let buffer_size = 425984;
        let linger_dur = Duration::new(1, 0);
        let config = TcpSocketConfig {
            override_defaults: true,
            recv_buffer_size: IggyByteSize::from(buffer_size),
            send_buffer_size: IggyByteSize::from(buffer_size),
            keepalive: true,
            nodelay: true,
            linger: IggyDuration::new(linger_dur),
        };
        let socket = build(false, config);
        assert!(socket.recv_buffer_size().unwrap() >= buffer_size as u32);
        assert!(socket.send_buffer_size().unwrap() >= buffer_size as u32);
        assert!(socket.keepalive().unwrap());
        assert!(socket.nodelay().unwrap());
        assert_eq!(socket.linger().unwrap(), Some(linger_dur));
    }
}
