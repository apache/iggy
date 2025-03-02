use std::time::Duration;

use bytes::Bytes;
use iggy::messages::send_messages::Message;

//TODO: Fix me
pub fn put_timestamp_in_first_message(message: &mut Message) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;

    // Convert current payload to Vec<u8>
    let mut payload_vec = message.payload.to_vec();

    // Ensure payload is at least 8 bytes
    if payload_vec.len() < 8 {
        let mut new_payload = vec![0u8; 8];
        new_payload.extend_from_slice(&payload_vec);
        payload_vec = new_payload;
    }

    // Put timestamp in first 8 bytes
    let timestamp_bytes = now.to_le_bytes();
    payload_vec[0..8].copy_from_slice(&timestamp_bytes);

    // Convert back to Bytes
    message.payload = Bytes::from(payload_vec);
}

/*
pub fn calculate_latency_from_first_message(message: &PolledMessage) -> Duration {
    let send_timestamp = u64::from_le_bytes(message.payload[0..8].try_into().unwrap());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    Duration::from_micros(now - send_timestamp)
}
*/
