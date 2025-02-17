use iggy::clients::consumer::ReceivedMessage;
use iggy::consumer_ext::MessageConsumer;
use iggy::error::IggyError;

#[derive(Debug)]
pub struct PrintEventConsumer {}

impl MessageConsumer for PrintEventConsumer {
    async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
        // Extract message payload as raw bytes
        let raw_message = message.message.payload.as_ref();
        // Convert raw bytes into string
        let message = String::from_utf8_lossy(raw_message);
        // Print message
        println!("Message received: {}", message);

        Ok(())
    }
}
