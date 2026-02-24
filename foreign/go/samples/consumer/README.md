# Consumer Sample

## About The Project

Sample message consumer written using the `iggy-go` sdk.

<!-- GETTING STARTED -->
## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

In order to use this SDK you need to install golang on your environment. Here's a <a hfref="https://go.dev/doc/install">link to official go documentation</a> explaining how you can do that!

## Usage

In order to successfully launch the consumer app follow these steps:

1. Clone iggy repo and run it in background

    ```sh
    git clone https://github.com/apache/iggy.git
    cd iggy
    cargo run --bin iggy-server -r
    ```

2. Open new terminal instance and enter `iggy-go` root folder

    ```sh
    cd iggy-go
    ```

3. Run the consumer

    ```sh
    go run ./samples/consumer
    ```

    If you have not consumed messages you should see output similar to this:

    ```sh
    $ go run ./samples/consumer

    Stream with ID: 1 exists.
    Topic with ID: 1 exists.
    Messages will be polled from stream '1', topic '1', partition '1' with interval 1000 ms.
    Handling message type: order_rejected at offset: 0 with message Id: 55ca2b5f-7a7d-48f3-95ab-7d41c702fef2 {Id:1 Timestamp:37314 Reason:Other}
    Handling message type: order_confirmed at offset: 1 with message Id: 2b5906c7-9a41-4f86-86de-8cceca023dc1 {Id:1 Price:215 Timestamp:28024}
    Received 0 messages.
    ```
