<!-- ABOUT THE PROJECT -->
## About The Project

Sample message producer written using the `iggy-go` sdk.

<!-- GETTING STARTED -->
## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

In order to use this SDK you need to install golang on your enviroment. Here's a <a hfref="https://go.dev/doc/install">link to official go documentation</a> explaining how you can do that!

## Usage

In order to successfully launch the producer app follow these steps:
1. Clone iggy repo and run it in background
    ```sh
    git clone https://github.com/iggy-rs/iggy.git
    cd iggy
    cargo run--bin server -r
    ```
1. Open new terminal instance and enter `iggy-go` root folder
    ```sh
    cd iggy-go
    ```
2. Run the producer
    ```sh
    go run ./samples/producer
    ```

    Producer should start sending new messages, output should look similiar to this:

    ```sh
    $ go run ./samples/producer

    Stream with ID: 1 exists.
    Topic with ID: 1 exists.
    Messages will be sent to stream '1', topic '1', partition '1' with interval 1000 ms.
    Sent messages: {"message_type":"order_rejected","payload":"{\"id\":1,\"timestamp\":37314,\"reason\":\"Other\"}"}
    Sent messages: {"message_type":"order_confirmed","payload":"{\"id\":1,\"price\":215,\"timestamp\":28024}"}
    ```
