# Python Examples for Iggy SDK

This directory contains comprehensive Python sample applications that showcase various usage patterns of the Iggy Python SDK, mirroring the Rust examples. These examples demonstrate basic usage, message patterns, multi-tenant scenarios, and advanced stream management.

## Prerequisites

- Python 3.8+
- Install dependencies:

```bash
pip install -r requirements.txt
```

- Ensure the Iggy server is running before executing the examples.

## Running Examples

Navigate to this directory and run the desired example:

### Basic Usage

- Producer:
  ```bash
  python basic/producer.py
  ```
- Consumer:
  ```bash
  python basic/consumer.py
  ```

### Getting Started

- Producer:
  ```bash
  python getting_started/producer.py
  ```
- Consumer:
  ```bash
  python getting_started/consumer.py
  ```

## Testing All Examples

To test all examples automatically, run:

```bash
./test_all_examples.sh
```

## Notes

- Each example is self-contained and includes comments explaining the concepts and usage patterns.
- For more details, see the [Iggy Python SDK documentation](../../foreign/python/README.md).
