# Wireshark Dissector for Iggy Protocol

Wireshark dissector for analyzing Iggy protocol traffic, written in Lua.

## Requirements

- Wireshark 4.6.0 or higher
- Iggy server

## Running Tests

1. Start the server:
   ```bash
   cargo run --bin iggy-server -- --with-default-root-credentials
   ```

2. Run tests:
   ```bash
   cargo test -p wireshark --features protocol-tests
   ```

## Wireshark Plugin Setup

1. Copy the dissector to Wireshark plugins directory (macOS):
   ```bash
   cp ./wireshark/dissector.lua ~/.local/lib/wireshark/plugins/
   ```

2. Reload plugins in Wireshark: `Analyze → Reload Lua Plugins`

3. Run demo code or test code to generate traffic

### Configuration

The default port used for protocol analysis is 8090. If using a different port, you need to change the target port.

Change the target port in preferences (macOS): `Wireshark → Preferences → Protocols → IGGY → Server Port`

### Troubleshooting

If you encounter a duplicate protocol error, remove and re-copy:
```bash
rm ~/.local/lib/wireshark/plugins/dissector.lua
```

## Known Limitations & TODO

- QUIC protocol support not yet implemented
- Server must be running locally for analysis
- Only a subset of Iggy commands currently supported
- Test code needs refactoring for better readability
- Test server should be isolated with a dedicated test script


