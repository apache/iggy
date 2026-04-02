# Simulator UI

The `simulator_ui` is a graphical interface for the Viewstamped Replication (VSR) Simulator. It is built using the Bevy game engine and provides tools to visualize and interact with distributed systems simulations. The `simulator_ui` integrates with the `simulator` core to display events, replica states, and network conditions in real-time.

The `simulator_ui` is part of the VSR (Viewstamped Replication) Simulator, built using the Bevy game engine. It provides a graphical interface for simulating and visualizing scenarios related to distributed systems and replication protocols within the [simulator](../simulator) core.

## Installation Instructions

### Running on macOS

The application runs out of the box on Apple M-series chips without additional dependencies.

### Running on Linux

### Step 1: Install ALSA Dependencies

Before building the project, ensure that ALSA development libraries are installed. Use the following commands based on your Linux distribution:

- **Ubuntu/Debian**:
  ```sh
  sudo apt-get update
  sudo apt-get install libasound2-dev pkg-config
  ```

- **Fedora**:
  ```sh
  sudo dnf install alsa-lib-devel pkgconf-pkg-config
  ```

- **Arch Linux**:
  ```sh
  sudo pacman -S alsa-lib pkgconf
  ```

- **OpenSUSE**:
  ```sh
  sudo zypper install alsa-devel pkg-config
  ```

If you encounter issues, ensure the `PKG_CONFIG_PATH` environment variable is set correctly. For example:
```sh
export PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig
```

### Vulkan Warnings (Optional)

You may encounter Vulkan-related warnings such as:
- `Unable to find extension: VK_EXT_physical_device_drm`
- `InstanceFlags::VALIDATION requested, but unable to find layer: VK_LAYER_KHRONOS_validation`

These warnings are non-critical and do not affect the application's functionality. However, for debugging Vulkan, you can install the Vulkan SDK:
```sh
# Example for Linux
# Download from https://vulkan.lunarg.com/sdk/home and follow the instructions
export VULKAN_SDK=/path/to/vulkan-sdk
export PATH=$VULKAN_SDK/bin:$PATH
export LD_LIBRARY_PATH=$VULKAN_SDK/lib:$LD_LIBRARY_PATH
export VK_ICD_FILENAMES=$VULKAN_SDK/etc/vulkan/icd.d
export VK_LAYER_PATH=$VULKAN_SDK/etc/vulkan/explicit_layer.d
```

To install and run the `simulator_ui` on a Linux system, follow the steps below:

### Running the Application

After building, you can run the application from the root repository with:
```sh
cargo r --bin simulator_ui
```

### Observability and Events

The `simulator_ui` leverages the `observability` module from the `simulator` core to track and display key simulation events. These events include:

- **ClientRequestReceived**: Logs when a client request is received by a replica.
- **OperationCommitted**: Tracks when an operation is successfully committed.
- **ViewChangeStarted**: Indicates the start of a view change process.
- **PrimaryElected**: Logs the election of a new primary replica.
- **NamespaceProgressUpdated**: Monitors progress within a namespace.

These events are emitted by the `simulator` core and visualized in the UI, providing insights into the behavior of the replication protocol.
