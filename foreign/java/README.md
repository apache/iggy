# Java SDK for Apache Iggy

Official Java client SDK for [Apache Iggy](https://iggy.apache.org) message streaming.

> This is part of the Apache Iggy monorepo. For the main project, see the [root repository](https://github.com/apache/iggy).

## Installation

Add the dependency to your project:

**Gradle:**

```gradle
implementation 'org.apache.iggy:iggy:0.6.0'
```

**Maven:**

```xml
<dependency>
    <groupId>org.apache.iggy</groupId>
    <artifactId>iggy</artifactId>
    <version>0.6.0</version>
</dependency>
```

Find the latest version on [Maven Repository](https://mvnrepository.com/artifact/org.apache.iggy/iggy).

### Snapshot Versions

Snapshot versions are also available through the ASF snapshot repository:

**Gradle:**

```gradle
repositories {
    maven {
        url = uri("https://repository.apache.org/content/repositories/snapshots/")
    }
}

dependencies {
    implementation 'org.apache.iggy:iggy:0.6.1-SNAPSHOT'
}
```

**Maven:**

```xml
<repositories>
    <repository>
        <id>apache-snapshots</id>
        <url>https://repository.apache.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>

<dependency>
    <groupId>org.apache.iggy</groupId>
    <artifactId>iggy</artifactId>
    <version>0.6.1-SNAPSHOT</version>
</dependency>
```

## Quick Start

### TCP Client (Blocking)

```java
import org.apache.iggy.Iggy;

// Create and connect with auto-login
var client = Iggy.tcpClientBuilder()
    .blocking()
    .host("localhost")
    .port(8090)
    .credentials("iggy", "iggy")
    .buildAndLogin();

// Or build, connect, and login separately
var client = Iggy.tcpClientBuilder()
    .blocking()
    .host("localhost")
    .port(8090)
    .build();
client.connect();
client.users().login("iggy", "iggy");
```

### TCP Client (Async)

```java
import org.apache.iggy.Iggy;

// Create async client
var asyncClient = Iggy.tcpClientBuilder()
    .async()
    .host("localhost")
    .port(8090)
    .credentials("iggy", "iggy")
    .buildAndLogin()
    .join();

// Or with manual connect and login
var asyncClient = Iggy.tcpClientBuilder()
    .async()
    .host("localhost")
    .build();
asyncClient.connect().join();
asyncClient.users().login("iggy", "iggy").join();
```

### HTTP Client

```java
import org.apache.iggy.Iggy;

// Using URL
var httpClient = Iggy.httpClientBuilder()
    .blocking()
    .url("http://localhost:3000")
    .credentials("iggy", "iggy")
    .buildAndLogin();

// Using host/port
var httpClient = Iggy.httpClientBuilder()
    .blocking()
    .host("localhost")
    .port(3000)
    .credentials("iggy", "iggy")
    .buildAndLogin();
```

### TLS Support

Both TCP and HTTP clients support TLS:

```java
// TCP with TLS
var secureClient = Iggy.tcpClientBuilder()
    .blocking()
    .host("iggy-server.example.com")
    .port(8090)
    .enableTls()
    .tlsCertificate("/path/to/ca.pem")  // Optional custom CA
    .credentials("admin", "secret")
    .buildAndLogin();

// HTTPS
var secureHttpClient = Iggy.httpClientBuilder()
    .blocking()
    .host("iggy-server.example.com")
    .port(443)
    .enableTls()
    .credentials("admin", "secret")
    .buildAndLogin();
```

### Builder Options

The client builders support additional configuration:

```java
var client = Iggy.tcpClientBuilder()
    .blocking()
    .host("localhost")
    .port(8090)
    .connectionTimeout(Duration.ofSeconds(10))
    .requestTimeout(Duration.ofSeconds(30))
    .connectionPoolSize(10)
    .retryPolicy(RetryPolicy.exponentialBackoff())
    .credentials("iggy", "iggy")
    .buildAndLogin();
```

### Version Information

```java
// Get SDK version
String version = Iggy.version();  // e.g., "0.6.1-SNAPSHOT"

// Get detailed version info
IggyVersion info = Iggy.versionInfo();
info.getVersion();     // Version string
info.getBuildTime();   // Build timestamp
info.getGitCommit();   // Git commit hash
info.getUserAgent();   // User-Agent string for HTTP
```

## Exception Handling

All exceptions thrown by the SDK inherit from `IggyException`. This allows you to catch all SDK-related errors with a single catch block, or handle specific exception types for more granular error handling.

## Examples

See the [`examples`](examples/) module for basic consumer and producer implementations using the SDK.

For Apache Flink integration, see the [Flink Connector Library](external-processors/iggy-connector-flink/iggy-connector-library/README.md).

## Contributing

Before opening a pull request:

1. **Format code:** `gradle spotlessApply`
2. **Validate build:** `gradle check`

This ensures code style compliance and that all tests and checkstyle validations pass.
