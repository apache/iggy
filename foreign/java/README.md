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

## Examples

See the [`examples`](examples/) module for basic consumer and producer implementations using the SDK.

For Apache Flink integration, see the [Flink Connector Library](external-processors/iggy-connector-flink/iggy-connector-library/README.md).

## Contributing

Before opening a pull request:

1. **Format code:** `gradle spotlessApply`
2. **Validate build:** `gradle check`

This ensures code style compliance and that all tests and checkstyle validations pass.
