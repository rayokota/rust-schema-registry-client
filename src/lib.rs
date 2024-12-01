//!A fully asynchronous Rust client library for interacting with the
//![Confluent Schema Registry](https://github.com/confluentinc/schema-registry).
//!
//!## The library
//!
//!`rust-schema-registry-client` provides a Schema Registry client, along with serdes (serializers/deserializers) for
//!Avro, Protobuf, and JSON Schema.
//!
//!
//!### Features
//!
//!- Support for Avro, Protobuf, and JSON Schema formats
//!- Specify data quality rules using Google Common Expression Language (CEL) expressions
//!- Specify schema migration rules using JSONata expressions
//!- Enforce client-side field-level encryption (CSFLE) rules using AWS KMS, Azure Key Vault, Google Cloud KMS, or HashiCorp Vault
//!
//!This library can be used with [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) but does not depend on it.
//!
//!### Serdes
//!
//!- [`AvroSerializer`] and [`AvroDeserializer`] - serdes that use `apache-avro`
//!- [`ProtobufSerializer`] and [`ProtobufDeserializer`] - serdes that use `prost` and `prost-reflect`. In particular, the Protobuf objects must implement the `ReflectMessage` trait.
//!- [`JsonSchemaSerializer`] and [`JsonSchemaDeserializer`] - serdes that use `jsonschema`
//!
//!## Installation
//!
//!Add this to your `Cargo.toml`:
//!
//!```toml
//![dependencies]
//!schema-registry-client = { version = "0.1" }
//!```
//!
//!The following features are available:
//!
//!- `rules-cel` - enables data quality rules using CEL
//!- `rules-encryption-awskms` - enables CSFLE rules using AWS KMS
//!- `rules-encryption-azurekms` - enables CSFLE rules using Azure Key Vault
//!- `rules-encryption-gcpkms` - enables CSFLE rules using Google Cloud KMS
//!- `rules-encryption-hcvault` - enables CSFLE rules using HashiCorp Vault
//!- `rules-encryption-localkms` - enables CSFLE rules using a local KMS (for testing)
//!- `rules-jsonata` - enables schema migration rules using JSONata
//!
//!For example, to use CSFLE with the AWS KMS, add this to your `Cargo.toml`:
//!
//!```toml
//!
//![dependencies]
//!schema-registry-client = { version = "0.1", features = ["rules", "rules-encryption-awskms"] }
//!```
//!
//!## Examples
//!
//!You can find examples in the [`examples`] folder. To run them:
//!
//!```bash
//!cargo run --example <example_name> -- <example_args>
//!```

use prost_reflect::DescriptorPool;
use std::sync::LazyLock;

pub mod rest;
pub mod rules;
pub mod serdes;

static FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("codegen/file_descriptor_set.bin");

static DESCRIPTOR_POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    // Get a copy of the global descriptor pool with the Google well-known types.
    let mut pool = DescriptorPool::global();
    pool.decode_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .expect("Failed to load file descriptor set");
    pool
});

static TEST_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("codegen/test/file_descriptor_set.bin");

static TEST_DESCRIPTOR_POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    // Get a copy of the global descriptor pool with the Google well-known types.
    let mut pool = DescriptorPool::global();
    pool.decode_file_descriptor_set(TEST_FILE_DESCRIPTOR_SET)
        .expect("Failed to load file descriptor set");
    pool
});
