use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;
use std::time::Duration;

use crate::example_utils::setup_logger;
use crate::test::Author;
use crate::test::author::PiiOneof;
use clap::{App, Arg};
use log::info;
use prost::EncodeError;
use prost_reflect::{DescriptorPool, FileDescriptor, ReflectMessage};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use schema_registry_client::rest::client_config::ClientConfig as SchemaRegistryClientConfig;
use schema_registry_client::rest::dek_registry_client::DekRegistryClient;
use schema_registry_client::rest::models::{Kind, Mode, Rule, RuleSet, Schema};
use schema_registry_client::rest::schema_registry_client::{Client, SchemaRegistryClient};
use schema_registry_client::serdes::config::SerializerConfig;
use schema_registry_client::serdes::protobuf::{
    ProtobufSerializer, default_reference_subject_name_strategy,
};
use schema_registry_client::serdes::serde::{
    SerdeFormat, SerdeType, SerializationContext, topic_name_strategy,
};

mod example_utils;

pub mod test {
    include!("../src/codegen/test/test.rs");
}

static TEST_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../src/codegen/test/file_descriptor_set.bin");

static TEST_DESCRIPTOR_POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    // Get a copy of the global descriptor pool with the Google well-known types.
    let mut pool = DescriptorPool::global();
    pool.decode_file_descriptor_set(TEST_FILE_DESCRIPTOR_SET)
        .expect("Failed to load file descriptor set");
    pool
});

async fn produce(brokers: &str, topic_name: &str, url: &str) {
    let client_conf = SchemaRegistryClientConfig::new(vec![url.to_string()]);
    let client = SchemaRegistryClient::new(client_conf);

    let schema_str = r#"
syntax = "proto3";

package test;

import "confluent/meta.proto";

message Author {
  string name = 1 [
    (confluent.field_meta).tags = "PII"
  ];
  int32 id = 2;
  bytes picture = 3 [
    (confluent.field_meta).tags = "PII"
  ];
  repeated string works = 4;
  oneof pii_oneof {
    Pizza oneof_message = 5;
    string oneof_string = 6 [(.confluent.field_meta).tags = "PII"];
  }
}

message Pizza {
  string size = 1;
  repeated string toppings = 2;
}
    "#;

    let ser_conf = SerializerConfig::new(true, None, true, false, HashMap::new());
    let ser = ProtobufSerializer::new(
        &client,
        topic_name_strategy,
        default_reference_subject_name_strategy,
        None,
        ser_conf,
    )
    .expect("Failed to create serializer");
    let ser_ctx = SerializationContext {
        topic: topic_name.to_string(),
        serde_type: SerdeType::Value,
        serde_format: SerdeFormat::Protobuf,
        headers: None,
    };

    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // This loop is non-blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..5)
        .map(|i| {
            let ser = ser.clone();
            let ser_ctx = ser_ctx.clone();
            async move {
                let obj = Author {
                    name: format!("Name {}", i),
                    id: i,
                    picture: vec![1u8, 2u8, 3u8],
                    works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
                    pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
                };
                let bytes: Vec<u8> = ser
                    .serialize(&ser_ctx, &obj)
                    .await
                    .expect("Failed to serialize");
                let mut record: FutureRecord<Vec<u8>, Vec<u8>> = FutureRecord::to(topic_name);
                record = record.payload(&bytes);
                // The send operation on the topic returns a future, which will be
                // completed once the result or failure from Kafka is received.
                let delivery_status = producer.send(record, Duration::from_secs(0)).await;

                // This will be executed when the result is received.
                info!("Delivery status for message {} received", i);
                delivery_status
            }
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("Protobuf producer encryption example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line producer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("topic")
                .short("t")
                .long("topic")
                .help("Destination topic")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("schema-registry-url")
                .short("u")
                .long("url")
                .help("Schema Registry URL")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    setup_logger(true, matches.value_of("log-conf"));

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let url = matches.value_of("schema-registry-url").unwrap();

    produce(brokers, topic, url).await;
}
