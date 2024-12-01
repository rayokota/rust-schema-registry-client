use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;
use std::time::Duration;

use crate::example_utils::setup_logger;
use crate::test::author::PiiOneof;
use crate::test::Author;
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
use schema_registry_client::rules::encryption::awskms::aws_driver::AwsKmsDriver;
use schema_registry_client::rules::encryption::azurekms::azure_driver::AzureKmsDriver;
use schema_registry_client::rules::encryption::encrypt_executor::FieldEncryptionExecutor;
use schema_registry_client::rules::encryption::gcpkms::gcp_driver::GcpKmsDriver;
use schema_registry_client::rules::encryption::hcvault::hcvault_driver::HcVaultDriver;
use schema_registry_client::rules::encryption::localkms::local_driver::LocalKmsDriver;
use schema_registry_client::serdes::config::{SchemaSelector, SerializerConfig};
use schema_registry_client::serdes::protobuf::{
    default_reference_subject_name_strategy, ProtobufSerializer,
};
use schema_registry_client::serdes::serde::{
    topic_name_strategy, SerdeFormat, SerdeType, SerializationContext,
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

async fn produce(
    brokers: &str,
    topic_name: &str,
    url: &str,
    kek_name: &str,
    kms_type: &str,
    kms_key_id: &str,
) {
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

    let rule = Rule {
        name: "encryptPII".to_string(),
        doc: None,
        kind: Some(Kind::Transform),
        mode: Some(Mode::WriteRead),
        r#type: "ENCRYPT".to_string(),
        tags: Some(vec!["PII".to_string()]),
        params: Some(BTreeMap::from([
            ("encrypt.kek.name".to_string(), kek_name.to_string()),
            ("encrypt.kms.type".to_string(), kms_type.to_string()),
            ("encrypt.kms.key.id".to_string(), kms_key_id.to_string()),
        ])),
        expr: None,
        on_success: None,
        on_failure: Some("ERROR,NONE".to_string()),
        disabled: None,
    };
    let rule_set = RuleSet {
        migration_rules: None,
        domain_rules: Some(vec![rule]),
    };
    let schema = Schema {
        schema_type: Some("PROTOBUF".to_string()),
        references: None,
        metadata: None,
        rule_set: Some(Box::new(rule_set)),
        schema: schema_str.to_string(),
    };
    client
        .register_schema(format!("{}-value", topic_name).as_str(), &schema, true)
        .await
        .expect("Failed to register schema");

    // KMS properties can be passed as follows
    //let rule_conf = HashMap::from([
    //    ("secret.access.key".to_string(), "xxx".to_string()),
    //    ("access.key.id".to_string(), "xxx".to_string()),
    //]);
    let rule_conf = HashMap::new();
    let ser_conf = SerializerConfig::new(
        false,
        Some(SchemaSelector::LatestVersion),
        true,
        false,
        rule_conf,
    );
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
        .arg(
            Arg::with_name("kek-name")
                .long("kek-name")
                .help("KEK name")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("kms-type")
                .long("kms-type")
                .help("KMS type")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("kms-key-id")
                .long("kms-key-id")
                .help("KMS key ID")
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
    let kek_name = matches.value_of("kek-name").unwrap();
    let kms_type = matches.value_of("kms-type").unwrap();
    let kms_key_id = matches.value_of("kms-key-id").unwrap();

    AwsKmsDriver::register();
    AzureKmsDriver::register();
    GcpKmsDriver::register();
    HcVaultDriver::register();
    LocalKmsDriver::register();
    FieldEncryptionExecutor::<DekRegistryClient>::register();

    produce(brokers, topic, url, kek_name, kms_type, kms_key_id).await;
}
