use apache_avro::types::Value;
use clap::{App, Arg};
use log::info;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::example_utils::setup_logger;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use schema_registry_client::rest::client_config::ClientConfig as SchemaRegistryClientConfig;
use schema_registry_client::rest::dek_registry_client::DekRegistryClient;
use schema_registry_client::rest::models::{Kind, Mode, Rule, RuleSet, Schema};
use schema_registry_client::rest::schema_registry_client::{Client, SchemaRegistryClient};
use schema_registry_client::serdes::avro::AvroSerializer;
use schema_registry_client::serdes::config::SerializerConfig;
use schema_registry_client::serdes::serde::{
    SerdeFormat, SerdeHeaders, SerdeType, SerializationContext, header_schema_id_serializer,
};

mod example_utils;

async fn produce(brokers: &str, topic_name: &str, url: &str) {
    let client_conf = SchemaRegistryClientConfig::new(vec![url.to_string()]);
    let client = SchemaRegistryClient::new(client_conf);

    let schema_str = r#"
     {
        "namespace": "confluent.io.examples.serialization.avro",
        "name": "User",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string", "confluent:tags": [ "PII" ]},
            {"name": "favorite_number", "type": "long"},
            {"name": "favorite_color", "type": "string"}
        ]
    }
    "#;

    let schema = Schema {
        schema_type: Some("AVRO".to_string()),
        references: None,
        metadata: None,
        rule_set: None,
        schema: schema_str.to_string(),
    };

    let mut ser_conf = SerializerConfig::new(true, None, true, false, HashMap::new());
    ser_conf.schema_id_serializer = header_schema_id_serializer;
    let ser = AvroSerializer::new(&client, Some(&schema), None, ser_conf)
        .expect("Failed to create serializer");
    let ser_ctx = SerializationContext {
        topic: topic_name.to_string(),
        serde_type: SerdeType::Value,
        serde_format: SerdeFormat::Avro,
        headers: Some(SerdeHeaders::default()),
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
                let fields = vec![
                    ("name".to_string(), Value::String(format!("Name {}", i))),
                    ("favorite_number".to_string(), Value::Int(i)),
                    (
                        "favorite_color".to_string(),
                        Value::String("blue".to_string()),
                    ),
                ];
                let value = Value::Record(fields.clone());
                let bytes: Vec<u8> = ser
                    .serialize(&ser_ctx, value)
                    .await
                    .expect("Failed to serialize");

                let mut headers = OwnedHeaders::new();
                for header in ser_ctx.headers.unwrap().iter() {
                    headers = headers.insert(Header {
                        key: &header.key,
                        value: header.value.as_deref(),
                    });
                }

                let mut record: FutureRecord<Vec<u8>, Vec<u8>> = FutureRecord::to(topic_name);
                record = record.payload(&bytes).headers(headers);
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
    let matches = App::new("JSON producer encryption example")
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
