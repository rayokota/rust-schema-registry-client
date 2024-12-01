use crate::example_utils::setup_logger;
use crate::test::Author;
use clap::{App, Arg};
use log::{info, warn};
use prost_reflect::DescriptorPool;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;
use schema_registry_client::rest::client_config::ClientConfig as SchemaRegistryClientConfig;
use schema_registry_client::rest::dek_registry_client::DekRegistryClient;
use schema_registry_client::rest::schema_registry_client::{Client, SchemaRegistryClient};
use schema_registry_client::rules::encryption::awskms::aws_driver::AwsKmsDriver;
use schema_registry_client::rules::encryption::azurekms::azure_driver::AzureKmsDriver;
use schema_registry_client::rules::encryption::encrypt_executor::FieldEncryptionExecutor;
use schema_registry_client::rules::encryption::gcpkms::gcp_driver::GcpKmsDriver;
use schema_registry_client::rules::encryption::hcvault::hcvault_driver::HcVaultDriver;
use schema_registry_client::rules::encryption::localkms::local_driver::LocalKmsDriver;
use schema_registry_client::serdes::config::DeserializerConfig;
use schema_registry_client::serdes::protobuf::ProtobufDeserializer;
use schema_registry_client::serdes::serde::{
    topic_name_strategy, SerdeFormat, SerdeType, SerializationContext,
};
use std::collections::HashMap;
use std::sync::LazyLock;

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

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str], url: &str) {
    let client_conf = SchemaRegistryClientConfig::new(vec![url.to_string()]);
    let client = SchemaRegistryClient::new(client_conf);

    // KMS properties can be passed as follows
    //let rule_conf = HashMap::from([
    //    ("secret.access.key".to_string(), "xxx".to_string()),
    //    ("access.key.id".to_string(), "xxx".to_string()),
    //]);
    let rule_conf = HashMap::new();
    let deser_conf = DeserializerConfig::new(None, false, rule_conf);
    let deser = ProtobufDeserializer::new(&client, topic_name_strategy, None, deser_conf)
        .expect("Failed to create deserializer");

    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let ser_ctx = SerializationContext {
                    topic: m.topic().to_string(),
                    serde_type: SerdeType::Value,
                    serde_format: SerdeFormat::Protobuf,
                    headers: None,
                };
                let payload: Author = deser
                    .deserialize(&ser_ctx, &m.payload().unwrap_or(b""))
                    .await
                    .unwrap();
                info!(
                    "payload: '{:?}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                    payload,
                    m.topic(),
                    m.partition(),
                    m.offset(),
                    m.timestamp()
                );
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("Protobuf consumer encryption example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("topics")
                .short("t")
                .long("topics")
                .help("Topic list")
                .takes_value(true)
                .multiple(true)
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

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let url = matches.value_of("schema-registry-url").unwrap();

    AwsKmsDriver::register();
    AzureKmsDriver::register();
    GcpKmsDriver::register();
    HcVaultDriver::register();
    LocalKmsDriver::register();
    FieldEncryptionExecutor::<DekRegistryClient>::register();

    consume_and_print(brokers, group_id, &topics, url).await
}
