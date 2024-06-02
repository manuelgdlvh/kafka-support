use std::time::Duration;
use rdkafka::{ClientConfig, Message};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use crate::producer::kafka_producer_config::KafkaProducerConfig;

const EVENT_TYPE: &'static str = "type";

pub struct KafkaProducer {
    topic_name: String,
    producer: FutureProducer,
}

impl KafkaProducer {
    fn build_producer(config: &KafkaProducerConfig) -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", config.bootstrap_servers())
            .set("message.timeout.ms", config.message_timeout_ms().to_string())
            .create()
            .expect("Producer creation error")
    }


    pub async fn produce<T1>(&self, type_: &str, key: &str, payload: &T1)
        where T1: Serialize {
        let payload_as_str = match serde_json::to_string(payload) {
            Ok(v) => {
                v
            }
            Err(e) => {
                println!("error serializing payload caused by: {}", e.to_string());
                return;
            }
        };


        let result = self.producer
            .send(
                FutureRecord::to(&self.topic_name)
                    .payload(&payload_as_str)
                    .key(key)
                    .headers(OwnedHeaders::new().insert(Header {
                        key: EVENT_TYPE,
                        value: Some(type_),
                    })),
                Duration::from_secs(0),
            )
            .await;

        if result.is_err() {
            println!("error producing message caused by: {:?}", result);
        }
    }

    pub fn build(config: KafkaProducerConfig) -> Self {
        Self {
            topic_name: config.topic_name().to_string(),
            producer: KafkaProducer::build_producer(&config),
        }
    }
}

