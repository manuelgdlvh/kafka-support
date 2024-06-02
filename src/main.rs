use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use rdkafka::message::ToBytes;
use serde::{Deserialize, Serialize};
use kafka_support::consumer::kafka_consumer::KafkaConsumer;
use kafka_support::consumer::kafka_consumer_config::KafkaConsumerConfig;
use kafka_support::producer::kafka_producer::KafkaProducer;
use kafka_support::producer::kafka_producer_config::KafkaProducerConfig;


#[derive(Debug, Serialize, Deserialize)]
pub struct Example {
    pub field: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Example2 {
    pub field2: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SharedStruct {
    pub count: Mutex<u32>,
}


#[tokio::main]
async fn main() {
    let shared = Arc::new(SharedStruct {
        count: Mutex::new(0),
    });

    // Pass DI container
    KafkaConsumer::with_two_callbacks::<SharedStruct, Example, Example2>(shared, KafkaConsumerConfig::default(),
                                                                         ("example", |shared, value| {
                                                                             let mut asd = shared.count.lock().unwrap();
                                                                             *asd += 1;
                                                                             println!("processing example message -> {}", *asd);
                                                                         }),
                                                                         ("example2", |shared, value| {
                                                                             let mut asd = shared.count.lock().unwrap();
                                                                             *asd += 1;
                                                                             println!("processing example message 2 -> {}", *asd);
                                                                         })).await;

    let producer = KafkaProducer::build(KafkaProducerConfig::default());

    loop {
        producer.produce("example2", "key", &Example2 { field2: "teest".to_string() }).await;
        producer.produce("example", "key", &Example { field: "teest".to_string() }).await;
    }
}
