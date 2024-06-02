use std::sync::Arc;
use rdkafka::{ClientConfig, Message};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers};
use serde::Deserialize;
use crate::consumer::kafka_consumer_config::KafkaConsumerConfig;


const EVENT_TYPE: &'static str = "type";

pub struct KafkaConsumer {}

impl KafkaConsumer {
    fn build_consumer(config: KafkaConsumerConfig) -> StreamConsumer {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", config.group_id())
            .set("bootstrap.servers", config.bootstrap_servers())
            .set("enable.partition.eof", config.enable_partition_eof().to_string())
            .set("session.timeout.ms", config.session_timeout_ms().to_string())
            .set("enable.auto.commit", config.enable_auto_commit().to_string())
            .set_log_level(config.log_level())
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&vec![config.topic_name()])
            .expect("Can't subscribe to specified topics");

        consumer
    }

    fn find_header_by_key(message: &BorrowedMessage, key: &str) -> Option<String> {
        if let Some(headers) = message.headers() {
            for header in headers.iter() {
                if !header.key.eq(key) || !header.value.is_some() {
                    continue;
                }
                if let Ok(value) = String::from_utf8(header.value.unwrap().to_vec()) {
                    return Some(value);
                }
            }
        }
        None
    }


    /*
        pub async fn single_callback<T1>(config: KafkaConsumerConfig, callback_0: (String, fn(T1)))
        where T1: for<'a> Deserialize<'a> + 'static {
        let consumer: StreamConsumer = KafkaConsumer::build_consumer(config);

        tokio::spawn(async move {
            loop {
                match consumer.recv().await {
                    Err(e) => println!("Kafka error: {}", e),
                    Ok(m) => {
                        let payload = match m.payload_view::<str>() {
                            None => continue,
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                println!("Error while deserializing message payload: {:?}", e);
                                continue;
                            }
                        };


                        if let Some(type_) = Self::find_header_by_key(&m, EVENT_TYPE) {
                            if !Self::process_message(&type_, payload, &callback_0) {
                                println!("no handler found for -> key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                                         m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                            }
                        } else {
                            println!("no event type header found for -> key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                                     m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                        }

                        let _ = consumer.commit_message(&m, CommitMode::Sync);
                    }
                };
            }
        });
    }
     */


    fn process_message<S, T>(container: Arc<S>, type_: &str, payload: &str, callback: &(&'static str, fn(Arc<S>, T))) -> bool
        where T: for<'a> Deserialize<'a> {
        if !&type_.eq(callback.0) {
            return false;
        }

        let value: Result<T, serde_json::Error> = serde_json::from_str::<T>(payload);
        if value.is_err() {
            println!("error deserializing message caused ");
            return false;
        }

        callback.1(container, value.unwrap());
        true
    }
    pub async fn with_two_callbacks<S, C1, C2>(container: Arc<S>, config: KafkaConsumerConfig, callback_0: (&'static str, fn(Arc<S>,C1)), callback_1: (&'static str, fn(Arc<S>, C2)))
        where S: Send + Sync + 'static,
              C1: for<'a> Deserialize<'a> + 'static,
              C2: for<'a> Deserialize<'a> + 'static {
        let consumer: StreamConsumer = KafkaConsumer::build_consumer(config);

        tokio::spawn(async move {
            loop {
                match consumer.recv().await {
                    Err(e) => println!("Kafka error: {}", e),
                    Ok(m) => {
                        let payload = match m.payload_view::<str>() {
                            None => continue,
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                println!("Error while deserializing message payload: {:?}", e);
                                continue;
                            }
                        };

                        if let Some(type_) = Self::find_header_by_key(&m, EVENT_TYPE) {
                            if !(Self::process_message(container.clone(), &type_, payload, &callback_0)
                                || Self::process_message(container.clone(), &type_, payload, &callback_1)) {
                                println!("no handler found for -> key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                                         m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                            }
                        } else {
                            println!("no event type header found for -> key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                                     m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                        }

                        let _ = consumer.commit_message(&m, CommitMode::Sync);
                    }
                };
            }
        });
    }

    /*
        pub async fn with_three_callbacks<T1, T2, T3>(config: KafkaConsumerConfig, callback_0: (String, fn(T1)), callback_1: (String, fn(T2)), callback_2: (String, fn(T3)))
        where T1: for<'a> Deserialize<'a> + 'static,
              T2: for<'a> Deserialize<'a> + 'static,
              T3: for<'a> Deserialize<'a> + 'static {
        let consumer: StreamConsumer = KafkaConsumer::build_consumer(config);

        tokio::spawn(async move {
            loop {
                match consumer.recv().await {
                    Err(e) => println!("Kafka error: {}", e),
                    Ok(m) => {
                        let payload = match m.payload_view::<str>() {
                            None => continue,
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                println!("Error while deserializing message payload: {:?}", e);
                                continue;
                            }
                        };


                        if let Some(type_) = Self::find_header_by_key(&m, EVENT_TYPE) {
                            if !(Self::process_message(&type_, payload, &callback_0)
                                || Self::process_message(&type_, payload, &callback_1)
                                || Self::process_message(&type_, payload, &callback_2)) {
                                println!("no handler found for -> key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                                         m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                            }
                        } else {
                            println!("no event type header found for -> key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                                     m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                        }

                        let _ = consumer.commit_message(&m, CommitMode::Sync);
                    }
                };
            }
        });
    }
     */
}

