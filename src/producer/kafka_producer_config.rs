pub struct KafkaProducerConfig {
    bootstrap_servers: String,
    topic_name: String,
    message_timeout_ms: u32,
}

impl KafkaProducerConfig {
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }
    pub fn message_timeout_ms(&self) -> u32 {
        self.message_timeout_ms
    }
}


impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            topic_name: "topic_example".to_string(),
            message_timeout_ms: 5000,
        }
    }
}