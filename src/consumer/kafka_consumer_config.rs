use rdkafka::config::RDKafkaLogLevel;

pub struct KafkaConsumerConfig {
    bootstrap_servers: String,
    group_id: String,
    topic_name: String,
    enable_partition_eof: bool,
    session_timeout_ms: u32,
    enable_auto_commit: bool,
    log_level: RDKafkaLogLevel,

}

impl KafkaConsumerConfig {
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }
    pub fn enable_partition_eof(&self) -> bool {
        self.enable_partition_eof
    }
    pub fn session_timeout_ms(&self) -> u32 {
        self.session_timeout_ms
    }
    pub fn enable_auto_commit(&self) -> bool {
        self.enable_auto_commit
    }
    pub fn log_level(&self) -> RDKafkaLogLevel {
        self.log_level
    }
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "group_id".to_string(),
            topic_name: "topic_example".to_string(),
            enable_partition_eof: false,
            session_timeout_ms: 6000,
            enable_auto_commit: false,
            log_level: RDKafkaLogLevel::Debug,
        }
    }
}