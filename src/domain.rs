#![forbid(unsafe_code)]

use std::collections::BTreeMap;

/// Core representation of data passed between transports and transformers.
/// Canonical metadata keys emitted by transports and consumed by pipelines.
pub const TRACE_ID_KEY: &str = "trace_id";
pub const RECORD_ID_KEY: &str = "record_id";
pub const ROUTE_KEY: &str = "route";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainMessage {
    pub provider: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub metadata: BTreeMap<String, String>,
}

impl DomainMessage {
    pub fn new(provider: impl Into<String>, headers: Vec<(String, String)>, body: Vec<u8>) -> Self {
        Self {
            provider: provider.into(),
            headers,
            body,
            metadata: BTreeMap::new(),
        }
    }

    pub fn metadata_value(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(|value| value.as_str())
    }

    pub fn trace_id(&self) -> Option<&str> {
        self.metadata_value(TRACE_ID_KEY)
    }

    pub fn record_id(&self) -> Option<&str> {
        self.metadata_value(RECORD_ID_KEY)
    }

    pub fn route(&self) -> Option<&str> {
        self.metadata_value(ROUTE_KEY)
    }

    pub fn with_trace_id(mut self, value: impl Into<String>) -> Self {
        self.metadata.insert(TRACE_ID_KEY.to_string(), value.into());
        self
    }

    pub fn with_record_id(mut self, value: impl Into<String>) -> Self {
        self.metadata
            .insert(RECORD_ID_KEY.to_string(), value.into());
        self
    }

    pub fn with_route(mut self, value: impl Into<String>) -> Self {
        self.metadata.insert(ROUTE_KEY.to_string(), value.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}
