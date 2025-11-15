#![allow(dead_code)]

use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// In-memory RabbitMQ broker mock used by integration tests to assert publish/consume flows.
#[derive(Clone, Default, Debug)]
pub struct MockRabbitmqBroker {
    inner: Arc<Mutex<RabbitmqState>>,
}

#[derive(Default, Debug)]
struct RabbitmqState {
    next_delivery_tag: u64,
    queue: VecDeque<MockRabbitmqDelivery>,
    confirmations: HashMap<u64, bool>,
}

#[derive(Clone, Debug)]
pub struct MockRabbitmqDelivery {
    delivery_tag: u64,
    exchange: String,
    routing_key: String,
    payload: JsonValue,
    headers: JsonMap<String, JsonValue>,
}

impl MockRabbitmqBroker {
    pub fn publish(
        &self,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
        payload: JsonValue,
        headers: JsonMap<String, JsonValue>,
    ) -> MockRabbitmqConfirmation {
        let mut inner = self.inner.lock().expect("rabbitmq broker state");
        let tag = inner.next_delivery_tag;
        inner.next_delivery_tag += 1;

        let delivery = MockRabbitmqDelivery {
            delivery_tag: tag,
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            payload,
            headers,
        };

        inner.confirmations.insert(tag, false);
        inner.queue.push_back(delivery.clone());

        MockRabbitmqConfirmation {
            broker: self.clone(),
            delivery_tag: tag,
        }
    }

    pub fn next_delivery(&self) -> Option<MockRabbitmqDelivery> {
        self.inner
            .lock()
            .expect("rabbitmq broker state")
            .queue
            .pop_front()
    }

    pub fn ack(&self, delivery_tag: u64) {
        if let Some(entry) = self
            .inner
            .lock()
            .expect("rabbitmq broker state")
            .confirmations
            .get_mut(&delivery_tag)
        {
            *entry = true;
        }
    }

    pub fn is_acknowledged(&self, delivery_tag: u64) -> bool {
        self.inner
            .lock()
            .expect("rabbitmq broker state")
            .confirmations
            .get(&delivery_tag)
            .copied()
            .unwrap_or(false)
    }
}

impl MockRabbitmqDelivery {
    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }

    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }

    pub fn payload(&self) -> &JsonValue {
        &self.payload
    }

    pub fn headers(&self) -> &JsonMap<String, JsonValue> {
        &self.headers
    }

    pub fn ack(self, broker: &MockRabbitmqBroker) {
        broker.ack(self.delivery_tag);
    }
}

#[derive(Clone, Debug)]
pub struct MockRabbitmqConfirmation {
    broker: MockRabbitmqBroker,
    delivery_tag: u64,
}

impl MockRabbitmqConfirmation {
    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }

    pub fn ack(&self) {
        self.broker.ack(self.delivery_tag);
    }

    pub fn is_acknowledged(&self) -> bool {
        self.broker.is_acknowledged(self.delivery_tag)
    }
}

#[derive(Clone, Default)]
pub struct MockRabbitmqClient {
    broker: MockRabbitmqBroker,
}

impl MockRabbitmqClient {
    pub fn new(broker: MockRabbitmqBroker) -> Self {
        Self { broker }
    }

    pub fn publish(
        &self,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
        payload: JsonValue,
        headers: JsonMap<String, JsonValue>,
    ) -> MockRabbitmqConfirmation {
        self.broker.publish(exchange, routing_key, payload, headers)
    }

    pub fn next_delivery(&self) -> Option<MockRabbitmqDelivery> {
        self.broker.next_delivery()
    }
}

/// In-memory MQTT broker mock that models QoS handshake metadata.
#[derive(Clone, Default, Debug)]
pub struct MockMqttBroker {
    inner: Arc<Mutex<MqttState>>,
}

#[derive(Default, Debug)]
struct MqttState {
    next_packet_id: u16,
    queue: VecDeque<MockMqttMessage>,
}

impl MockMqttBroker {
    pub fn push(
        &self,
        topic: impl Into<String>,
        payload: JsonValue,
        qos: u8,
        retain: bool,
    ) -> MockMqttMessage {
        let mut inner = self.inner.lock().expect("mqtt broker state");
        let packet_id = if qos > 0 {
            inner.next_packet_id = inner.next_packet_id.wrapping_add(1).max(1);
            inner.next_packet_id
        } else {
            0
        };

        let message = MockMqttMessage {
            packet_id,
            topic: topic.into(),
            payload,
            qos,
            retain,
        };

        inner.queue.push_back(message.clone());
        message
    }

    pub fn next(&self) -> Option<MockMqttMessage> {
        self.inner
            .lock()
            .expect("mqtt broker state")
            .queue
            .pop_front()
    }
}

#[derive(Clone, Debug)]
pub struct MockMqttMessage {
    packet_id: u16,
    topic: String,
    payload: JsonValue,
    qos: u8,
    retain: bool,
}

impl MockMqttMessage {
    pub fn packet_id(&self) -> u16 {
        self.packet_id
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn payload(&self) -> &JsonValue {
        &self.payload
    }

    pub fn qos(&self) -> u8 {
        self.qos
    }

    pub fn retain(&self) -> bool {
        self.retain
    }
}

#[derive(Clone, Default)]
pub struct MockMqttClient {
    broker: MockMqttBroker,
}

impl MockMqttClient {
    pub fn new(broker: MockMqttBroker) -> Self {
        Self { broker }
    }

    pub fn publish(
        &self,
        topic: impl Into<String>,
        payload: JsonValue,
        qos: u8,
        retain: bool,
    ) -> MockMqttPublishResult {
        let message = self.broker.push(topic, payload, qos, retain);

        MockMqttPublishResult {
            packet_id: message.packet_id(),
            qos,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MockMqttPublishResult {
    packet_id: u16,
    qos: u8,
}

impl MockMqttPublishResult {
    pub fn packet_id(&self) -> u16 {
        self.packet_id
    }

    pub fn qos(&self) -> u8 {
        self.qos
    }
}
