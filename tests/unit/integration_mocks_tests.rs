use serde_json::{json, Map as JsonMap};

#[path = "../support/mod.rs"]
mod support;

use support::mocks::{
    MockMqttBroker, MockMqttClient, MockRabbitmqBroker, MockRabbitmqClient,
};

#[test]
fn rabbitmq_mock_tracks_acknowledgements() {
    let broker = MockRabbitmqBroker::default();
    let client = MockRabbitmqClient::new(broker.clone());

    let confirmation = client.publish(
        "events",
        "records.processed",
        json!({"id": "123"}),
        JsonMap::new(),
    );

    assert!(!confirmation.is_acknowledged());
    let delivery = client.next_delivery().expect("delivery present");
    delivery.clone().ack(&broker);
    confirmation.ack();

    assert!(broker.is_acknowledged(delivery.delivery_tag()));
}

#[test]
fn mqtt_mock_assigns_packet_ids_for_qos() {
    let broker = MockMqttBroker::default();
    let client = MockMqttClient::new(broker.clone());

    let qos0 = client.publish("updates", json!({"v": 1}), 0, false);
    assert_eq!(qos0.packet_id(), 0);

    let qos1 = client.publish("updates", json!({"v": 2}), 1, true);
    assert!(qos1.packet_id() > 0);
    assert_eq!(qos1.qos(), 1);

    let message = broker.next().expect("message present");
    assert_eq!(message.topic(), "updates");
    assert_eq!(message.qos(), 0);
    let message = broker.next().expect("second message");
    assert_eq!(message.qos(), 1);
    assert!(message.retain());
}
