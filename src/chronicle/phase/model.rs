use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseDefinition {
    pub name: String,
    pub steps: Vec<PhaseStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseStep {
    pub name: String,
    pub action: PhaseAction,
    pub compensation: Option<PhaseAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PhaseAction {
    Rest {
        method: String,
        url: String,
        #[serde(default)]
        body_template: Option<Value>,
    },
    Kafka {
        topic: String,
        key_template: Option<Value>,
    },
    Database {
        query: String,
        #[serde(default)]
        parameters: Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseInstance {
    pub id: Uuid,
    pub status: PhaseStatus,
}

#[derive(Debug)]
pub enum PhaseMessage {
    Heartbeat { source: String },
    Trigger { phase_id: Uuid, payload: Value },
}
