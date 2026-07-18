use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub name: String,
    pub payload: serde_json::Value,
}

pub struct ScratchPad {
    memory: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl Default for ScratchPad {
    fn default() -> Self {
        Self::new()
    }
}

impl ScratchPad {
    pub fn new() -> Self {
        Self {
            memory: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.memory.read().await.get(key).cloned()
    }

    pub async fn set(&self, key: String, value: serde_json::Value) {
        self.memory.write().await.insert(key, value);
    }
}

pub struct EventBus {
    pub sender: mpsc::Sender<Event>,
    pub receiver: mpsc::Receiver<Event>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self { sender, receiver }
    }
}

pub struct Kernel {
    pub event_bus: EventBus,
    pub scratch_pad: ScratchPad,
}

impl Default for Kernel {
    fn default() -> Self {
        Self::new()
    }
}

impl Kernel {
    pub fn new() -> Self {
        Self {
            event_bus: EventBus::new(100),
            scratch_pad: ScratchPad::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        // Main event loop scaffold
        while let Some(_event) = self.event_bus.receiver.recv().await {
            // Process event, trigger topologies, update ScratchPad, etc.
        }
        Ok(())
    }
}
