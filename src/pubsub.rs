use std::collections::{HashMap, HashSet};

use crate::ClientId;

#[derive(Debug)]
pub struct PubSub {
    pub subscriptions: HashMap<String, HashSet<ClientId>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    pub fn add_subscription(&mut self, client_id: ClientId, channel: &String) {
        self.subscriptions
            .entry(channel.clone())
            .and_modify(|subs| {
                (*subs).insert(client_id);
            })
            .or_insert(HashSet::from([client_id]));
    }

    pub fn get_client_subscriptions(&self, client_id: ClientId) -> u64 {
        let mut c = 0;
        for (_, clients) in &self.subscriptions {
            if clients.contains(&client_id) {
                c += 1;
            }
        }
        c
    }

    pub fn subscribers_count(&self, channel: &str) -> usize {
        self.subscriptions
            .get(&channel.to_string())
            .map_or(0, |cs| cs.len())
    }

    pub fn unsubscribe(&mut self, client_id: ClientId, channel: &str) {
        self.subscriptions
            .entry(channel.to_string())
            .and_modify(|subs| {
                (*subs).remove(&client_id);
            });
    }
}
