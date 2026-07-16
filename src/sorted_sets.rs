use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

#[derive(Debug, Clone, Copy)]
pub struct SafeFloat(f64);

// Implement PartialEq using total_cmp
impl PartialEq for SafeFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}

// Implement Eq since total_cmp guarantees a total equivalence relation
impl Eq for SafeFloat {}

// Implement PartialOrd
impl PartialOrd for SafeFloat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Implement Ord using total_cmp
impl Ord for SafeFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug)]
pub struct SortedSets {
    pub data: HashMap<String, BTreeMap<SafeFloat, String>>,
}

impl SortedSets {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: &String, score: f64, member: &String) {
        self.data
            .entry(key.clone())
            .and_modify(|s| {
                (*s).insert(SafeFloat(score), member.clone());
            })
            .or_insert(BTreeMap::from([(SafeFloat(score), member.clone())]));
    }
}
