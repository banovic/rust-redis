use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::Bound::Included,
};

use clap::builder::Str;

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
    pub data: HashMap<String, BTreeSet<(SafeFloat, String)>>,
}

impl SortedSets {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: &String, score: f64, member: &String) -> usize {
        let mut prev_score_key: Option<SafeFloat> = None;

        if !self.data.contains_key(key) {
            self.data.insert(key.clone(), BTreeSet::new());
        }

        let set = self.data.get(key).unwrap();

        for (sf, m) in set {
            // Update key for existing member
            if m == member {
                prev_score_key = Some(*sf);
                break;
            }
        }

        let set = self.data.get_mut(key).unwrap();

        if let Some(prev_score_key) = prev_score_key {
            set.remove(&(prev_score_key, member.clone()));
        }

        set.insert((SafeFloat(score), member.clone()));

        match prev_score_key {
            Some(_) => 0, // update, no inserts
            None => 1,    // pure insert
        }
    }

    pub fn rank(&self, key: &String, member: &String) -> Option<u64> {
        let mut r = 0_u64;
        if let Some(set) = self.data.get(key) {
            for (k, m) in set.iter() {
                if m == member {
                    return Some(r);
                }
                r += 1;
            }
        }
        None
    }

    pub fn range(&self, key: &String, start: i32, stop: i32) -> Vec<String> {
        let mut ms = Vec::new();

        let len = self.data.get(key).map(|s| s.len() as i32).unwrap_or(0);

        let a = if start < 0 { start + len } else { start };
        let a = 0.max(a);

        let b = if stop < 0 { stop + len } else { stop };
        let b = (len - 1).min(b);

        if a > b {
            return ms;
        }

        if let Some(set) = self.data.get(key) {
            for (i, (_, m)) in set.iter().enumerate() {
                if a <= (i as i32) && (i as i32) <= b {
                    ms.push(m.clone());
                }
            }
        }
        ms
    }

    pub fn card(&self, key: &String) -> usize {
        self.data.get(key).map_or(0, |set| set.len())
    }

    pub fn score(&self, key: &String, member: &String) -> Option<f64> {
        if let Some(set) = self.data.get(key) {
            for (k, m) in set.iter() {
                if m == member {
                    return Some(k.0);
                }
            }
        }
        None
    }
}
