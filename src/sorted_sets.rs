use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
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
    pub data: HashMap<String, BTreeMap<SafeFloat, BTreeSet<String>>>,
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
            self.data.insert(key.clone(), BTreeMap::new());
        }

        let map = self.data.get_mut(key).unwrap();

        for (sk, set) in map {
            // Update key for existing member
            if set.contains(member) {
                prev_score_key = Some(*sk);
                break;
            }
        }

        let mut map = self.data.get_mut(key).unwrap();

        if let Some(prev_score_key) = prev_score_key {
            let mut set = map.get_mut(&prev_score_key).unwrap();
            set.remove(member);
            if set.is_empty() {
                map.remove(&prev_score_key);
            }
        }

        let nk = SafeFloat(score);
        map.entry(nk)
            .and_modify(|set| {
                (*set).insert(member.clone());
            })
            .or_insert(BTreeSet::from([member.clone()]));

        match prev_score_key {
            Some(_) => 0, // update, no inserts
            None => 1,    // pure insert
        }
    }

    pub fn rank(&self, key: &String, member: &String) -> Option<u64> {
        let mut r = 0_u64;
        if let Some(map) = self.data.get(key) {
            for (k, set) in map.iter() {
                if set.contains(member) {
                    for m in set {
                        if m == member {
                            return Some(r);
                        }
                        r += 1;
                    }
                }
                r += 1;
            }
        }
        None
    }
}
