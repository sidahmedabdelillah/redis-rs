use std::time::SystemTime;
use std::{collections::HashMap, sync::Mutex};

pub struct Store {
    db: Mutex<HashMap<String, Entery>>,
}

#[derive(Debug, Clone)]
pub struct Entery {
    pub value: String,
    pub set_time: SystemTime,
    pub expire_time: u64,
}

impl Entery {
    pub fn new(value: String, expire_time: u64) -> Self {
        Entery {
            value,
            set_time: SystemTime::now(),
            expire_time,
        }
    }
}

impl Store {
    pub fn new() -> Self {
        Store {
            db: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&self, key: String, value: String) {
        let mut db = self.db.lock().unwrap();
        let entry = Entery::new(value, 0);
        db.insert(key, entry);
    }

    pub fn set_with_expiry(&self, key: String, value: String, expiry: u64) {
        let mut db = self.db.lock().unwrap();
        let entry = Entery::new(value, expiry);
        db.insert(key, entry);
    }

    pub fn get(&self, key: String) -> Option<Entery> {
        let db = self.db.lock().unwrap();
        if let Some(entery) = db.get(&key) {
            if entery.expire_time == 0 {
                return Some(entery.clone());
            }
            let now = SystemTime::now();
            let diff = now.duration_since(entery.set_time).unwrap().as_millis();
            println!("debug: diff {} expire_time {}", diff, entery.expire_time);
            if diff < entery.expire_time as u128 {
                return Some(entery.clone());
            } else {
                return None;
            }
        } else {
            return None;
        }
    }
}
