use std::{collections::HashMap, sync::Mutex};

pub struct Store {
    db: Mutex<HashMap<String,String>>
}

impl Store {
    pub fn new() -> Self {
        Store {
            db: Mutex::new(HashMap::new())
        }
    }

    pub fn set(&self, key: String, value: String) {
        let mut db = self.db.lock().unwrap();
        db.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        let db = self.db.lock().unwrap();
        db.get(&key).cloned()
    }

    pub fn del(&self, key: String) {
        let mut db = self.db.lock().unwrap();
        db.remove(&key);
    }
}