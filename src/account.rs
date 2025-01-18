use std::{
    collections::HashMap,
    fmt::Display,
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Context;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::transaction::Transaction;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    #[default]
    Active,
    Locked,
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
pub struct Account {
    client_id: ClientId,
    /// Full copy of this account's transaction history,
    /// for auditing/redundancy purposes.
    #[builder(skip)]
    pub transactions: Vec<Transaction>,
    #[builder(skip)]
    pub available: Decimal,
    #[builder(skip)]
    pub held: Decimal,
    #[builder(skip)]
    pub total: Decimal,
    #[builder(skip)]
    pub status: AccountStatus,
}

impl Account {
    pub fn is_locked(&self) -> bool {
        self.status == AccountStatus::Locked
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, SerializeDisplay, DeserializeFromStr)]
pub struct ClientId(u128);

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ClientId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse().context("invalid client id")?))
    }
}

/// Simulated database of accounts.
#[derive(Default, Clone)]
pub struct AccountDatabase {
    /// Account data, keyed by client id.
    /// Wrapped in RwLock because account operations are far more common than
    /// account creation/deletion.
    /// Each account is wrapped in Arc<Mutex<>> to allow operations on different
    /// accounts concurrently.
    data: Arc<RwLock<HashMap<ClientId, Arc<Mutex<Account>>>>>,
}

impl AccountDatabase {
    pub fn account(&self, client_id: ClientId) -> Arc<Mutex<Account>> {
        if let Some(account) = self.data.read().expect("lock poisoned").get(&client_id) {
            return account.clone();
        }

        // Account does not exist, add it.
        let mut data = self.data.write().expect("lock poisoned");
        // NOTE: There is a potential race, so don't just insert blindly.
        data.entry(client_id)
            .or_insert_with(|| {
                Arc::new(Mutex::new(Account::builder().client_id(client_id).build()))
            })
            .clone()
    }

    pub fn output_data(&self) {
        for account_mutex in self.data.read().expect("lock poisoned").values() {
            let account = account_mutex.lock().expect("lock poisoned");
            let client = account.client_id;
            let available = account.available;
            let held = account.held;
            let total = account.total;
            let locked = account.is_locked();

            println!("{client}, {available}, {held}, {total}, {locked}");
        }
    }
}
