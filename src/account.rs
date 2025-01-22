use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    io::Write,
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::{
    currency::Currency,
    transaction::{Transaction, TransactionId},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, SerializeDisplay, DeserializeFromStr)]
pub struct ClientId(u16);

impl From<u16> for ClientId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    #[default]
    Active,
    Locked,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bon::Builder)]
pub struct Account {
    client_id: ClientId,
    /// Full copy of this account's transaction history,
    /// for auditing/redundancy purposes.
    #[builder(skip)]
    pub history: Vec<Transaction>,
    /// Transaction cache for lookups.
    #[builder(skip)]
    pub transactions: HashMap<TransactionId, Transaction>,
    #[builder(skip)]
    pub disputes: HashSet<TransactionId>,
    #[builder(skip)]
    pub available: Currency,
    #[builder(skip)]
    pub held: Currency,
    #[builder(skip)]
    pub total: Currency,
    #[builder(skip)]
    status: AccountStatus,
}

impl Account {
    pub fn is_locked(&self) -> bool {
        self.status == AccountStatus::Locked
    }

    pub fn freeze(&mut self) {
        self.status = AccountStatus::Locked
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

    pub fn output_data<W: Write>(&self, mut writer: W) -> anyhow::Result<()> {
        writeln!(writer, "client,available,held,total,locked")?;
        for account_mutex in self.data.read().expect("lock poisoned").values() {
            let account = account_mutex.lock().expect("lock poisoned");
            let client = account.client_id;
            let available = account.available;
            let held = account.held;
            let total = account.total;
            let locked = account.is_locked();

            writeln!(writer, "{client},{available},{held},{total},{locked}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test_support {
    use proptest::prelude::*;

    use super::*;

    use crate::{processor::apply_transaction, transaction::TransactionType};

    impl Account {
        pub fn sanity_check(&self) {
            // Verify amounts.
            assert_eq!(self.available, self.total - self.held);
            // Account should only be locked if a chargeback occurred, and
            // if so, the chargeback should be the last transaction.
            assert_eq!(
                self.history
                    .iter()
                    .last()
                    .map(|x| x.transaction_type == TransactionType::Chargeback)
                    .unwrap_or_default(),
                self.is_locked()
            );

            let mut new_account = Account::builder().client_id(self.client_id).build();

            for transaction in &self.history {
                apply_transaction(transaction.clone(), &mut new_account).ok();
            }

            assert_eq!(self, &new_account);
        }
    }

    impl AccountDatabase {
        pub fn verify_all_accounts(&self) {
            let data = self.data.read().unwrap();
            for account_mutex in data.values() {
                let account = account_mutex.lock().unwrap();
                account.sanity_check();
            }
        }
    }

    impl Arbitrary for ClientId {
        type Parameters = ();

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (0..11u16).prop_map(|x| Self(x)).boxed()
        }

        type Strategy = BoxedStrategy<Self>;
    }
}
