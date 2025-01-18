use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};

use crate::{account::ClientId, currency::Currency};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct TransactionId(u32);

impl From<u32> for TransactionId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for TransactionId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse().context("invalid transaction id")?))
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    SerializeDisplay,
    DeserializeFromStr,
    strum::Display,
    strum::EnumString,
)]
#[strum(serialize_all = "lowercase")]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bon::Builder)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Transaction {
    #[serde(rename = "type")]
    pub transaction_type: TransactionType,
    #[serde(rename = "client")]
    pub client_id: ClientId,
    #[serde(rename = "tx")]
    pub transaction_id: TransactionId,
    #[builder(default)]
    #[serde(default)]
    pub amount: Currency,
}
