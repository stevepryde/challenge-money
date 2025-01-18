use std::{
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::{self, JoinHandle},
};

use anyhow::Context;

use crate::{
    account::{Account, AccountDatabase},
    transaction::{Transaction, TransactionType},
};

#[derive(Debug)]
pub enum Message {
    End,
    Transaction(Transaction),
}

pub struct Processor {
    tx: SyncSender<Message>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Processor {
    pub fn new(database: AccountDatabase) -> Self {
        let (tx, rx) = sync_channel(100);

        let handle = thread::spawn(move || {
            process_transactions(database, rx)?;
            Ok(())
        });

        Self {
            tx,
            handle: Some(handle),
        }
    }

    pub fn send_transaction(&self, transaction: Transaction) -> anyhow::Result<()> {
        self.tx
            .send(Message::Transaction(transaction))
            .context("failed to send transaction")
    }

    pub fn close(mut self) {
        if let Some(handle) = self.handle.take() {
            if self.tx.send(Message::End).is_err() {
                tracing::error!("failed to send End message to processor");
            }

            if let Err(e) = handle.join() {
                tracing::error!("failed to join processor thread: {e:#?}");
            }
        }
    }
}

/// Process transactions in a loop.
pub fn process_transactions(
    database: AccountDatabase,
    rx: Receiver<Message>,
) -> anyhow::Result<()> {
    loop {
        let message = rx.recv().context("failed to receive message")?;
        tracing::debug!("Received message: {message:#?}");

        match message {
            Message::End => {
                tracing::debug!("sentinel received. shutting down...");
                return Ok(());
            }
            Message::Transaction(t) => {
                let account_mutex = database.account(t.client_id);
                let mut account = account_mutex.lock().expect("lock poisoned");
                apply_transaction(t, &mut account);
            }
        }
    }
}

pub fn apply_transaction(transaction: Transaction, account: &mut Account) {
    match transaction.transaction_type {
        TransactionType::Deposit => apply_deposit(transaction, account),
        TransactionType::Withdrawal => apply_withdrawal(transaction, account),
        TransactionType::Dispute => apply_dispute(transaction, account),
        TransactionType::Resolve => apply_resolve(transaction, account),
        TransactionType::Chargeback => apply_chargeback(transaction, account),
    }
}

pub fn apply_deposit(transaction: Transaction, account: &mut Account) {}

pub fn apply_withdrawal(transaction: Transaction, account: &mut Account) {}

pub fn apply_dispute(transaction: Transaction, account: &mut Account) {}

pub fn apply_resolve(transaction: Transaction, account: &mut Account) {}

pub fn apply_chargeback(transaction: Transaction, account: &mut Account) {}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: write tests.
}
