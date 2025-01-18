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
                if let Err(e) = apply_transaction(t, &mut account) {
                    // Failed transactions could be sent to a queue for further processing.
                    tracing::error!("transaction failed: {e:#}");
                }
            }
        }
    }
}

fn ensure_transaction_does_not_exist(
    transaction: &Transaction,
    account: &Account,
) -> anyhow::Result<()> {
    match account
        .transactions
        .contains_key(&transaction.transaction_id)
    {
        true => Err(anyhow::anyhow!("transaction id already exists")),
        false => Ok(()),
    }
}

pub fn apply_transaction(transaction: Transaction, account: &mut Account) -> anyhow::Result<()> {
    if account.is_locked() {
        return Err(anyhow::anyhow!("account is locked"));
    }

    if transaction.amount.is_negative() {
        return Err(anyhow::anyhow!("transaction amount must not be negative"));
    }

    match transaction.transaction_type {
        TransactionType::Deposit => apply_deposit(&transaction, account)?,
        TransactionType::Withdrawal => apply_withdrawal(&transaction, account)?,
        TransactionType::Dispute => apply_dispute(&transaction, account)?,
        TransactionType::Resolve => apply_resolve(&transaction, account)?,
        TransactionType::Chargeback => apply_chargeback(&transaction, account)?,
    }

    account.history.push(transaction);

    Ok(())
}

fn apply_deposit(transaction: &Transaction, account: &mut Account) -> anyhow::Result<()> {
    ensure_transaction_does_not_exist(&transaction, &account)?;

    account.available += transaction.amount;
    account.total += transaction.amount;

    account
        .transactions
        .insert(transaction.transaction_id, transaction.clone());

    Ok(())
}

fn apply_withdrawal(transaction: &Transaction, account: &mut Account) -> anyhow::Result<()> {
    ensure_transaction_does_not_exist(&transaction, &account)?;

    if account.available < transaction.amount {
        return Err(anyhow::anyhow!("insufficient funds"));
    }

    account.total -= transaction.amount;
    account.available -= transaction.amount;

    account
        .transactions
        .insert(transaction.transaction_id, transaction.clone());
    Ok(())
}

fn apply_dispute(transaction: &Transaction, account: &mut Account) -> anyhow::Result<()> {
    let disputed_transaction = account
        .transactions
        .get(&transaction.transaction_id)
        .context("disputed transaction not found")?;

    if account.disputes.contains(&transaction.transaction_id) {
        return Err(anyhow::anyhow!("transaction already disputed"));
    }

    account.disputes.insert(transaction.transaction_id);
    account.held += disputed_transaction.amount;
    account.available -= disputed_transaction.amount;
    Ok(())
}

fn apply_resolve(transaction: &Transaction, account: &mut Account) -> anyhow::Result<()> {
    if !account.disputes.contains(&transaction.transaction_id) {
        return Err(anyhow::anyhow!("transaction not in dispute"));
    }

    let disputed_transaction = account
        .transactions
        .get(&transaction.transaction_id)
        .context("disputed transaction not found")?;

    assert!(account.held >= disputed_transaction.amount);
    account.held -= disputed_transaction.amount;
    account.available += disputed_transaction.amount;

    Ok(())
}

fn apply_chargeback(transaction: &Transaction, account: &mut Account) -> anyhow::Result<()> {
    if !account.disputes.contains(&transaction.transaction_id) {
        return Err(anyhow::anyhow!("transaction not in dispute"));
    }

    let disputed_transaction = account
        .transactions
        .get(&transaction.transaction_id)
        .context("disputed transaction not found")?;

    assert!(account.held >= disputed_transaction.amount);
    account.held -= disputed_transaction.amount;
    account.total -= disputed_transaction.amount;
    account.freeze();
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    use crate::{account::ClientId, currency::Currency, transaction::TransactionId};

    fn vec_transactions(count: usize) -> impl Strategy<Value = Vec<Transaction>> {
        prop::collection::vec(any::<Transaction>(), 1..count)
    }

    proptest! {
        #[test]
        fn test_transactions_proptest(transactions in vec_transactions(100)) {
            let database = AccountDatabase::default();
            let processor = Processor::new(database.clone());
            for transaction in transactions {
                processor.send_transaction(transaction).unwrap();
            }
            processor.close();
            database.verify_all_accounts();
        }
    }

    fn init_account(initial_balance: f64) -> Account {
        let mut account = Account::builder().client_id(ClientId::from(1)).build();
        let amount = Currency::from_f64(initial_balance);
        let transaction = Transaction::builder()
            .transaction_type(TransactionType::Deposit)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(1))
            .amount(amount)
            .build();
        apply_transaction(transaction.clone(), &mut account).unwrap();
        account
    }

    #[test]
    fn test_deposit() {
        let mut account = Account::builder().client_id(ClientId::from(1)).build();
        let amount = Currency::from_f64(100.0);

        let mut transaction = Transaction::builder()
            .transaction_type(TransactionType::Deposit)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(1))
            .amount(amount)
            .build();

        apply_transaction(transaction.clone(), &mut account).unwrap();
        assert_eq!(account.available, amount);
        assert_eq!(account.total, account.available);

        // Duplicate transaction should be rejected.
        apply_transaction(transaction.clone(), &mut account)
            .expect_err("duplicate transaction should be rejected");

        // Apply with new transaction id, should succeed.
        transaction.transaction_id = TransactionId::from(2);
        apply_transaction(transaction, &mut account).unwrap();
        assert_eq!(account.available, amount + amount);
        assert_eq!(account.total, account.available);
        account.sanity_check();
    }

    #[test]
    fn test_withdrawal() {
        let mut account = init_account(100.0);

        let mut transaction = Transaction::builder()
            .transaction_type(TransactionType::Withdrawal)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(2))
            .amount(Currency::from_f64(42.0))
            .build();

        apply_transaction(transaction.clone(), &mut account).unwrap();
        assert_eq!(account.available, Currency::from_f64(100.0 - 42.0));
        assert_eq!(account.total, account.available);

        // Duplicate transaction should be rejected.
        apply_transaction(transaction.clone(), &mut account)
            .expect_err("duplicate transaction should be rejected");

        // Apply with new transaction id, should succeed.
        transaction.transaction_id = TransactionId::from(3);
        apply_transaction(transaction, &mut account).unwrap();
        assert_eq!(account.available, Currency::from_f64(100.0 - (42.0 * 2.0)));
        assert_eq!(account.total, account.available);
        account.sanity_check();
    }

    #[test]
    fn test_dispute_resolve() {
        let mut account = init_account(100.0);

        let mut transaction = Transaction::builder()
            .transaction_type(TransactionType::Dispute)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(1))
            .build();

        apply_transaction(transaction.clone(), &mut account).unwrap();
        assert_eq!(account.available, Currency::from_f64(0.));
        assert_eq!(account.held, Currency::from_f64(100.));
        assert_eq!(account.total, Currency::from_f64(100.0));

        // Duplicate dispute should be rejected.
        apply_transaction(transaction.clone(), &mut account)
            .expect_err("duplicate dispute should be rejected");

        // Apply with new transaction id, should fail to find the transaction.
        transaction.transaction_id = TransactionId::from(2);
        apply_transaction(transaction, &mut account)
            .expect_err("disputing a missing transaction fail");

        // Resolve the dispute.
        let transaction = Transaction::builder()
            .transaction_type(TransactionType::Resolve)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(1))
            .build();

        apply_transaction(transaction.clone(), &mut account).unwrap();
        assert_eq!(account.available, Currency::from_f64(100.));
        assert_eq!(account.held, Currency::from_f64(0.));
        assert_eq!(account.total, Currency::from_f64(100.0));
        account.sanity_check();
    }

    #[test]
    fn test_dispute_chargeback() {
        let mut account = init_account(100.0);

        let mut transaction = Transaction::builder()
            .transaction_type(TransactionType::Dispute)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(1))
            .build();

        apply_transaction(transaction.clone(), &mut account).unwrap();
        assert_eq!(account.available, Currency::from_f64(0.));
        assert_eq!(account.held, Currency::from_f64(100.));
        assert_eq!(account.total, Currency::from_f64(100.0));

        // Duplicate dispute should be rejected.
        apply_transaction(transaction.clone(), &mut account)
            .expect_err("duplicate dispute should be rejected");

        // Apply with new transaction id, should fail to find the transaction.
        transaction.transaction_id = TransactionId::from(2);
        apply_transaction(transaction, &mut account)
            .expect_err("disputing a missing transaction fail");

        // Issue chargeback.
        let transaction = Transaction::builder()
            .transaction_type(TransactionType::Chargeback)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(1))
            .build();

        apply_transaction(transaction.clone(), &mut account).unwrap();
        assert_eq!(account.available, Currency::from_f64(0.));
        assert_eq!(account.held, Currency::from_f64(0.));
        assert_eq!(account.total, Currency::from_f64(0.0));
        assert!(account.is_locked());
        account.sanity_check();

        // New transactions should be rejected.
        let transaction = Transaction::builder()
            .transaction_type(TransactionType::Deposit)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(3))
            .amount(Currency::from_f64(1.0))
            .build();

        apply_transaction(transaction.clone(), &mut account)
            .expect_err("transactions should be rejected if account is locked");
    }

    #[test]
    fn test_all() {
        let mut account = init_account(100.0);
        let amount0 = Currency::from_f64(100.0);

        // Deposit1 - to be disputed and resolved
        let amount1 = Currency::from_f64(42.0);
        let mut transaction1 = Transaction::builder()
            .transaction_type(TransactionType::Deposit)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(2))
            .amount(amount1)
            .build();

        apply_transaction(transaction1.clone(), &mut account).unwrap();

        // Deposit2 - to be disputed and chargeback
        let amount2 = Currency::from_f64(3.14);
        let mut transaction2 = Transaction::builder()
            .transaction_type(TransactionType::Deposit)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(3))
            .amount(amount2)
            .build();

        apply_transaction(transaction2.clone(), &mut account).unwrap();

        // Deposit3 - to be disputed
        let amount3 = Currency::from_f64(9.87);
        let mut transaction3 = Transaction::builder()
            .transaction_type(TransactionType::Deposit)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(4))
            .amount(amount3)
            .build();

        apply_transaction(transaction3.clone(), &mut account).unwrap();

        assert_eq!(account.available, amount0 + amount1 + amount2 + amount3);
        assert_eq!(account.held, Currency::from_f64(0.));
        assert_eq!(account.total, account.available);

        // Dispute all 3.
        transaction1.transaction_type = TransactionType::Dispute;
        apply_transaction(transaction1.clone(), &mut account).unwrap();
        transaction2.transaction_type = TransactionType::Dispute;
        apply_transaction(transaction2.clone(), &mut account).unwrap();
        transaction3.transaction_type = TransactionType::Dispute;
        apply_transaction(transaction3.clone(), &mut account).unwrap();

        // Deposit one more.
        let amount4 = Currency::from_f64(12.34);
        let transaction4 = Transaction::builder()
            .transaction_type(TransactionType::Deposit)
            .client_id(ClientId::from(1))
            .transaction_id(TransactionId::from(5))
            .amount(amount4)
            .build();

        apply_transaction(transaction4.clone(), &mut account).unwrap();

        // Resolve 1
        transaction1.transaction_type = TransactionType::Resolve;
        apply_transaction(transaction1.clone(), &mut account).unwrap();
        // Reject 2.
        transaction2.transaction_type = TransactionType::Chargeback;
        apply_transaction(transaction2.clone(), &mut account).unwrap();

        // Verify.
        assert_eq!(account.available, amount0 + amount1 + amount4);
        assert_eq!(account.held, amount3);
        assert_eq!(account.total, amount0 + amount1 + amount3 + amount4);
        assert!(account.is_locked());
        account.sanity_check();
    }
}
