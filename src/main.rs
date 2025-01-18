use std::{fs::File, path::Path};

use account::AccountDatabase;
use anyhow::Context;
use processor::Processor;
use transaction::Transaction;

mod account;
mod processor;
mod transaction;

fn main() -> anyhow::Result<()> {
    let filename = std::env::args()
        .nth(1)
        .context("Please provide the CSV filename")?;

    let database = AccountDatabase::default();
    let processor = Processor::new(database.clone());
    let result = process_csv(Path::new(&filename), &processor);
    processor.close();
    result?;

    database.output_data();
    Ok(())
}

fn process_csv(filename: &Path, processor: &Processor) -> anyhow::Result<()> {
    let f = File::open(filename)
        .with_context(|| format!("failed to open file: {}", filename.display()))?;
    let mut reader = csv::Reader::from_reader(f);
    for result in reader.deserialize() {
        let record: Transaction = result.context("failed to parse record from CSV")?;
        processor.send_transaction(record)?;
    }
    Ok(())
}
