use std::{fs::File, path::Path};

use account::AccountDatabase;
use anyhow::Context;
use csv::ReaderBuilder;
use processor::Processor;
use transaction::Transaction;

mod account;
mod currency;
mod processor;
mod transaction;

fn main() -> anyhow::Result<()> {
    // NOTE: enable for logging.
    // tracing_subscriber::fmt::init();

    let filename = std::env::args()
        .nth(1)
        .context("Please provide the CSV filename")?;
    let path = Path::new(&filename);
    let f = File::open(path).with_context(|| format!("failed to open file: {}", path.display()))?;

    let database = AccountDatabase::default();
    let processor = Processor::new(database.clone());
    let result = process_csv(&processor, f);
    processor.close();
    result?;

    database.output_data(std::io::stdout())?;
    Ok(())
}

fn process_csv<R: std::io::Read>(processor: &Processor, input: R) -> anyhow::Result<()> {
    let mut reader = ReaderBuilder::new()
        .trim(csv::Trim::All) // Trims leading and trailing whitespace
        .from_reader(input);
    for result in reader.deserialize() {
        let record: Transaction = result.context("failed to parse record from CSV")?;
        processor.send_transaction(record)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, io::Cursor};

    use super::*;

    fn lines_sorted(input: &str) -> HashSet<String> {
        input.lines().map(|x| x.to_string()).collect()
    }

    #[test]
    fn test_example_data() {
        let input = r#"type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0"#;

        let database = AccountDatabase::default();
        let processor = Processor::new(database.clone());
        process_csv(&processor, Cursor::new(input)).unwrap();
        processor.close();

        let mut output = Cursor::new(Vec::new());
        database.output_data(&mut output).unwrap();
        database.verify_all_accounts();

        let expected_output = r#"client,available,held,total
1,1.5,0,1.5,false
2,2.0,0,2.0,false"#;

        assert_eq!(
            lines_sorted(&String::from_utf8(output.into_inner()).unwrap()),
            lines_sorted(expected_output)
        );
    }
}
