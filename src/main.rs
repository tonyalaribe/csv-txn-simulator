use clap::Parser;
use csv;
use eyre::Result;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "csv-txn-simulator")]
struct Args {
    #[arg(value_name = "INPUT FILE")]
    input_file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum InputType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize)]
struct Input {
    r#type: InputType,
    client: u16,
    tx: u32,
    // Decimal is prefered for financial data because:
    // 1. It avoids floating point errors
    // 2. maintains the exact decimal representation.
    // Alternative would be to use integers and track the decimal place/precision separately.
    amount: Decimal,
}

#[derive(Debug, Deserialize)]
struct Output {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: Decimal,
}

fn process_txn(txn: &Input) {
    println!("{:?}", txn)
}

fn main() -> Result<()> {
    let args = Args::parse();

    // the csv reader is buffered automatically,
    // with a reasonable buffer size.
    let mut input_csv = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(args.input_file)?;
    for row in input_csv.deserialize::<Input>() {
        let record = row?;
        process_txn(&record);
    }

    Ok(())
}
