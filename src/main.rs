use clap::Parser;
use eyre::Result;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "csv-txn-simulator")]
struct Args {
    #[arg(value_name = "INPUT FILE")]
    input_file: PathBuf,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum InputType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize, Clone)]
struct Input {
    r#type: InputType,
    client: u16,
    tx: u32,
    // Decimal is prefered for financial data because:
    // 1. It avoids floating point errors
    // 2. maintains the exact decimal representation.
    // Alternative would be to use integers and track the decimal place/precision separately.
    amount: Option<Decimal>,
}

#[derive(Debug, Serialize, Default, Clone)]
struct Output {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

fn process_transactions(transactions: impl Iterator<Item = Input>) -> HashMap<u16, Output> {
    let mut accounts: HashMap<u16, Output> = HashMap::new();
    let mut txn_history: HashMap<u32, (u16, Decimal, bool)> = HashMap::new();

    for txn in transactions {
        let account = accounts.entry(txn.client).or_insert(Output {
            client: txn.client,
            ..Default::default()
        });

        if account.locked {
            continue;
        }

        match txn.r#type {
            InputType::Deposit | InputType::Withdrawal => {
                if let Some(amount) = txn.amount {
                    let is_deposit = matches!(txn.r#type, InputType::Deposit);
                    if is_deposit || account.available >= amount {
                        let (add, sub) = if is_deposit { (amount, Decimal::ZERO) } else { (Decimal::ZERO, amount) };
                        account.available = account.available.saturating_add(add).saturating_sub(sub);
                        account.total = account.total.saturating_add(add).saturating_sub(sub);
                        txn_history.insert(txn.tx, (txn.client, amount, false));
                    }
                }
            }
            _ => {
                if let Some((client, amount, disputed)) = txn_history.get_mut(&txn.tx) {
                    if *client == txn.client {
                        match (txn.r#type, *disputed) {
                            (InputType::Dispute, false) => {
                                account.available = account.available.saturating_sub(*amount);
                                account.held = account.held.saturating_add(*amount);
                                *disputed = true;
                            }
                            (InputType::Resolve, true) => {
                                account.available = account.available.saturating_add(*amount);
                                account.held = account.held.saturating_sub(*amount);
                                *disputed = false;
                            }
                            (InputType::Chargeback, true) => {
                                account.held = account.held.saturating_sub(*amount);
                                account.total = account.total.saturating_sub(*amount);
                                account.locked = true;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    accounts
}

fn main() -> Result<()> {
    let args = Args::parse();

    // the csv reader is buffered automatically,
    // with a reasonable buffer size.
    let mut input_csv = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(args.input_file)?;

    let accounts = process_transactions(input_csv.deserialize().filter_map(Result::ok));

    let mut wtr = csv::Writer::from_writer(std::io::stdout());
    for account in accounts.values() {
        wtr.serialize(account)?;
    }
    wtr.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use rust_decimal_macros::dec;

    #[rstest]
    #[case::deposit(vec![(InputType::Deposit, 1, 1, Some(dec!(10)))], 1, dec!(10), dec!(0), dec!(10), false)]
    #[case::withdrawal_success(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Withdrawal, 1, 2, Some(dec!(5)))], 1, dec!(5), dec!(0), dec!(5), false)]
    #[case::withdrawal_insufficient(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Withdrawal, 1, 2, Some(dec!(15)))], 1, dec!(10), dec!(0), dec!(10), false)]
    #[case::dispute(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Dispute, 1, 1, None)], 1, dec!(0), dec!(10), dec!(10), false)]
    #[case::resolve(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Dispute, 1, 1, None), (InputType::Resolve, 1, 1, None)], 1, dec!(10), dec!(0), dec!(10), false)]
    #[case::chargeback(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Dispute, 1, 1, None), (InputType::Chargeback, 1, 1, None)], 1, dec!(0), dec!(0), dec!(0), true)]
    #[case::locked_ignores_txns(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Dispute, 1, 1, None), (InputType::Chargeback, 1, 1, None), (InputType::Deposit, 1, 2, Some(dec!(5)))], 1, dec!(0), dec!(0), dec!(0), true)]
    #[case::dispute_nonexistent(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Dispute, 1, 999, None)], 1, dec!(10), dec!(0), dec!(10), false)]
    #[case::double_dispute(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Dispute, 1, 1, None), (InputType::Dispute, 1, 1, None)], 1, dec!(0), dec!(10), dec!(10), false)]
    #[case::resolve_non_disputed(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Resolve, 1, 1, None)], 1, dec!(10), dec!(0), dec!(10), false)]
    #[case::chargeback_non_disputed(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Chargeback, 1, 1, None)], 1, dec!(10), dec!(0), dec!(10), false)]
    #[case::dispute_withdrawal(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Withdrawal, 1, 2, Some(dec!(5))), (InputType::Dispute, 1, 2, None)], 1, dec!(0), dec!(5), dec!(5), false)]
    #[case::multiple_clients(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Deposit, 2, 2, Some(dec!(20))), (InputType::Withdrawal, 1, 3, Some(dec!(5)))], 1, dec!(5), dec!(0), dec!(5), false)]
    #[case::saturation(vec![(InputType::Deposit, 1, 1, Some(Decimal::MAX)), (InputType::Deposit, 1, 2, Some(dec!(1)))], 1, Decimal::MAX, dec!(0), Decimal::MAX, false)]
    #[case::cross_client_dispute(vec![(InputType::Deposit, 1, 1, Some(dec!(10))), (InputType::Dispute, 2, 1, None)], 1, dec!(10), dec!(0), dec!(10), false)]
    #[case::precision_4_decimals(vec![(InputType::Deposit, 1, 1, Some(dec!(1.2345))), (InputType::Withdrawal, 1, 2, Some(dec!(0.1234)))], 1, dec!(1.1111), dec!(0), dec!(1.1111), false)]
    #[case::chronological_order(vec![(InputType::Deposit, 1, 2, Some(dec!(10))), (InputType::Deposit, 1, 1, Some(dec!(5)))], 1, dec!(15), dec!(0), dec!(15), false)]
    fn test_transactions(
        #[case] txns: Vec<(InputType, u16, u32, Option<Decimal>)>,
        #[case] client: u16,
        #[case] expected_available: Decimal,
        #[case] expected_held: Decimal,
        #[case] expected_total: Decimal,
        #[case] expected_locked: bool,
    ) {
        let inputs: Vec<_> = txns.into_iter().map(|(r#type, client, tx, amount)| Input { r#type, client, tx, amount }).collect();
        let accounts = process_transactions(inputs.into_iter());
        let acc = &accounts[&client];
        assert_eq!(acc.available, expected_available);
        assert_eq!(acc.held, expected_held);
        assert_eq!(acc.total, expected_total);
        assert_eq!(acc.locked, expected_locked);
    }
    
    use quickcheck::{Arbitrary, Gen};
    
    impl Arbitrary for InputType {
        fn arbitrary(g: &mut Gen) -> Self {
            match u32::arbitrary(g) % 5 {
                0 => InputType::Deposit,
                1 => InputType::Withdrawal,
                2 => InputType::Dispute,
                3 => InputType::Resolve,
                _ => InputType::Chargeback,
            }
        }
    }
    
    impl Arbitrary for Input {
        fn arbitrary(g: &mut Gen) -> Self {
            let r#type = InputType::arbitrary(g);
            Input {
                r#type,
                client: u16::arbitrary(g) % 100 + 1,
                tx: u32::arbitrary(g) % 10000 + 1,
                amount: matches!(r#type, InputType::Deposit | InputType::Withdrawal)
                    .then(|| Decimal::from_f64_retain(f64::arbitrary(g).abs() % 10000.0 + 0.01).unwrap_or(Decimal::ONE)),
            }
        }
    }
    
    #[quickcheck_macros::quickcheck]
    fn prop_total_equals_available_plus_held(txns: Vec<Input>) -> bool {
        let accounts = process_transactions(txns.into_iter());
        accounts.values().all(|acc| acc.total == acc.available.saturating_add(acc.held))
    }
    
    #[quickcheck_macros::quickcheck]
    fn prop_no_negative_balances(txns: Vec<Input>) -> bool {
        let accounts = process_transactions(txns.into_iter());
        accounts.values().all(|acc| {
            acc.available >= Decimal::ZERO && 
            acc.held >= Decimal::ZERO && 
            acc.total >= Decimal::ZERO
        })
    }
    
    #[test]
    fn test_spec_example() {
        let csv = "type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0";
        
        let mut rdr = csv::ReaderBuilder::new().trim(csv::Trim::All).from_reader(csv.as_bytes());
        let accounts = process_transactions(rdr.deserialize::<Input>().filter_map(Result::ok));
        
        assert_eq!((accounts[&1].available, accounts[&1].total), (dec!(1.5), dec!(1.5)));
        assert_eq!((accounts[&2].available, accounts[&2].total), (dec!(2.0), dec!(2.0)));
    }
    
    #[test]
    fn test_performance() {
        use std::time::Instant;
        let start = Instant::now();
        let accounts = process_transactions((0..1_000_000).map(|i| Input {
            r#type: if i % 2 == 0 { InputType::Deposit } else { InputType::Withdrawal },
            client: (i % 10000) as u16,
            tx: i as u32,
            amount: Some(Decimal::from(i % 100 + 1)),
        }));
        assert!(start.elapsed().as_secs() < 2);
        assert_eq!(accounts.len(), 10000);
    }
}
