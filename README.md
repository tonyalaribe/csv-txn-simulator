# CSV Transaction simulator

This CSV transaction simulator accepts an input csv file of client transactions and returns as output, a summary of the clients and their account status.

Run it via:

```
cargo run -- input.csv


```

# Implementation strategy

1. stream the csv from disk and process it, so we don't have to fit it all into memory.
   The ReadBuilder from the csv package streams the csv by default with an automatically managed buffer, so I stuck with the default. But its possible to adjust buffer sizes to match our memory constraints.
2. Store the transactions in a hashmap, so that when there's a dispute we only need to lookup the transaction in that `txn_history` hashmap instead of rereading the entire csv. But this adds a slight memory overhead to store this history.
3. Using saturating_sub/add instead of regular arithmetic, in case we deal with large amounts, which could trigger an overflow, we simply force the results into a valid range. in the case of the Decimal package we use as the representation for amounts, money is stored as 3 `u32::MAX` internally, so the limit (`Decimal::MAX`) is much higher than just a u32.
4. I used table driven tests via the `rstest` package. I believe that when testing pure logic, we always benefit from making it easy to add new test cases without a lot of boilerplate. So we test the following cases:

   - deposits
   - successfull withdrawals
   - insufficient funds during withdrawal
   - disputes
   - resolve disputes
   - perform charge backs
   - when an account is locked, the next transactions are ignored
   - disputes on nonexisting accounts are ignored
   - double diputes ignore the second.
   - resolve when account is undisputed is ignored
   - chargeback when account is not disputed is ignored
   - dispute works fine even after withrawal.
   - multiple clients support
   - no overflow when depositing into account with max amount (`Decimal::MAX`)
   - cross client disputes.
   - support for 4 decimals
   - check that irrespective of transaction ids, transactions are handled in order of their presence in the csv

5. I use property based testing both as a means of benchmarking and as a way to assert that certain properties always hold:
   - Irrespective of what transactions are executed, the accounts total will always e the sum of the available and the held amounts.
   - Irrespective of the withdrawals and deposit orders, we will never have negative amounts in available, held or total balances.
6. Benchmarking. Property based testing allows generating arbitrary values for tests based on properties we decide on. Which means we can generate huge amounts of test data without an explicit mocking or faker script. This was then used to benchmark the process_transactions logic.

   You can run it like this: `cargo test prop_large_volume_benchmark -- --nocapture`

   Ignoring the csv parsing timelines, the benchmark results in the following:

   ```
   Processed 100k transactions in 69.370625ms (1441532 tx/sec)
   Processed 100k transactions in 68.313125ms (1463848 tx/sec)
   Processed 100k transactions in 68.34425ms (1463181 tx/sec)
   Processed 100k transactions in 68.310333ms (1463907 tx/sec)
   Processed 100k transactions in 68.035916ms (1469812 tx/sec)
   Processed 100k transactions in 69.334083ms (1442292 tx/sec)
   ```

   So, we process 1.4 million transactions per second.

> All of the acual implementation logic fits into 125 lines of rust. So I could not rationalize breaking up the main.rs into more files or applying any fancy architectures (like clean code architecture or the likes). Keeping things simple is also a way to make code inherently maintainable.
