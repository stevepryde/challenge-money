# Coding task

Simulate processing of transactions.

## Usage

To run the tests:

    cargo test

To run with a CSV file input:

    cargo run --release -- input.csv > output.csv

## Notes

Several design decisions are listed below:

### Transactions are cached in memory for each client

In order to identity an existing transaction for a dispute, all deposits
and withdrawals need to be cached.

### Entire transaction history is stored in memory for each client

I also opted to store all history for checking purposes. This could be
disabled if you only care about the current values, but having an audit
trail is useful for other reasons (and yes this aspect could be moved to a
blockchain instead).

### Use of channels and threading

Currently the code only uses a single thread, but it's written in a way
that is easily extendable. The current version could support multiple CSV
readers pushing data to the same channel. However, by switching to a mpmc
channel it could support multiple processors/writers as well.

### Use of an in-memory "database"

Accounts are stored in a `HashMap` in memory. These are simulated using
nested locks for better performance, but ideally this would be moved to
some kind of persistent concurrent database. This would allow much larger
datasets to be processed, at the cost of performance where a client's data
needs to be read from/written to the database. I'd consider switching to
async in that case, probably using tokio.

### Logging

I disabled logging by default because I think the code is checked via an
automated process. The code is designed to log any errors such as attempting
to withdraw more than the available balance, dispute a missing transaction,
or resolve a missing dispute, etc.

### Code Safety

I added newtypes in a few places to make things more robust, particularly
anything that handles money values.

If there were many more transaction types, and especially if the list was
likely to grow over time or if some transaction types would live in another
crate, I would consider adding an `ApplyTransaction` trait and replace the
various `apply_*` functions with structs that implement that trait. That
provides more extensibility. However, it also adds complexity, and since the
number of transaction types is small, I opted for the simpler solution.

### Builders

I like the use of `bon` for builders combined with `#[non_exhaustive]`
because it makes code more future-proof. With `bon` it is very well documented
which operations can be done without breaking compatibility. Reducing
breaking changes is important because these have a very real time and energy
cost across the team.

### Testability

The use of `<W: Write>` and `<R: Read>` allow for tests to use these
functions with strings without needing to divert stdin/stdout.

### Property testing

I used `proptest` for some basic property testing. This makes use of the
transaction history to sanity check each account.