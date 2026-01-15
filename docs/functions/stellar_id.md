### stellar_id

Generate a [Stellar operation id](https://github.com/stellar/stellar-etl/blob/master/internal/toid/main.go).

**Syntax**

```sql
select stellar_id(ledger_sequence, transaction_order, operation_order)
```

**Parameters**

- `ledger_sequence` - The sequence number of the ledger the. [Int32](https://clickhouse.com/docs/sql-reference/data-types/int-uint)
- `transaction_order` - The order of the transaction within the ledger. [Int32](https://clickhouse.com/docs/sql-reference/data-types/int-uint)
- `operation_order` - The order of the operation within the transaction. [Int32](https://clickhouse.com/docs/sql-reference/data-types/int-uint)

**Returned value**

- The id as an Int64.

**Example**

Query:

```sql
select stellar_id(1::Int32, 1::Int32, 1::Int32)
```

Result:

| id |
|-:|
| 4294971393 |