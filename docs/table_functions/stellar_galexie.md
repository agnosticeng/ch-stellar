### stellar_galexie

Create a table from [Stellar Galexie](https://developers.stellar.org/docs/data/indexers/build-your-own/galexie) data lake.

**Syntax**

```sql
executable(
    'ch-stellar table-function stellar-galexie',
    ArrowStream, 
    'ledger_close_meta JSON',
    (...)
)
```

**Input query result shape**

- `url` - The URL of a Galexie data lake [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
- `start` - The start ledger sequence. [UInt32](https://clickhouse.com/docs/sql-reference/data-types/int-uint)  
- `end` - The end ledger sequence. [UInt32](https://clickhouse.com/docs/sql-reference/data-types/int-uint)

**Returned table**

- The returned table contains a single column containing the JSON representation of the [LedgerCloseMeta structure](https://developers.stellar.org/docs/data/indexers/build-your-own/ingest-sdk/developer_guide/ledgerbackends#ledgerclosemeta-structure), wrapped in a [`Result`](../error_handling.md).
The output table will contain one row per ledger in the various input ranges.

**Example**

Query:

```sql
select 
    ledger_close_meta.v0.ledger_header.header.ledger_seq
from executable(
    'ch-stellar table-function stellar-galexie',
    ArrowStream, 
    'ledger_close_meta JSON',
    (
        select
            'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst' as url,
            50000000 as start,
            50000005 as end
    ),
    settings stderr_reaction='log', check_exit_code=true
)
settings output_format_arrow_string_as_string=0
```

Result:

| ledger_close_meta.v0.ledger_header.header.ledger_seq |
|:-|
| 50000000 |
| 50000001 |
| 50000002 |
| 50000003 |
| 50000004 |
| 50000005 |