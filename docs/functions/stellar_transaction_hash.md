### stellar_transaction_hash

Generate the hash of a transaction given it's JSON-encoded envelope.

**Syntax**

```sql
select stellar_transaction_hash(envelope, passphrase)
```

**Parameters**

- `envelope` - An JSON-encoded [transaction envelope](https://github.com/stellar/stellar-xdr/blob/0a621ec7811db000a60efae5b35f78dee3aa2533/Stellar-transaction.x#L973). [String](https://clickhouse.com/docs/sql-reference/data-types/string)
- `passphrase` - The network passphrase. [String](https://clickhouse.com/docs/sql-reference/data-types/string)

**Returned value**

- The hash as an hexadecimal string.

**Example**

Query:

```sql
select stellar_hash_transaction('{"tx":{"signatures":[{"hint":"7ecc9ca6","signature":"eb04265e6c6720e0427d4f5ce43fa7931658e8842e54df09ff1e9d49200dccd342ef1a234190742a81d4f4ee6cbab3f14c873a5c722c4e6ea823cb7ee9d82509"}],"tx":{"cond":{"time":{"max_time":"1729245920","min_time":"0"}},"ext":"v0","fee":5000,"memo":"none","operations":[{"body":{"manage_buy_offer":{"buy_amount":"239170638","buying":"native","offer_id":"1616775113","price":{"d":841967,"n":1542346615},"selling":{"credit_alphanum12":{"asset_code":"BNDES","issuer":"GAWG3NPQXQEJYQY3SXAOI62W5MTORFVOGYLOWDXEHPJAG62PPKC47DGB"}}}}}],"seq_num":"175163940450866598","source_account":"GDGRBF2GVDNBXUC4QYG5PGW7ASGA64DEXWO4X76JVDQ4XS36ZSOKM65T"}}}', 'Public Global Stellar Network ; September 2015') as hash 
settings output_format_arrow_string_as_string=0 
format Markdown
```

Result:

| hash |
|:-|
| 54960f6ca1cd131ad26fda2c217d927b6ceed75678dc1c462a388aaff35ac7fb |