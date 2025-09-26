### stellar_xdr_decode

Decodes an [XDR-encoded](https://en.wikipedia.org/wiki/External_Data_Representation) [Stellar datastructures](https://github.com/stellar/stellar-xdr).

**Syntax**

```sql
select stellar_xdr_decode(type, data)
```

**Parameters**

- `type` - The name of the type of the XDR-encoded data (all suuported types listed [here](https://docs.rs/stellar-xdr/latest/stellar_xdr/next/enum.TypeVariant.html)). [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
- `data` - Base64-encoded data to decode. [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)

**Returned value**

- The decoded [Stellar datastructures](https://github.com/stellar/stellar-xdr), wrapped in a [`Result`](../error_handling.md).

**Example**

Query:

```sql
select stellar_xdr_decode('TransactionEvent', 'AAAAAAAAAAAAAAABJbT82FmuwvpjSEOMSJs8PBDJi20hvk\/TyzDLaJU++XcAAAABAAAAAAAAAAIAAAAPAAAAA2ZlZQAAAAASAAAAAAAAAABmOMzcP3GB7aqvq\/w2S20taRvi4ycXhF9ChvSvlfhGjAAAAAoAAAAAAAAAAAAAAAAAAAEs') as decoded
settings output_format_arrow_string_as_string=0
```

Result:

| decoded |
|:-|
| {"value":{"event":{"body":{"v0":{"data":{"i128":"300"},"topics":[{"symbol":"fee"},{"address":"GBTDRTG4H5YYD3NKV6V7YNSLNUWWSG7C4MTRPBC7IKDPJL4V7BDIY426"}]}},"contract_id":"CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2EVH34XOWMA","ext":"v0","type_":"contract"},"stage":"before_all_txs"}} |