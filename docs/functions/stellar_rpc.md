### stellar_rpc

Call [stellar-rpc methods](https://developers.stellar.org/docs/data/apis/rpc).

**Syntax**

```sql
stellar_rpc(endpoint, method, params)
```

**Parameters**

- `endpoint` - An Stellar-RPC endpoint URL. [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
- `method` - Any RPC method supported by the endpoint. [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
- `params` - An JSON-encoded object representing the parameters of the RPC method. [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)


**Returned value**

- The response to the RPC call, wrapped in a [`Result`](../error_handling.md).

**Example**

Query:

```sql
select 
    stellar_rpc(
        'https://rpc.lightsail.network/',
        'getHealth', 
        'null'
    ) as res
settings output_format_arrow_string_as_string=0
```

Result:

| res |
|:-|
| {"value":{"latestLedger":59107822,"ledgerRetentionWindow":120960,"oldestLedger":58986863,"status":"healthy"}} |