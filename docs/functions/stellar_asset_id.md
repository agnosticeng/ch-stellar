### stellar_asset_id

Generate a Stellar asset id.

**Syntax**

```sql
select stellar_asset_id(asset_code, asset_issuer, asset_type)
```

**Parameters**

- `asset_code` - The asset code. [String](https://clickhouse.com/docs/sql-reference/data-types/string)
- `asset_issuer` - The account of the asset's issuer. [Int32](https://clickhouse.com/docs/sql-reference/data-types/int-uint)
- `asset_type` - The asset type. [Int32](https://clickhouse.com/docs/sql-reference/data-types/int-uint)

**Returned value**

- The id as an UInt64. The id is computed as farmhash(concat(asset_code, asset_issuer, asset_type))

**Example**

Query:

```sql
select stellar_asset_id('AstroDollar', 'GC2BKLYOOYPDEFJKLKY6FNNRQMGFLVHJKQRGNSSRRGSMPGF32LHCQVGF', 'credit_alphanum12') as id
```

Result:

| id |
|-:|
| 5914178279203891945 |