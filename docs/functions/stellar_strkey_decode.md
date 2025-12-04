### stellar_strkey_decode

Decodes Stellar strkeys to structured objects.

**Syntax**

```sql
select stellar_strkey_decode(str)
```

**Parameters**

- `str` - The Stellar key string representation. [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)

**Returned value**

- A decoded key in the form of a JSON object.

**Example**

Query:

```sql
select stellar_strkey_decode('GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF') as decoded

settings output_format_arrow_string_as_string=0
```

Result:

| decoded |
|:-|
| {"public_key_ed25519":"0000000000000000000000000000000000000000000000000000000000000000"} |
