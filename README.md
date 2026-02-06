# ch-stellar

**[Stellar](https://stellar.org/)-focused UDFs for ClickHouse – Accelerate [Stellar](https://stellar.org/) blockchain analytics**

## Overview

`ch-stellar` is a collection of high-performance [**user-defined functions (UDFs)**](https://clickhouse.com/docs/sql-reference/functions/udf) that extend [ClickHouse](https://clickhouse.com/) with capabilities tailored for [Stellar](https://stellar.org/) blockchain's data processing.

Whether you're building blockchain explorers, indexing on-chain data, or running deep analytics on Stellar chains, this project brings native decoding, parsing, and querying support into your [ClickHouse](https://clickhouse.com/) workflows.

## ✨ Features

- [**Fast, optimized RPC calls**](./docs/json_rpc_client.md) to Stellar-RPC nodes directly from ClickHouse queries
    - [stellar_rpc](./docs/functions/stellar_rpc.md)    
- Query Galexie data lakes directly from within your SQL queries
    - [stellar_galexie](./docs/table_functions/stellar_galexie.md)
- Utility functions
    - [stellar_id](./docs/functions/stellar_id.md)
    - [stellar_transaction_hash](./docs/functions/stellar_transaction_hash.md)
    - [stellar_xdr_decode](./docs/functions/stellar_xdr_decode.md)
    - [stellar_strkey_decode](./docs/functions/stellar_strkey_decode.md)

## 📦 Artifact: The Bundle

The output of the build process is distributed as a **compressed archive** called a **bundle**. This bundle includes everything needed to deploy and use the UDFs in ClickHouse.

### 📁 Bundle Contents

Each bundle contains:

- 🧩 **Standalone binary** implementing the native UDFs (compiled with ClickHouse compatibility)
- ⚙️ **ClickHouse configuration files** (`.xml`) to register each native UDF
- 📝 **SQL files** for SQL-based UDFs (used for lightweight functions where SQL outperforms compiled code)

### 📦 Bundle Usage

#### 🛠️ Build the Bundle

```sh
make bundle              # Build for native execution
```

This will:

- Generate the bundle directory at `tmp/bundle/`
- Create a compressed archive at `tmp/bundle.tar.gz`

The internal file structure of the bundle reflects the default layout of a basic ClickHouse installation.  
As a result, **decompressing the archive at the root of a ClickHouse server filesystem should "just work"** with no additional path configuration.

---

#### ▶️ Run with `clickhouse-local`

```sh
clickhouse local \
    --log-level=debug \
    --path tmp/clickhouse \
    -- \
    --user_scripts_path="./tmp/bundle/var/lib/clickhouse/user_scripts" \
    --user_defined_executable_functions_config="./tmp/bundle/etc/clickhouse-server/*_function.*ml" \
    --user_defined_path="./tmp/bundle/var/lib/clickhouse/user_defined" \
    --send_logs_level=trace
```

This runs ClickHouse in local mode using the provided config and a temporary storage path.

---

#### 🐳 Run in development mode with `clickhouse-server` in Docker

```sh
docker compose up -d
```

This launches a ClickHouse server inside a Docker container using the configuration and UDFs from the bundle.

## Testing

### Integration Tests

UDFs are tested end-to-end by running SQL queries through `clickhouse local` with the compiled binary and UDF configs loaded, then comparing the actual output against expected `.tsv` snapshots.

The test runner (`tests/integration.rs`) uses [rstest](https://github.com/la10736/rstest)'s `#[files]` attribute to automatically discover every `.sql` file under `tests/sql/` (including subdirectories) and create one test case per file. Each test:

1. Executes the `.sql` file with `clickhouse local`, pointing it at the bundle in `tmp/bundle/`
2. Asserts that the query succeeds (non-zero exit code = immediate failure with stderr shown)
3. Compares stdout against the sibling `.tsv` file (e.g. `stellar_id.sql` → `stellar_id.tsv`)
4. On mismatch, prints a line-by-line diff

#### Prerequisites

- The bundle must be built first: `make build && make bundle`
- `clickhouse` must be available in `PATH` (override with `CLICKHOUSE_BIN=/path/to/clickhouse`)

#### Running the test suite

```sh
cargo test --test integration
```

Filter to specific tests:

```sh
cargo test --test integration stellar_id
```

#### Adding a new test

1. Create a `.sql` file in `tests/sql/` (subdirectories are supported for organization)
2. Generate the expected output snapshot:
    ```sh
    clickhouse local \
        --log-level=debug \
        -- \
        --user_scripts_path="./tmp/bundle/var/lib/clickhouse/user_scripts" \
        --user_defined_executable_functions_config="./tmp/bundle/etc/clickhouse-server/*_function.*ml" \
        --user_defined_path="./tmp/bundle/var/lib/clickhouse/user_defined" \
        < tests/sql/my_test.sql \
        > tests/sql/my_test.tsv
        
    ```
3. Commit both the `.sql` and `.tsv` files
4. Verify with `cargo test --test integration`
