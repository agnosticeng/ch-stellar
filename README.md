# ch-stellar

**[Stellar](https://stellar.org/)-focused UDFs for ClickHouse – Accelerate [Stellar](https://stellar.org/) blockchain analytics**

## Overview

`ch-stellar` is a collection of high-performance [**user-defined functions (UDFs)**](https://clickhouse.com/docs/sql-reference/functions/udf) that extend [ClickHouse](https://clickhouse.com/) with capabilities tailored for [Stellar](https://stellar.org/) blockchain's data processing.

Whether you're building blockchain explorers, indexing on-chain data, or running deep analytics on Stellar chains, this project brings native decoding, parsing, and querying support into your [ClickHouse](https://clickhouse.com/) workflows.

## ✨ Features

- [**Fast, optimized RPC calls**](./docs/json_rpc_client.md) to Stellar-RPC nodes directly from ClickHouse queries
    - [stellar_rpc](./docs/functions/stellar_rpc.md)
- Utility functuins for working with Stellar XDR encoded structures
    - [stellar_xdr_decode](./docs/functions/stellar_xdr_decode.md)
- Query Galexie data lakes directly from within your SQL queries
    - [stellar_galexie](./docs/table_functions/stellar_galexie.md)


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
