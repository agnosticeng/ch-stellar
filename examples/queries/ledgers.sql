with 
    ledgers as (
        select * from file('./tmp/galexie_sample.bin', 'Native')
    )

select 
    coalesce(
        ledger_close_meta.v0.ledger_header.header.ledger_seq::Nullable(UInt32),
        ledger_close_meta.v1.ledger_header.header.ledger_seq::Nullable(UInt32)
    ) as sequence,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC')),
        ledger_close_meta.v1.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC'))
    ) as close_time,

    coalesce(
        ledger_close_meta.v0.ledger_header.hash::Nullable(String),
        ledger_close_meta.v1.ledger_header.hash::Nullable(String)
    ) as hash,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.previous_ledger_hash::Nullable(String),
        ledger_close_meta.v1.ledger_header.header.previous_ledger_hash::Nullable(String)
    ) as previous_ledger_hash,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.total_coins::Nullable(UInt64),
        ledger_close_meta.v1.ledger_header.header.total_coins::Nullable(UInt64)
    ) as total_coins,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.fee_pool::Nullable(UInt64),
        ledger_close_meta.v1.ledger_header.header.fee_pool::Nullable(UInt64)
    ) as fee_pool,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.base_fee::Nullable(UInt64),
        ledger_close_meta.v1.ledger_header.header.base_fee::Nullable(UInt64)
    ) as base_fee,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.base_reserve::Nullable(UInt64),
        ledger_close_meta.v1.ledger_header.header.base_reserve::Nullable(UInt64)
    ) as base_reserve,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.max_tx_set_size::Nullable(UInt32),
        ledger_close_meta.v1.ledger_header.header.max_tx_set_size::Nullable(UInt32)
    ) as max_tx_set_size,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.ledger_version::Nullable(UInt32),
        ledger_close_meta.v1.ledger_header.header.ledger_version::Nullable(UInt32)
    ) as ledger_version,

    length(
        arrayConcat(
            ledger_close_meta.v0.tx_set.txs::Array(JSON),
            arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v0[].txset_comp_txs_maybe_discounted_fee.txs[])::Array(JSON),
            arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v1.execution_stages[][][])::Array(JSON)
        )
    ) as transaction_count,

    ledger_close_meta.v1.ext.v1.soroban_fee_write1_kb::Nullable(UInt64) as soroban_fee_write1_kb,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.scp_value.ext.signed.node_id::Nullable(String),
        ledger_close_meta.v1.ledger_header.header.scp_value.ext.signed.node_id::Nullable(String)
    ) as node_id,

    coalesce(
        ledger_close_meta.v0.ledger_header.header.scp_value.ext.signed.signature::Nullable(String),
        ledger_close_meta.v1.ledger_header.header.scp_value.ext.signed.signature::Nullable(String)
    ) as signature,

    ledger_close_meta.v1.total_byte_size_of_live_soroban_state::Nullable(UInt64) as total_byte_size_of_live_soroban_state,

    arraySum(
        arrayMap(
            tx -> (
                tx.result.result.result.tx_success is not null
                or 
                tx.result.result.result.tx_fee_bump_inner_success is not null
            ),
            arrayConcat(
                ledger_close_meta.v0.tx_processing[],
                ledger_close_meta.v1.tx_processing[]
            )
        )
    ) as successful_transaction_count,

    arraySum(
        arrayMap(
            tx -> (
                tx.result.result.result.tx_success is null
                and 
                tx.result.result.result.tx_fee_bump_inner_success is null
            ),
            arrayConcat(
                ledger_close_meta.v0.tx_processing[],
                ledger_close_meta.v1.tx_processing[]
            )
        )
    ) as failed_transaction_count,

    arraySum(
        arrayMap(
            tx -> 
                length(tx.tx_apply_processing[]) +
                length(tx.tx_apply_processing.v1.operations[]) +
                length(tx.tx_apply_processing.v2.operations[]) +
                length(tx.tx_apply_processing.v3.operations[]) +
                length(tx.tx_apply_processing.v4.operations[]) 
            ,
            arrayFilter(
                tx -> (
                    tx.result.result.result.tx_success is not null
                    or 
                    tx.result.result.result.tx_fee_bump_inner_success is not null
                ), 
                arrayConcat(
                    ledger_close_meta.v0.tx_processing[],
                    ledger_close_meta.v1.tx_processing[]
                )
            )
        )
    ) as operation_count,


    (
        length(ledger_close_meta.v0.tx_set.txs[].tx.tx.tx.operations[]) 
        +
        length(arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v0[].txset_comp_txs_maybe_discounted_fee.txs[].tx.tx.operations[])) 
        +
        length(arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v1.execution_stages[][][].tx.tx.operations[]))
    ) as tx_set_operation_count

from ledgers

settings output_format_arrow_string_as_string=0
