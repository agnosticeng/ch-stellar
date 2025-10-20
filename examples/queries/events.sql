with 
    ledgers as (
        select * from file('./tmp/galexie_sample.bin', 'Native')
    ),

    txs as (
        select
            coalesce(
                ledger_close_meta.v0.ledger_header.header.ledger_seq::Nullable(UInt32),
                ledger_close_meta.v1.ledger_header.header.ledger_seq::Nullable(UInt32)
            ) as ledger_sequence,
            coalesce(
                ledger_close_meta.v0.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC')),
                ledger_close_meta.v1.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC'))
            ) as ledger_close_time,
            coalesce(
                ledger_close_meta.v0.ledger_header.hash::Nullable(String),
                ledger_close_meta.v1.ledger_header.hash::Nullable(String)
            ) as ledger_hash,
            tx
        from ledgers 
        array join arrayConcat(
            ledger_close_meta.v0.tx_set.txs[]::Array(JSON),
            arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v0[].txset_comp_txs_maybe_discounted_fee.txs[])::Array(JSON),
            arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v1.execution_stages[][][])::Array(JSON)
        ) as tx
    ),

    txs_meta as (
        select 
            tx_meta 
        from ledgers
        array join arrayConcat(
            ledger_close_meta.v0.tx_processing[],
            ledger_close_meta.v1.tx_processing[]
        ) as tx_meta
    ),    

    txs_match as (
        select 
            txs.* except (tx),
            stellar_hash_transaction(txs.tx::String, 'Public Global Stellar Network ; September 2015') as hash,
            tx,
            tx_meta
        from txs
        left join txs_meta
        on hash = tx_meta.result.transaction_hash::String
    ),

    ops as (
        select 
            ledger_sequence,
            ledger_close_time,
            ledger_hash,
            hash as transaction_hash,

            coalesce(
                splitByChar('.', JSONAllPaths(tx_meta.^result.result.result)[1], 2)[1],
                splitByChar('.', JSONAllPaths(tx_meta.^result.result.result)[1], 2)[1]
            ) as transaction_result_code,

            coalesce(
                tx.tx.tx.source_account::Nullable(String),
                tx.tx_fee_bump.tx.inner_tx.tx.tx.source_account::Nullable(String)
            ) as source_account,

            source_account::String as source_account_muxed,

            splitByChar('.', JSONAllPaths(op.^body)[1], 2)[1] as type,

            op,

            op_apply

        from txs_match
        array join 
            arrayConcat(
                tx.tx.tx.operations[],
                tx.tx_fee_bump.tx.inner_tx.tx.tx.operations[]
            ) as op,

            arrayConcat(
                tx_meta.tx_apply_processing.v4.operations[]
            ) as op_apply
    ),

    tx_events as (
        select 
            ledger_sequence,
            ledger_close_time,
            ledger_hash,
            hash as transaction_hash,

            coalesce(
                splitByChar('.', JSONAllPaths(tx_meta.^result.result.result)[1], 2)[1],
                splitByChar('.', JSONAllPaths(tx_meta.^result.result.result)[1], 2)[1]
            ) as transaction_result_code,

            coalesce(
                tx.tx.tx.source_account::Nullable(String),
                tx.tx_fee_bump.tx.inner_tx.tx.tx.source_account::Nullable(String)
            ) as source_account,

            source_account::String as source_account_muxed,
            event::JSON as event
        from txs_match
        array join tx_meta.tx_apply_processing.v4.events[] as event
    ),

    ops_events as (
        select
            ledger_sequence,
            ledger_close_time,
            ledger_hash,
            transaction_hash,
            transaction_result_code,
            source_account,
            source_account_muxed,
            event::JSON as event
        from ops
        array join op_apply.events[] as event
    ),

    all_events as (
        select * from tx_events
        union all
        select * from ops_events
    )

select 
    * except event,  
    event.contract_id::String as contract_id,
    event.type_::String as type,
    event.body.v0.topics[] as topics,
    event.^body.v0.data as data
from all_events

settings 
    output_format_arrow_string_as_string=0,
    enable_unaligned_array_join = 1