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
        )as tx_meta
    )    

select 
    txs.* except (tx),

    stellar_hash_transaction(txs.tx::String, 'Public Global Stellar Network ; September 2015') as hash,
    
    coalesce(
        txs.tx.tx.tx.source_account::Nullable(String),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.source_account::Nullable(String)
    ) as account,

    coalesce(
        txs.tx.tx.tx.seq_num::Nullable(UInt64),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.seq_num::Nullable(UInt64)
    ) as sequence,

    coalesce(
        txs.tx.tx.tx.fee::Nullable(UInt64),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.fee::Nullable(UInt64)
    ) as max_fee,

    length(
        arrayConcat(
            txs.tx.tx.tx.operations[],
            txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.operations[]
        )
    ) as operation_count,

    coalesce(
        splitByChar('.', JSONAllPaths(txs_meta.tx_meta.^result.result.result)[1], 2)[1],
        splitByChar('.', JSONAllPaths(txs_meta.tx_meta.^result.result.result)[1], 2)[1]
    ) as result_code, 

    multiIf(
        txs.tx.tx.tx.memo.text::Nullable(String) is not null, 'text',
        txs.tx.tx.tx.memo.hash::Nullable(String) is not null, 'hash',
        txs.tx.tx.tx.memo.id::Nullable(UInt64) is not null, 'id',
        txs.tx.tx.tx.memo.return::Nullable(String) is not null, 'return',
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.memo.text::Nullable(String) is not null, 'text',
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.memo.hash::Nullable(String) is not null, 'hash',
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.memo.id::Nullable(UInt64) is not null, 'id',
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.memo.return::Nullable(String) is not null, 'return',
        'none'
    ) as memo_type,

    multiIf(
        txs.tx.tx.tx.memo.text::Nullable(String) is not null, txs.tx.tx.tx.memo.text::String,
        txs.tx.tx.tx.memo.hash::Nullable(String) is not null, txs.tx.tx.tx.memo.hash::String,
        txs.tx.tx.tx.memo.id::Nullable(UInt64) is not null, txs.tx.tx.tx.memo.id::String,
        txs.tx.tx.tx.memo.return::Nullable(String) is not null, txs.tx.tx.tx.memo.return::String,
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.memo.text::Nullable(String) is not null, txs.tx.tx.tx.memo.text::String,
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.memo.hash::Nullable(String) is not null, txs.tx.tx.tx.memo.hash::String,
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.memo.id::Nullable(UInt64) is not null, txs.tx.tx.tx.memo.id::String,
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.memo.return::Nullable(String) is not null, txs.tx.tx.tx.memo.return::String,
        null
    ) as memo,

    coalesce(
        txs.tx.tx.tx.cond.time.min_time::Nullable(DateTime64(3, 'UTC')),
        txs.tx.tx.tx.cond.time_bounds.min_time::Nullable(DateTime64(3, 'UTC')),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.time.min_time::Nullable(DateTime64(3, 'UTC')),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.time_bounds.min_time::Nullable(DateTime64(3, 'UTC'))
    ) as min_time_bound,

    coalesce(
        txs.tx.tx.tx.cond.time.max_time::Nullable(DateTime64(3, 'UTC')),
        txs.tx.tx.tx.cond.time_bounds.max_time::Nullable(DateTime64(3, 'UTC')),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.time.max_time::Nullable(DateTime64(3, 'UTC')),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.time_bounds.max_time::Nullable(DateTime64(3, 'UTC'))
    ) as max_time_bound,

    coalesce(
        txs.tx.tx.tx.cond.v2.ledger_bounds.min_ledger::Nullable(UInt32),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.ledger_bounds.min_ledger::Nullable(UInt32)
    ) as min_ledger_bound,

    coalesce(
        txs.tx.tx.tx.cond.v2.ledger_bounds.max_ledger::Nullable(UInt32),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.ledger_bounds.max_ledger::Nullable(UInt32)
    ) as max_ledger_bound,

    coalesce(
        txs.tx.tx.tx.cond.v2.min_account_sequence::Nullable(UInt64),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.min_account_sequence::Nullable(UInt64)
    ) as min_account_sequence,

    coalesce(
        txs.tx.tx.tx.cond.v2.min_seq_age::Nullable(UInt64),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.min_seq_age::Nullable(UInt64)
    ) as min_account_sequence_age,

    coalesce(
        txs.tx.tx.tx.cond.v2.min_seq_ledger_gap::Nullable(UInt32),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.min_seq_ledger_gap::Nullable(UInt32)
    ) as min_account_sequence_ledger_gap,

    arrayConcat(
        txs.tx.tx.tx.cond.v2.extra_signers[],
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.cond.v2.extra_signers[]
    ) as extra_signers,

    coalesce(
        splitByChar('.', JSONAllPaths(txs_meta.tx_meta.^result.result.result)[1], 2)[1],
        splitByChar('.', JSONAllPaths(txs_meta.tx_meta.^result.result.result)[1], 2)[1]
    ) as result_code, 

    if (
        result_code in ['tx_fee_bump_inner_success', 'tx_success'],
        1,
        0
    ) as success,

    txs_meta.tx_meta.result.result.fee_charged::UInt64 as fee_charged,

    txs_meta.tx_meta.result.result.result.tx_fee_bump_inner_success.transaction_hash::String as inner_transaction_hash,

    txs.tx.tx_fee_bump.tx.fee_source::String as fee_account,

    txs.tx.tx_fee_bump.tx.fee::UInt64 as new_max_fee,

    coalesce(
        txs.tx.tx.tx.ext.v1.resource_fee::Nullable(UInt64),
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.ext.v1.resource_fee::Nullable(UInt64)
    ) as resource_fee,

    coalesce (
            txs_meta.tx_meta.tx_apply_processing.v3.soroban_meta.ext.v1.total_non_refundable_resource_fee_charged::Nullable(UInt64),
            txs_meta.tx_meta.tx_apply_processing.v4.soroban_meta.ext.v1.total_non_refundable_resource_fee_charged::Nullable(UInt64)
    ) as non_refundable_resource_fee_charged,

    coalesce(
        txs_meta.tx_meta.tx_apply_processing.v3.soroban_meta.ext.v1.total_refundable_resource_fee_charged::Nullable(UInt64),
        txs_meta.tx_meta.tx_apply_processing.v4.soroban_meta.ext.v1.total_refundable_resource_fee_charged::Nullable(UInt64)
    ) as refundable_resource_fee_charged,

    coalesce(
        txs_meta.tx_meta.tx_apply_processing.v3.soroban_meta.ext.v1.rent_fee_charged::Nullable(UInt64),
        txs_meta.tx_meta.tx_apply_processing.v4.soroban_meta.ext.v1.rent_fee_charged::Nullable(UInt64)
    ) as rent_fee_charged,

    non_refundable_resource_fee_charged + refundable_resource_fee_charged as resource_fee_charged,

    max_fee - resource_fee as inclusion_fee,

    fee_charged - resource_fee_charged as inclusion_fee_charged,

    coalesce(
        txs.tx.tx.tx.ext.v1.resources.instructions::UInt64,
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.ext.v1.resources.instructions::UInt64
    ) as soroban_resources_instructions,

    coalesce(
        txs.tx.tx.tx.ext.v1.resources.disk_read_bytes::UInt64,
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.ext.v1.resources.disk_read_bytes::UInt64
    ) as soroban_resources_read_bytes,

    coalesce(
        txs.tx.tx.tx.ext.v1.resources.write_bytes::UInt64,
        txs.tx.tx_fee_bump.tx.inner_tx.tx.tx.ext.v1.resources.write_bytes::UInt64
    ) as soroban_resources_write_bytes

    -- Potentially missing columns:

    -- account_muxed,
    -- fee_account_muxed
    -- resource_fee_refund
    -- tx_signers
    -- refundable_fee

from txs
left join txs_meta
on hash = tx_meta.result.transaction_hash::String

settings output_format_arrow_string_as_string=0
