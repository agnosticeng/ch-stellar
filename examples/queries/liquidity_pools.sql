with 
    galexie as (
        select * from file('./tmp/galexie_sample.bin', 'Native')
    ),

    ledgers as (
        select
            coalesce(
                ledger_close_meta.v0.ledger_header.header.ledger_seq::Nullable(UInt32),
                ledger_close_meta.v1.ledger_header.header.ledger_seq::Nullable(UInt32),
                ledger_close_meta.v2.ledger_header.header.ledger_seq::Nullable(UInt32)
            ) as ledger_sequence,

            coalesce(
                ledger_close_meta.v0.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC')),
                ledger_close_meta.v1.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC')),
                ledger_close_meta.v2.ledger_header.header.scp_value.close_time::Nullable(DateTime64(3, 'UTC'))
            ) as ledger_closed_at,

            coalesce(
                ledger_close_meta.v0.ledger_header.hash::Nullable(String),
                ledger_close_meta.v1.ledger_header.hash::Nullable(String),
                ledger_close_meta.v2.ledger_header.hash::Nullable(String)
            ) as ledger_hash,

            arrayConcat(
                ledger_close_meta.v0.tx_set.txs[]::Array(JSON),
                arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v0[].txset_comp_txs_maybe_discounted_fee.txs[])::Array(JSON),
                arrayFlatten(ledger_close_meta.v1.tx_set.v1.phases[].v1.execution_stages[][][])::Array(JSON),
                arrayFlatten(ledger_close_meta.v2.tx_set.v1.phases[].v0[].txset_comp_txs_maybe_discounted_fee.txs[])::Array(JSON),
                arrayFlatten(ledger_close_meta.v2.tx_set.v1.phases[].v1.execution_stages[][][])::Array(JSON)
            ) as tx_envelopes,

            arrayConcat(
                ledger_close_meta.v0.tx_processing[],
                ledger_close_meta.v1.tx_processing[],
                ledger_close_meta.v2.tx_processing[]
            ) as tx_metas,

            arrayConcat(
                ledger_close_meta.v0.upgrades_processing[].changes[],
                ledger_close_meta.v1.upgrades_processing[].changes[],
                ledger_close_meta.v2.upgrades_processing[].changes[]
            ) as upgrade_changes
        from galexie
    ),

    tx_envelopes as (
        select
            * except (tx_envelopes),
            tx_envelope
        from ledgers 
        array join 
            tx_envelopes as tx_envelope
    ),

    tx_metas as (
        select 
            tx_meta,
            tx_order
        from ledgers
        array join 
            tx_metas as tx_meta,
            arrayEnumerate(tx_metas) as tx_order
    ),

    txs as (
        select 
            ledger_sequence,
            ledger_closed_at,
            ledger_hash,
            stellar_hash_transaction(tx_envelope::String, 'Public Global Stellar Network ; September 2015') as hash,
            stellar_id(ledger_sequence::Int32, tx_order::Int32, 0::Int32) as id,
            splitByChar('.', JSONAllPaths(tx_meta.^result.result.result)[1])[1] as result_code,
            
            firstNonDefault(
                tx_envelope.tx.tx.source_account::String,
                tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.source_account::String
            ) as source_account,

            tx_order,

            arrayConcat(
                tx_envelope.tx.tx.operations[],
                tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx.operations[]
            )::Array(JSON) as ops,

            arrayConcat(
                tx_meta.result.result.result.tx_success[],
                tx_meta.result.result.result.tx_failed[],
                tx_meta.result.result.result.tx_fee_bump_inner_success.result.result.tx_success[],
                tx_meta.result.result.result.tx_fee_bump_inner_failed.result.result.tx_failed[]
            )::Array(JSON) as ops_result,

            arrayConcat(
                tx_meta.tx_apply_processing[],
                tx_meta.tx_apply_processing.v1.operations[],
                tx_meta.tx_apply_processing.v2.operations[],
                tx_meta.tx_apply_processing.v3.operations[],
                tx_meta.tx_apply_processing.v4.operations[]
            ) as ops_meta,

            tx_meta.fee_processing[] as fee_changes,

            arrayConcat(
                tx_meta.tx_apply_processing.v1.tx_changes[]
            ) as tx_changes,

            arrayConcat(
                tx_meta.tx_apply_processing.v2.tx_changes_before[],
                tx_meta.tx_apply_processing.v3.tx_changes_before[],
                tx_meta.tx_apply_processing.v4.tx_changes_before[]
            ) as tx_changes_before,

            arrayConcat(
                tx_meta.tx_apply_processing.v2.tx_changes_after[],
                tx_meta.tx_apply_processing.v3.tx_changes_after[],
                tx_meta.tx_apply_processing.v4.tx_changes_after[]
            ) as tx_changes_after

        from tx_envelopes
        left join tx_metas
        on hash = tx_meta.result.transaction_hash::String
    ),

    ops as (
        select 
            ledger_sequence,
            ledger_closed_at,
            ledger_hash,
            hash as transaction_hash,
            id as transaction_id,
            result_code as transaction_result_code,
            source_account as transaction_source_account,
            stellar_id(ledger_sequence::Int32, tx_order::Int32, op_order::Int32) as id,
            op.source_account::String as source_account_muxed,

            firstNonDefault(
                splitByChar('.', JSONAllPaths(op.^body)[1])[1],
                op.body::String
            ) as type,

            op_meta.changes[] as changes

        from txs
        array join 
            ops as op,
            ops_result as op_result,
            ops_meta as op_meta,
            arrayEnumerate(ops) as op_order
        where result_code in ('success', 'tx_fee_bump_inner_success')
    ),

    upgrade_changes as (
        select 
            ledger_sequence,
            ledger_closed_at,
            ledger_hash,
            null as transaction_hash,
            null as transaction_id,
            null as source_account,
            null as operation_id,
            change::JSON as change
        from ledgers
        array join arrayFlatten(upgrade_changes) as change
    ),

    tx_changes as (
        select 
            ledger_sequence,
            ledger_closed_at,
            ledger_hash,
            hash as transaction_hash,
            id as transaction_id,
            source_account,
            null as operation_id,
            change::JSON as change
        from txs 
        array join tx_changes as change
    ),

    tx_changes_before as (
        select 
            ledger_sequence,
            ledger_closed_at,
            ledger_hash,
            hash as transaction_hash,
            id as transaction_id,
            source_account,
            null as operation_id,
            change::JSON as change
        from txs 
        array join tx_changes_before as change
    ),

    tx_changes_after as (
        select 
            ledger_sequence,
            ledger_closed_at,
            ledger_hash,
            hash as transaction_hash,
            id as transaction_id,
            source_account,
            null as operation_id,
            change::JSON as change
        from txs 
        array join tx_changes_after as change
    ),

    fee_changes as (
        select 
            ledger_sequence,
            ledger_closed_at,
            ledger_hash,
            hash as transaction_hash,
            id as transaction_id,
            source_account,
            null as operation_id,
            change::JSON as change
        from txs 
        array join fee_changes as change  
    ),

    ops_changes as (
        select 
            ledger_sequence,
            ledger_closed_at,
            ledger_hash,
            transaction_hash,
            transaction_id,
            firstNonDefault(
                source_account_muxed::String,
                transaction_source_account::String
            ) as source_account,
            id as operation_id,
            change::JSON as change
        from ops 
        array join changes as change
    ),

    all_changes as (
        select * from upgrade_changes
        union all 
        select * from tx_changes
        union all
        select * from tx_changes_before
        union all 
        select * from tx_changes_after
        union all 
        select * from fee_changes
        union all
        select * from ops_changes
    ),

    changes as (
        select 
            * except (change),
            splitByChar('.', JSONAllPaths(change)[1])[1] as change_type,
            firstNonDefault(
                change.^created,
                change.^updated,
                change.^removed,
                change.^state,
                change.^restored
            ) as ledger_entry
        from all_changes
    ),

    liquidity_pools as (
        select
            * except (ledger_entry),
            ledger_entry.last_modified_ledger_seq::UInt32 as last_modified_ledger_sequence,
            ledger_entry.^data.liquidity_pool as liquidity_pool_entry
        from changes
        where empty(liquidity_pool_entry) = 0
    )

select 
    * except (liquidity_pool_entry),
    liquidity_pool_entry.liquidity_pool_id::String as liquidity_pool_id,
    splitByChar('.', JSONAllPaths(liquidity_pool_entry.^body)[1])[1] as type,
    
    coalesce(
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.fee::Nullable(UInt64)
    ) as fee,

    coalesce(
        liquidity_pool_entry.body.liquidity_pool_constant_product.pool_shares_trust_line_count::String
    ) as trustline_count,

    coalesce(
        liquidity_pool_entry.body.liquidity_pool_constant_product.total_pool_shares
    ) as pool_share_count,

    firstNonDefault(
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_a::Nullable(String),
        splitByChar('.', JSONAllPaths(liquidity_pool_entry.^body.liquidity_pool_constant_product.params.asset_a)[1])[1]
    ) as asset_a_type,

    firstNonDefault(
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_a.credit_alphanum4.asset_code::String,
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_a.credit_alphanum12.asset_code::String,
    ) as asset_a_code,

    firstNonDefault(
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_a.credit_alphanum4.issuer::String,
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_a.credit_alphanum12.issuer::String,
    ) as asset_a_issuer,

    coalesce(
        liquidity_pool_entry.body.liquidity_pool_constant_product.reserve_a::Nullable(UInt64)
    ) as asset_a_amount,

    firstNonDefault(
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_b::Nullable(String),
        splitByChar('.', JSONAllPaths(liquidity_pool_entry.^body.liquidity_pool_constant_product.params.asset_b)[1])[1]
    ) as asset_b_type,

    firstNonDefault(
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_b.credit_alphanum4.asset_code::String,
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_b.credit_alphanum12.asset_code::String,
    ) as asset_b_code,

    firstNonDefault(
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_b.credit_alphanum4.issuer::String,
        liquidity_pool_entry.body.liquidity_pool_constant_product.params.asset_b.credit_alphanum12.issuer::String,
    ) as asset_b_issuer,

    coalesce(
        liquidity_pool_entry.body.liquidity_pool_constant_product.reserve_b::Nullable(UInt64)
    ) as asset_b_amount
from liquidity_pools

format Vertical
settings 
    output_format_arrow_string_as_string=0,
    enable_unaligned_array_join = 1,
    enable_named_columns_in_function_tuple=1