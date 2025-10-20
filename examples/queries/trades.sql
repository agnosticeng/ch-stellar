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

            op.source_account::String as source_account_muxed,

            splitByChar('.', JSONAllPaths(op.^body)[1], 2)[1] as op_type,

            (
                case op_type 
                    when 'path_payment_strict_receive' then map(
                        'op', op.^body.path_payment_strict_receive::JSON,
                        'op_result', op_result.^op_inner.path_payment_strict_receive.success::JSON
                    )::JSON

                    when 'manage_sell_offer' then map(
                        'op', op.^body.manage_sell_offer::JSON,
                        'op_result', op_result.^op_inner.manage_sell_offer.success::JSON
                    )::JSON

                    when 'create_passive_sell_offer' then map(
                        'op', op.^body.create_passive_sell_offer::JSON,
                        'op_result', op_result.^op_inner.create_passive_sell_offer.success::JSON
                    )::JSON

                    when 'manage_buy_offer' then map(
                        'op', op.^body.manage_buy_offer::JSON,
                        'op_result', op_result.^op_inner.manage_buy_offer.success::JSON
                    )::JSON

                    when 'path_payment_strict_send' then map(
                        'op', op.^body.path_payment_strict_send::JSON,
                        'op_result', op_result.^op_inner.path_payment_strict_send.success::JSON
                    )::JSON

                    else map()::JSON
                end
            ) as op_match

        from txs_match
        array join 
            arrayConcat(
                tx.tx.tx.operations[],
                tx.tx_fee_bump.tx.inner_tx.tx.tx.operations[]
            ) as op,

            arrayConcat(
                tx_meta.result.result.result.tx_success[],
                tx_meta.result.result.result.tx_failed[],
                tx_meta.result.result.result.tx_fee_bump_inner_success.result.result.tx_success[],
                tx_meta.result.result.result.tx_fee_bump_inner_failed.result.result.tx_failed[]
            ) as op_result
        where transaction_result_code in ('success', 'tx_fee_bump_inner_success')
        and op_type in ('path_payment_strict_receive', 'manage_sell_offer', 'create_passive_sell_offer', 'manage_buy_offer', 'path_payment_strict_send')
    )

select 
    ledger_sequence,
    ledger_close_time,
    ledger_hash,
    transaction_hash,
    source_account,
    source_account_muxed,
    op_type,

    (
        case op_type 
            when 'path_payment_strict_receive' then map(
                'offers', op_match.^op_result.offers[]
            )::JSON

            when 'manage_buy_offer' then map(
                'offer', op_match.^op_result.offer,
                'offers_claimed', op_match.^op_result.offers_claimed
            )::JSON

            else map()::JSON
        end
    ) as t

from ops

settings 
    output_format_arrow_string_as_string=0,
    enable_unaligned_array_join = 1
