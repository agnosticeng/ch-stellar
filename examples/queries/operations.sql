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

            splitByChar('.', JSONAllPaths(op.^body)[1], 2)[1] as type,

            (case type 
                when 'create_account' then tuple(
                    op.^body.create_account::JSON,
                    coalesce(
                        op_result.op_inner.create_account::Dynamic,
                        op_result.^op_inner.create_account::Dynamic
                    )
                )

                when 'payment' then tuple(
                    op.^body.payment::JSON,
                    coalesce(
                        op_result.op_inner.payment::Dynamic,
                        op_result.^op_inner.payment::Dynamic
                    )
                )

                when 'path_payment_strict_receive' then tuple(
                    op.^body.path_payment_strict_receive::JSON,
                    coalesce(
                        op_result.op_inner.path_payment_strict_receive::Dynamic,
                        op_result.^op_inner.path_payment_strict_receive::Dynamic
                    )
                )

                when 'manage_sell_offer' then tuple(
                    op.^body.manage_sell_offer::JSON,
                    coalesce(
                        op_result.op_inner.manage_sell_offer::Dynamic,
                        op_result.^op_inner.manage_sell_offer::Dynamic
                    )
                )

                when 'create_passive_sell_offer' then tuple(
                    op.^body.create_passive_sell_offer::JSON,
                    coalesce(
                        op_result.op_inner.create_passive_sell_offer::Dynamic,
                        op_result.^op_inner.create_passive_sell_offer::Dynamic
                    )
                )

                when 'set_options' then tuple(
                    op.^body.set_options::JSON,
                    coalesce(
                        op_result.op_inner.set_options::Dynamic,
                        op_result.^op_inner.set_options::Dynamic
                    )
                )

                when 'change_trust' then tuple(
                    op.^body.change_trust::JSON,
                    coalesce(
                        op_result.op_inner.change_trust::Dynamic,
                        op_result.^op_inner.change_trust::Dynamic
                    )
                )

                when 'allow_trust' then tuple(
                    op.^body.allow_trust::JSON,
                    coalesce(
                        op_result.op_inner.allow_trust::Dynamic,
                        op_result.^op_inner.allow_trust::Dynamic
                    )
                )

                when 'account_merge' then tuple(
                    op.^body.account_merge::JSON,
                    coalesce(
                        op_result.op_inner.account_merge::Dynamic,
                        op_result.^op_inner.account_merge::Dynamic
                    )
                )

                when 'inflation' then tuple(
                    op.^body.inflation::JSON,
                    coalesce(
                        op_result.op_inner.inflation::Dynamic,
                        op_result.^op_inner.inflation::Dynamic
                    )
                )

                when 'manage_data' then tuple(
                    op.^body.manage_data::JSON,
                    coalesce(
                        op_result.op_inner.manage_data::Dynamic,
                        op_result.^op_inner.manage_data::Dynamic
                    )
                )

                when 'bump_sequence' then tuple(
                    op.^body.bump_sequence::JSON,
                    coalesce(
                        op_result.op_inner.bump_sequence::Dynamic,
                        op_result.^op_inner.bump_sequence::Dynamic
                    )
                )

                when 'manage_buy_offer' then tuple(
                    op.^body.manage_buy_offer::JSON,
                    coalesce(
                        op_result.op_inner.manage_buy_offer::Dynamic,
                        op_result.^op_inner.manage_buy_offer::Dynamic
                    )
                )

                when 'path_payment_strict_send' then tuple(
                    op.^body.path_payment_strict_send::JSON,
                    coalesce(
                        op_result.op_inner.path_payment_strict_send::Dynamic,
                        op_result.^op_inner.path_payment_strict_send::Dynamic
                    )
                )

                when 'create_claimable_balance' then tuple(
                    op.^body.create_claimable_balance::JSON,
                    coalesce(
                        op_result.op_inner.create_claimable_balance::Dynamic,
                        op_result.^op_inner.create_claimable_balance::Dynamic
                    )
                )

                when 'claim_claimable_balance' then tuple(
                    op.^body.claim_claimable_balance::JSON,
                    coalesce(
                        op_result.op_inner.claim_claimable_balance::Dynamic,
                        op_result.^op_inner.claim_claimable_balance::Dynamic
                    )
                )

                when 'begin_sponsoring_future_reserves' then tuple(
                    op.^body.begin_sponsoring_future_reserves::JSON,
                    coalesce(
                        op_result.op_inner.begin_sponsoring_future_reserves::Dynamic,
                        op_result.^op_inner.begin_sponsoring_future_reserves::Dynamic
                    )
                )

                when 'end_sponsoring_future_reserves' then tuple(
                    op.^body.end_sponsoring_future_reserves::JSON,
                    coalesce(
                        op_result.op_inner.end_sponsoring_future_reserves::Dynamic,
                        op_result.^op_inner.end_sponsoring_future_reserves::Dynamic
                    )
                )

                when 'revoke_sponsorship' then tuple(
                    op.^body.revoke_sponsorship::JSON,
                    coalesce(
                        op_result.op_inner.revoke_sponsorship::Dynamic,
                        op_result.^op_inner.revoke_sponsorship::Dynamic
                    )
                )

                when 'clawback' then tuple(
                    op.^body.clawback::JSON,
                    coalesce(
                        op_result.op_inner.clawback::Dynamic,
                        op_result.^op_inner.clawback::Dynamic
                    )
                )

                when 'clawback_claimable_balance' then tuple(
                    op.^body.clawback_claimable_balance::JSON,
                    coalesce(
                        op_result.op_inner.clawback_claimable_balance::Dynamic,
                        op_result.^op_inner.clawback_claimable_balance::Dynamic
                    )
                )

                when 'set_trust_line_flags' then tuple(
                    op.^body.set_trust_line_flags::JSON,
                    coalesce(
                        op_result.op_inner.set_trust_line_flags::Dynamic,
                        op_result.^op_inner.set_trust_line_flags::Dynamic
                    )
                )

                when 'liquidity_pool_deposit' then tuple(
                    op.^body.liquidity_pool_deposit::JSON,
                    coalesce(
                        op_result.op_inner.liquidity_pool_deposit::Dynamic,
                        op_result.^op_inner.liquidity_pool_deposit::Dynamic
                    )
                )

                when 'liquidity_pool_withdraw' then tuple(
                    op.^body.liquidity_pool_withdraw::JSON,
                    coalesce(
                        op_result.op_inner.liquidity_pool_withdraw::Dynamic,
                        op_result.^op_inner.liquidity_pool_withdraw::Dynamic
                    )
                )

                when 'invoke_host_function' then tuple(
                    op.^body.invoke_host_function::JSON,
                    coalesce(
                        op_result.op_inner.invoke_host_function::Dynamic,
                        op_result.^op_inner.invoke_host_function::Dynamic
                    )
                )

                when 'extend_footprint_ttl' then tuple(
                    op.^body.extend_footprint_ttl::JSON,
                    coalesce(
                        op_result.op_inner.extend_footprint_ttl::Dynamic,
                        op_result.^op_inner.extend_footprint_ttl::Dynamic
                    )
                )

                when 'restore_footprint' then tuple(
                    op.^body.restore_footprint::JSON,
                    coalesce(
                        op_result.op_inner.restore_footprint::Dynamic,
                        op_result.^op_inner.restore_footprint::Dynamic
                    )
                )

                else tuple(null, null)
            end)

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
    )

select * from ops

settings 
    output_format_arrow_string_as_string=0,
    enable_unaligned_array_join = 1