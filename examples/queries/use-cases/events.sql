with
    galexie as (
        select * from file('./tmp/galexie_sample_mainnet.bin', 'Native')
    ),

    ledgers as (
        select
            firstNonDefault(
                JSONExtractString(ledger, 'v0'),
                JSONExtractString(ledger, 'v1'),
                JSONExtractString(ledger, 'v2')
            ) as _lcm_raw,

            JSONExtract(_lcm_raw, ' Tuple(
                ledger_header Tuple(
                    hash String,
                    header Tuple(
                        ledger_seq Int32,
                        scp_value Tuple(
                            close_time DateTime64(6, \'UTC\')
                        )
                    )
                ),
                tx_set Tuple(
                    txs Array(String),
                    v1 Tuple(
                        phases Array(Tuple(
                            v0 Array(Tuple(
                                txset_comp_txs_maybe_discounted_fee Tuple(
                                    txs Array(String)
                                )
                            )),
                            v1 Tuple(
                                execution_stages Array(Array(Array(String)))
                            )
                        ))
                    )
                ),
                tx_processing Array(String)
            )') as _lcm,

            _lcm.ledger_header.header.ledger_seq as ledger_sequence,
            _lcm.ledger_header.header.scp_value.close_time as ledger_close_time,
            _lcm.ledger_header.hash as ledger_hash,

            arrayConcat(
                _lcm.tx_set.txs,
                arrayFlatten(_lcm.tx_set.v1.phases.v0.txset_comp_txs_maybe_discounted_fee.txs),
                arrayFlatten(_lcm.tx_set.v1.phases.v1.execution_stages)
            ) as _tx_envelopes_raw,

            _lcm.tx_processing as _tx_result_metas_raw

        from galexie
    ),

    tx_envelopes as (
        select
            columns('^[^_]'),

            JSONExtract(_tx_envelope_raw, 'Tuple(
                tx Tuple(
                    tx String
                ),
                tx_fee_bump Tuple(
                    tx Tuple(
                        inner_tx Tuple(
                            tx Tuple(
                                tx String
                            )
                        )
                    )
                )
            )') as _tx_envelope,

            JSONExtract(
                firstNonDefault(
                    _tx_envelope.tx_fee_bump.tx.inner_tx.tx.tx,
                    _tx_envelope.tx.tx
                ),
                'Tuple(
                    operations Array(String)
                )'
            ) _tx_envelope_inner,

            stellar_hash_transaction(
                _tx_envelope_raw,
                'Public Global Stellar Network ; September 2015'
            ) as transaction_hash,

            _tx_envelope_inner.operations as _ops_raw
        from ledgers
        array join
            _tx_envelopes_raw as _tx_envelope_raw
    ),

    tx_result_metas as (
        select
            JSONExtract(_tx_result_meta_raw, 'Tuple(
                result Tuple(
                    transaction_hash String,
                    result Tuple(
                        result String,
                    )
                ),
                tx_apply_processing String
            )') as _tx_result_meta,

            JSONExtractKeysAndValues(_tx_result_meta.result.result.result, 'String')[1] as _result,

            firstNonDefault(
                JSONExtractArrayRaw(_result.2),
                JSONExtractArrayRaw(_result.2, 'result', 'result', 'tx_success'),
                JSONExtractArrayRaw(_result.2, 'result', 'result', 'tx_failed')
            ) as _ops_results_raw,

            firstNonDefault(
                JSONExtractArrayRaw(_tx_result_meta.tx_apply_processing, 'operations'),
                JSONExtractArrayRaw(_tx_result_meta.tx_apply_processing, 'v1', 'operations'),
                JSONExtractArrayRaw(_tx_result_meta.tx_apply_processing, 'v2', 'operations'),
                JSONExtractArrayRaw(_tx_result_meta.tx_apply_processing, 'v3', 'operations'),
                JSONExtractArrayRaw(_tx_result_meta.tx_apply_processing, 'v4', 'operations')
            ) as _ops_metas_raw,

            firstNonDefault(
                JSONExtractArrayRaw(_tx_result_meta.tx_apply_processing, 'v4', 'events')
            ) as _events,

            _tx_order,

            if(
                JSONType(_tx_result_meta.result.result.result) = 'Object',
                _result.1,
                _tx_result_meta.result.result.result
            ) as transaction_result_code,

            (transaction_result_code in ('tx_fee_bump_inner_success', 'tx_success')) as transaction_successful
        from ledgers
        array join
            _tx_result_metas_raw as _tx_result_meta_raw,
            arrayEnumerate(_tx_result_metas_raw) as _tx_order
    ),

    txs as (
        select
            columns('^[^_]'),
            stellar_id(ledger_sequence::Int32, _tx_order::Int32, 0::Int32) as transaction_id,
            _ops_raw,
            _ops_results_raw,
            _ops_metas_raw,
            _tx_order,
            _events,

            -- fail if you can't match tx_evelope with tx_meta (eg: hash with wrong network passphrase)
            throwIf(transaction_result_code = '', 'tx_match_failed') as _
        from tx_envelopes
        left join tx_result_metas
        on tx_envelopes.transaction_hash = tx_result_metas._tx_result_meta.result.transaction_hash
    ),

    ops as (
        select
            columns('^[^_]'),
            stellar_id(ledger_sequence::Int32, _tx_order::Int32, _op_order::Int32) as id,

            JSONExtractKeysAndValues(_op_result_raw, 'String')[1] as _op_result,
            JSONExtractKeysAndValues(_op_result.2, 'String')[1] as _op_result_tr,
            JSONExtractKeysAndValues(_op_result_tr.2, 'String')[1] as _op_result_tr_inner,
            JSONExtractArrayRaw(_op_meta_raw, 'events') as _events,

            if(
                JSONType(_op_result_raw) = 'Object',
                _op_result.1,
                JSONExtractString(_op_result_raw)
            ) as result_code,

            if (
                JSONType(_op_result_tr.2) = 'Object',
                _op_result_tr_inner.1,
                _op_result_tr.2
            ) as inner_result_code
        from txs
        array join
            _ops_raw as _op_raw,
            _ops_results_raw as _op_result_raw,
            _ops_metas_raw as _op_meta_raw,
            arrayEnumerate(_ops_raw) as _op_order
    ),

    txs_events as (
        select
            JSONExtract(_event, 'Tuple(
                stage String,
                event String
            )') as _tx_event,

            ledger_sequence,
            ledger_close_time,
            ledger_hash,
            transaction_hash,
            transaction_id,
            transaction_result_code,
            transaction_successful,
            _tx_event.stage as transaction_event_stage,
            null as operation_id,
            null as operation_result_code,
            null as operation_inner_result_code,
            _tx_event.event as _contract_event_raw
        from txs
        array join _events as _event
    ),

    ops_events as (
        select
            ledger_sequence,
            ledger_close_time,
            ledger_hash,
            transaction_hash,
            transaction_id,
            transaction_result_code,
            transaction_successful,
            null as transaction_event_stage,
            id as operation_id,
            result_code as operation_result_code,
            inner_result_code as operation_inner_result_code,
            _contract_event_raw
        from ops
        array join _events as _contract_event_raw
    ),

    events as (
        select
            columns('^[^_]'),

            JSONExtract(_contract_event_raw, 'Tuple(
                contract_id String,
                type_ String,
                body String
            )') as _contract_event,

            JSONExtract(
                firstNonDefault(
                    JSONExtractRaw(_contract_event.body, 'v0')
                ),
                'Tuple(
                    topics Array(String),
                    data String
                )'
            ) as _contract_event_body,

            _contract_event.contract_id as contract_id,
            _contract_event.type_ as type,
            _contract_event_body.topics as topics,
            _contract_event_body.data as data
        from (
            select columns('^[^_]'), _contract_event_raw from ops_events
            union all
            select columns('^[^_]'), _contract_event_raw  from txs_events
        )
    )

select
    ledger_sequence,
    ledger_close_time,
    ledger_hash,
    transaction_hash,
    transaction_id,
    transaction_result_code,
    transaction_successful,
    transaction_event_stage,
    operation_id,
    operation_result_code,
    operation_inner_result_code,
    contract_id,
    type,
    topics,
    data
from events
limit 100

format Vertical
settings
    output_format_arrow_string_as_string=0,
    enable_unaligned_array_join = 1
