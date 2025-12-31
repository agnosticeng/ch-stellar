with 
    trustlines as (
        select 
            JSONExtract(ledger_entry_data, 'Tuple(
                account_id String,
                asset String,
                balance Int64,
                limit Int64,
                flags UInt32,
                ext Tuple(
                    v1 Tuple(
                        liabilities Tuple(
                            buying Int64,
                            selling Int64
                        ),
                        ext Tuple(
                            v2 Tuple(
                                liquidity_pool_use_count Int32
                            )
                        )
                    )
                )
            )') _trustline_entry,

            JSONExtractKeysAndValues(_trustline_entry.asset, 'String')[1] as _asset_type_and_data,

            ledger_sequence,
            ledger_close_time,
            ledger_hash,
            transaction_hash,
            transaction_id,
            transaction_result_code,
            transaction_successful,
            operation_id,
            operation_result_code,
            operation_inner_result_code,
            changes.source as change_source,
            changes.type as change_type,
            changes.last_modified_ledger_sequence as change_last_modified_ledger_sequence,

            if(
                JSONType(_trustline_entry.asset) = 'Object',
                _asset_type_and_data.1,
                _trustline_entry.asset
            ) as asset_type,

            _trustline_entry.account_id as account_id,
            JSONExtractString(_asset_type_and_data.2, 'asset_code') as asset_code,
            JSONExtractString(_asset_type_and_data.2, 'issuer') as asset_issuer,
            stellar_asset_id(asset_code, asset_issuer, asset_type) as asset_id,
            _trustline_entry.balance as balance,
            _trustline_entry.limit as limit,
            _trustline_entry.flags as flags,
            _trustline_entry.ext.v1.liabilities.buying as buying_liabilities,
            _trustline_entry.ext.v1.liabilities.selling as selling_liabilities,
            _trustline_entry.ext.v1.ext.v2.liquidity_pool_use_count as liquidity_pool_use_count
        from iceberg('https://stellar-iceberg-testnet.agnostic.tech/changes', NOSIGN, settings iceberg_use_version_hint=1) as changes
        where ledger_entry_type = 'trustline'
    )

select 
    columns('^[^_]')
from trustlines
limit 10

format Vertical
settings 
    output_format_arrow_string_as_string=0,
    enable_unaligned_array_join = 1,
    enable_named_columns_in_function_tuple=1