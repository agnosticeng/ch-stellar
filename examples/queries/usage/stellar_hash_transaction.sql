with 
    ledgers as (
        select 
            ledger_close_meta
        from executable(
            'ch-stellar table-function galexie',
            ArrowStream,
            'ledger_close_meta JSON',
            (
                select 
                    *
                from values(
                    'url String, start UInt32, end UInt32',
                    (
                        'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                        34000000,
                        34000002
                    ),
                    (
                        'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                        54000000,
                        54000002
                    )
                )
            ),
            settings 
                stderr_reaction='log', 
                check_exit_code=true,
                command_read_timeout=60000
        )
    ),

    txs as (
        select 
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
    txs.tx::String as str,
    stellar_hash_transaction(txs.tx::String, 'Public Global Stellar Network ; September 2015') as tx_hash,
    tx_meta.result.transaction_hash::String as tx_meta_hash
from txs
inner join txs_meta
on tx_hash = tx_meta_hash

settings output_format_arrow_string_as_string=0
