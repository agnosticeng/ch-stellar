insert into function file('./tmp/galexie_sample.bin', 'Native')
select 
    ledger_close_meta
from executable(
    'ch-stellar table-function stellar-galexie',
    ArrowStream,
    'ledger_close_meta JSON',
    (
        select 
            *
        from values(
            'url String, start UInt32, end UInt32',
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                33028304,
                33028304
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                34000000,
                34000002
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                54000000,
                54000002
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                50461314,
                50461314
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                50461745,
                50461745
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                50560940,
                50560940
            )
        )
    ),
    settings 
        stderr_reaction='log', 
        check_exit_code=true,
        command_read_timeout=60000
)

settings output_format_arrow_string_as_string=0
