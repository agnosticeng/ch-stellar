insert into function file('./tmp/galexie_sample_testnet.bin', 'Native')
select 
    ledger_close_meta
from executable(
    'ch-stellar table-function stellar-galexie',
    ArrowStream,
    'ledger_close_meta String',
    (
        select 
            *
        from values(
            'url String, start UInt32, end UInt32',
            (
                'https://pub-e502ccd47ced4d73aa9b68caffde7fb1.r2.dev#ledgers_per_file=64&files_per_partition=64000&extension=xdr.zst',
                1726,
                1726
            ),
            (
                'https://pub-e502ccd47ced4d73aa9b68caffde7fb1.r2.dev#ledgers_per_file=64&files_per_partition=64000&extension=xdr.zst',
                173440,
                173759
            )
        )
    ),
    settings 
        stderr_reaction='log', 
        check_exit_code=true,
        command_read_timeout=60000
)

settings output_format_arrow_string_as_string=0
