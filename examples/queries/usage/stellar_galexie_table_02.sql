select
    ledger_close_meta.v0.ledger_header.header.ledger_seq as sequence,
    ledger_close_meta.v0.ledger_header.header.scp_value.close_time as close_time
from executable(
    'ch-stellar table-function stellar-galexie',
    ArrowStream,
    'ledger_close_meta JSON',
    (
        select * from values(
            'url String, start UInt32, end UInt32',
            ('https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst', 50000000, 50000099)
        )
    ),
    settings 
        stderr_reaction='log', 
        check_exit_code=true,
        command_read_timeout=60000
)
settings output_format_arrow_string_as_string=0