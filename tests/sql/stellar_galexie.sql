select
    ledger.v0.ledger_header.header.ledger_seq as sequence,
    ledger.v0.ledger_header.header.scp_value.close_time as close_time
from executable(
    'ch-stellar table-function galexie',
    ArrowStream,
    'ledger JSON',
    (
        select * from values(
            'url String, start UInt32, end UInt32',
            ('https://galexie.lightsail.network/v1/', 50000000, 50000003)
        )
    ),
    settings
        stderr_reaction='log',
        check_exit_code=true,
        command_read_timeout=60000
)
settings output_format_arrow_string_as_string=0
