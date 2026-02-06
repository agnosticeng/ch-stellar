select
    ledger.v0.ledger_header.header.ledger_seq
from executable(
    'ch-stellar table-function galexie',
    ArrowStream,
    'ledger JSON',
    (
        select
            'https://galexie.lightsail.network/v1/' as url,
            50000000 as start,
            50000005 as end
    ),
    settings stderr_reaction='log', check_exit_code=true
)
settings output_format_arrow_string_as_string=0
