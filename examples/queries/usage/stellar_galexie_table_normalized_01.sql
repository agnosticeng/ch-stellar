select
    ledger.ledger_header.header.ledger_seq
from executable(
    'ch-stellar table-function galexie-normalized',
    ArrowStream,
    'ledger JSON',
    (
        select
            'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst' as url,
            50000000 as start,
            50000005 as end,
            'Public Global Stellar Network ; September 2015' as passphrase
    ),
    settings stderr_reaction='log', check_exit_code=true
)
settings output_format_arrow_string_as_string=0
