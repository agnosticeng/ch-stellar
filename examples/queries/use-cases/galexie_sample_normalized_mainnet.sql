insert into function file('./tmp/galexie_sample_normalized_mainnet.bin', 'Native')
select
    ledger
from executable(
    'ch-stellar table-function galexie --normalized',
    ArrowStream,
    'ledger String',
    (
        select
            *
        from values(
            'url String, start UInt32, end UInt32',
            (
                'https://galexie.lightsail.network/v1/',
                3,
                10
            ),
            (
            '   https://galexie.lightsail.network/v1/',
                250000,
                250020
            ),
            (
                'https://galexie.lightsail.network/v1/',
                557450,
                557460
            ),
            (
                'https://galexie.lightsail.network/v1/',
                4000000,
                4000005
            ),
            (
                'https://galexie.lightsail.network/v1/',
                12000000,
                12000024
            ),
            (
                'https://galexie.lightsail.network/v1/',
                23000000,
                23000016
            ),
            (
                'https://galexie.lightsail.network/v1/',
                33028304,
                33028314
            ),
            (
                'https://galexie.lightsail.network/v1/',
                34000000,
                34000010
            ),
            (
                'https://galexie.lightsail.network/v1/',
                41447181,
                41447183
            ),
            (
                'https://galexie.lightsail.network/v1/',
                54000000,
                54000010
            ),
            (
                'https://galexie.lightsail.network/v1/',
                50461314,
                50461324
            ),
            (
                'https://galexie.lightsail.network/v1/',
                50461745,
                50461755
            ),
            (
                'https://galexie.lightsail.network/v1/',
                50560940,
                50560950
            ),
            (
                'https://galexie.lightsail.network/v1/',
                58000000,
                58000010
            )
        )
    ),
    settings
        stderr_reaction='log',
        check_exit_code=true,
        command_read_timeout=60000
)

settings output_format_arrow_string_as_string=0
