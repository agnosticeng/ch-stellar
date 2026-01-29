insert into function file('./tmp/galexie_sample_normalized_mainnet.bin', 'Native')
select
    ledger
from executable(
    'ch-stellar table-function galexie-normalized',
    ArrowStream,
    'ledger String',
    (
        select
            *
        from values(
            'url String, start UInt32, end UInt32, passphrase String',
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                33028304,
                33028314,
                'Public Global Stellar Network ; September 2015'
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                34000000,
                34000010,
                'Public Global Stellar Network ; September 2015'
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                54000000,
                54000010,
                'Public Global Stellar Network ; September 2015'
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                50461314,
                50461324,
                'Public Global Stellar Network ; September 2015'
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                50461745,
                50461755,
                'Public Global Stellar Network ; September 2015'
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                50560940,
                50560950,
                'Public Global Stellar Network ; September 2015'
            ),
            (
                'https://galexie.lightsail.network/v1/#ledgers_per_file=8&files_per_partition=64000&extension=xdr.zst',
                58000000,
                58000010,
                'Public Global Stellar Network ; September 2015'
            )
        )
    ),
    settings
        stderr_reaction='log',
        check_exit_code=true,
        command_read_timeout=60000
)

settings output_format_arrow_string_as_string=0
