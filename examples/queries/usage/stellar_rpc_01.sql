select 
    stellar_rpc(
        'https://rpc.lightsail.network/',
        'getHealth', 
        'null'
    )
settings output_format_arrow_string_as_string=0
