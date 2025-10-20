use url::Url;
use querystring::querify;

pub fn build_opts(u: &Url) -> Vec<(String, String)> {
    let mut opts: Vec<(String,String)> = Vec::new();

    for (k, v) in std::env::vars() {
        if k.starts_with("AWS_") {
            opts.push((k.to_lowercase(), v));
        }
    }

    for (k, v) in querify(u.fragment().unwrap_or_default()) {
        opts.push((k.to_string(), v.to_string()));
    }

    opts
}