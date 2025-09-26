use std::str::FromStr;
use querystring::QueryParams;

pub trait QueryParamsExt {
    fn get_or_default<T: FromStr>(&self, key: &str, default_value: T) -> T;
}

impl QueryParamsExt for QueryParams<'_> {
    fn get_or_default<T: FromStr>(&self, key: &str, default_value: T) -> T {
        match self.iter().find(|(k, _)| *k == key).map(|(_,v)| *v) {
            Some(v) => std::str::FromStr::from_str(v).unwrap_or(default_value),
            None => default_value
        }        
    }
}

impl QueryParamsExt for Vec<(String, String)> {
    fn get_or_default<T: FromStr>(&self, key: &str, default_value: T) -> T {
        match self.iter().find(|(k, _)| k == key).map(|(_,v)| v) {
            Some(v) => std::str::FromStr::from_str(v).unwrap_or(default_value),
            None => default_value
        }        
    }
}