use super::result::Result;
use crate::stellar::Galexie;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Default)]
pub struct GalexieCache {
    inner: Arc<RwLock<HashMap<String, Arc<Galexie>>>>,
}

impl GalexieCache {
    pub async fn get_or_create(&self, url: &str) -> Result<Arc<Galexie>> {
        if let Some(g) = self.inner.read().await.get(url).cloned() {
            return Ok(g);
        }
        let g = Arc::new(Galexie::new(url).await?);
        self.inner
            .write()
            .await
            .insert(url.to_string(), Arc::clone(&g));
        Ok(g)
    }
}
