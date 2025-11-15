use crate::config::DatabaseConfig;
#[cfg(feature = "db-postgres")]
use crate::error::Context;
use crate::error::Result;

#[cfg(feature = "db-postgres")]
use sqlx::postgres::PgPoolOptions;
#[cfg(feature = "db-postgres")]
use sqlx::{Pool, Postgres};
#[cfg(feature = "db-postgres")]
use std::time::Duration;

#[cfg(feature = "db-postgres")]
#[derive(Clone)]
pub struct Database {
    pool: Pool<Postgres>,
}

#[cfg(not(feature = "db-postgres"))]
#[derive(Clone)]
pub struct Database;

#[cfg(feature = "db-postgres")]
impl Database {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self> {
        let max_conn = config.max_connections.unwrap_or(5);
        let timeout = config.acquire_timeout_secs.unwrap_or(5);

        let pool = PgPoolOptions::new()
            .max_connections(max_conn)
            .acquire_timeout(Duration::from_secs(timeout))
            .connect(&config.url)
            .await
            .with_context(|| format!("failed to connect to {}", config.url))?;

        Ok(Self { pool })
    }

    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("database ping failed")?;

        Ok(())
    }
}

#[cfg(not(feature = "db-postgres"))]
impl Database {
    pub async fn connect(_config: &DatabaseConfig) -> Result<Self> {
        Err(crate::err!(
            "database support requires the `db-postgres` feature at compile time"
        ))
    }

    pub async fn ping(&self) -> Result<()> {
        Ok(())
    }
}
