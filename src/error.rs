#![forbid(unsafe_code)]

use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type ChronicleError = Error;

use std::any::Any;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("acquire error: {0}")]
    Acquire(#[from] tokio::sync::AcquireError),
    #[error("HTTP request error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("invalid URI: {0}")]
    InvalidUri(#[from] http::uri::InvalidUri),
    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),
    #[error("SQL error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("integration config error: {0}")]
    Config(#[from] crate::config::integration::IntegrationConfigError),
    #[error("connector registry error: {0}")]
    ConnectorRegistry(#[from] crate::integration::registry::ConnectorRegistryError),
    #[error("connector factory error: {0}")]
    ConnectorFactory(#[from] crate::integration::factory::ConnectorFactoryError),
    #[error("chronicle engine error: {0}")]
    ChronicleEngine(#[from] crate::chronicle::engine::ChronicleEngineError),
    #[cfg(feature = "kafka")]
    #[error("kafka trigger error: {0}")]
    KafkaTrigger(#[from] crate::transport::kafka::KafkaTriggerError),
    #[cfg(feature = "kafka")]
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[cfg(feature = "rabbitmq")]
    #[error("rabbitmq trigger error: {0}")]
    RabbitmqTrigger(#[from] crate::transport::rabbitmq::RabbitmqTriggerError),
    #[cfg(feature = "mqtt")]
    #[error("mqtt trigger error: {0}")]
    MqttTrigger(#[from] crate::transport::mqtt::MqttTriggerError),
    #[cfg(feature = "db-redis")]
    #[error("redis trigger error: {0}")]
    RedisTrigger(#[from] crate::transport::redis::RedisTriggerError),
    #[cfg(feature = "db-mongodb")]
    #[error("mongodb trigger error: {0}")]
    MongodbTrigger(#[from] crate::transport::mongodb::MongodbTriggerError),
    #[cfg(feature = "db-mongodb")]
    #[error("mongodb error: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[cfg(feature = "db-redis")]
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[cfg(feature = "grpc")]
    #[error("gRPC status: {0}")]
    GrpcStatus(#[from] tonic::Status),
    #[cfg(feature = "grpc")]
    #[error("gRPC transport error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),
    #[cfg(feature = "smtp")]
    #[error("SMTP error: {0}")]
    Smtp(#[from] lettre::error::Error),
    #[cfg(feature = "smtp")]
    #[error("SMTP transport error: {0}")]
    SmtpTransport(#[from] lettre::transport::smtp::Error),
    #[error("duration parse error: {0}")]
    Duration(#[from] humantime::DurationError),
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("address parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("{context}")]
    Context {
        context: String,
        #[source]
        source: Box<Error>,
    },
}

impl Error {
    pub fn new<E>(error: E) -> Self
    where
        Error: From<E>,
    {
        error.into()
    }

    pub fn msg<M>(message: M) -> Self
    where
        M: Into<String>,
    {
        Self::Message(message.into())
    }

    pub fn with_context<M>(context: M, source: Error) -> Self
    where
        M: Into<String>,
    {
        Self::Context {
            context: context.into(),
            source: Box::new(source),
        }
    }

    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        match self {
            Error::Sqlx(err) => (err as &dyn Any).downcast_ref(),
            Error::Context { source, .. } => source.downcast_ref(),
            _ => None,
        }
    }
}

pub trait Context<T> {
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Into<String>;

    fn with_context<C, F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Into<String>;
}

impl<T, E> Context<T> for std::result::Result<T, E>
where
    Error: From<E>,
{
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Into<String>,
    {
        self.map_err(|err| Error::with_context(context.into(), err.into()))
    }

    fn with_context<C, F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Into<String>,
    {
        self.map_err(|err| Error::with_context(f().into(), err.into()))
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Message(value)
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Error::Message(value.to_string())
    }
}

#[macro_export]
macro_rules! err {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {{
        $crate::error::Error::msg(format!($fmt $(, $arg)*))
    }};
    ($err:expr) => {{
        $crate::error::Error::new($err)
    }};
}

#[macro_export]
macro_rules! bail_err {
    ($($arg:tt)*) => {{
        return Err($crate::err!($($arg)*));
    }};
}

#[macro_export]
macro_rules! ensure_err {
    ($cond:expr $(,)?) => {
        if !$cond {
            return Err($crate::err!(concat!("condition failed: ", stringify!($cond))));
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            $crate::bail_err!($($arg)+);
        }
    };
}
