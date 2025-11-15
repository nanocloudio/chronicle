//! Helper macros enforcing consistent Chronical log fields.
//!
//! These macros keep `connector` (and optionally `chronicle`) fields present on every log
//! emitted from transport/dispatcher layers so downstream parsing can rely on them.

/// Log an event for a connector/chronicle pair plus any extra fields.
#[macro_export]
macro_rules! chronicle_event {
    ($level:ident, $target:expr, $event:expr, connector = $connector:expr, chronicle = $chronicle:expr $(, $field:ident = $value:expr )* $(,)?) => {
        tracing::$level!(
            target = $target,
            event = $event,
            connector = $connector,
            chronicle = $chronicle,
            $($field = %$value,)*
        )
    };
    ($level:ident, $target:expr, $event:expr, connector = $connector:expr $(, $field:ident = $value:expr )* $(,)?) => {
        tracing::$level!(
            target = $target,
            event = $event,
            connector = $connector,
            $($field = %$value,)*
        )
    };
}
