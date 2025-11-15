use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use jaq_interpret::{
    Ctx as JaqCtx, FilterT, ParseCtx as JaqParseCtx, RcIter as JaqRcIter, Val as JaqVal,
};
use jaq_parse;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

/// Flat representation of context data keyed by jq-style slot selectors.
pub type FlatContext = BTreeMap<String, JsonValue>;
const OBSERVABILITY_SLOT: usize = 0;
const TRACE_POINTERS: [&str; 4] = [
    "/header/trace_id",
    "/headers/trace_id",
    "/body/trace/trace_id",
    "/body.trace.trace_id",
];
const RECORD_POINTERS: [&str; 3] = [
    "/body/record/id",
    "/body/summary/record_id",
    "/body.record_id",
];
#[derive(Debug, Default, Clone)]
pub struct ExecutionContext {
    slots: BTreeMap<usize, JsonValue>,
    flat: FlatContext,
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self::default()
    }

    fn evaluate_with_fallback(
        &self,
        original: &str,
        canonical: &str,
    ) -> Result<JsonValue, ContextError> {
        match self.evaluate_jq(original, canonical) {
            Ok(value) => {
                if value.is_null() {
                    if let Some(detail) = self.try_simple_resolution(original, canonical) {
                        return Err(detail);
                    }
                }
                Ok(value)
            }
            Err(err) => {
                if matches!(err, ContextError::JaqRuntime { .. }) {
                    if let Some(detail) = self.try_simple_resolution(original, canonical) {
                        return Err(detail);
                    }
                }
                Err(err)
            }
        }
    }

    pub fn insert_slot(&mut self, slot: usize, value: JsonValue) {
        self.slots.insert(slot, value.clone());

        let prefix = format!(".[{slot}]");
        self.flat.retain(|key, _| !key.starts_with(&prefix));

        let flattened = flatten_json(slot, &value);
        for (key, val) in flattened {
            self.flat.insert(key, val);
        }
    }

    pub fn slot_value(&self, slot: usize) -> Option<&JsonValue> {
        self.slots.get(&slot)
    }

    pub fn resolve_expression(&self, expression: &str) -> Result<JsonValue, ContextError> {
        self.evaluate_expression(expression)
    }

    pub fn maybe_resolve(&self, expression: &str) -> Result<Option<JsonValue>, ContextError> {
        if expression.trim().starts_with("$.") {
            self.evaluate_expression(expression).map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn maybe_resolve_jq(&self, expression: &str) -> Result<Option<JsonValue>, ContextError> {
        let trimmed = expression.trim();

        if let Some(literal) = parse_literal_string(trimmed) {
            return Ok(Some(JsonValue::String(literal)));
        }

        if !trimmed.starts_with('.') || trimmed.starts_with("$.") {
            return Ok(None);
        }

        self.evaluate_expression(expression).map(Some)
    }

    fn evaluate_expression(&self, expression: &str) -> Result<JsonValue, ContextError> {
        let trimmed = expression.trim();

        if trimmed.is_empty() {
            return Err(ContextError::InvalidExpression {
                expression: expression.to_string(),
            });
        }

        if let Some(literal) = parse_literal_string(trimmed) {
            return Ok(JsonValue::String(literal));
        }

        if trimmed.starts_with("$.") {
            let canonical = convert_dollar_expression(expression)?;
            return self.evaluate_with_fallback(expression, &canonical);
        }

        if trimmed.starts_with('.') {
            return self.evaluate_with_fallback(expression, trimmed);
        }

        Err(ContextError::InvalidExpression {
            expression: expression.to_string(),
        })
    }

    fn evaluate_jq(&self, original: &str, canonical: &str) -> Result<JsonValue, ContextError> {
        let (parsed, parse_errors) = jaq_parse::parse(canonical, jaq_parse::main());

        if !parse_errors.is_empty() {
            let reason = parse_errors
                .into_iter()
                .map(|err| err.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ContextError::InvalidJqExpression {
                expression: original.to_string(),
                reason,
            });
        }

        let main = parsed.ok_or_else(|| ContextError::InvalidJqExpression {
            expression: original.to_string(),
            reason: "expression did not produce a filter".to_string(),
        })?;

        let mut ctx = JaqParseCtx::new(Vec::new());
        let filter = ctx.compile(main);
        if !ctx.errs.is_empty() {
            return Err(ContextError::InvalidJqExpression {
                expression: original.to_string(),
                reason: "failed to compile expression".to_string(),
            });
        }

        let inputs = JaqRcIter::new(std::iter::empty::<Result<JaqVal, String>>());
        let input = JaqVal::from(JsonValue::Array(self.snapshot_slots()));
        let mut results = filter.run((JaqCtx::new([], &inputs), input));

        let first = results
            .next()
            .ok_or_else(|| ContextError::JaqNoResults {
                expression: original.to_string(),
            })?
            .map_err(|err| ContextError::JaqRuntime {
                expression: original.to_string(),
                error: err.to_string(),
            })?;

        if results.next().is_some() {
            return Err(ContextError::JaqMultipleResults {
                expression: original.to_string(),
            });
        }

        Ok(JsonValue::from(first))
    }

    fn try_simple_resolution(&self, original: &str, canonical: &str) -> Option<ContextError> {
        let segments = match parse_jq_segments(canonical) {
            Ok(segments) => segments,
            Err(_) => return None,
        };

        match self.resolve_simple_selector(original, segments) {
            Ok(_) => None,
            Err(ContextError::InvalidJqExpression { .. }) => None,
            Err(err) => Some(err),
        }
    }

    fn resolve_simple_selector(
        &self,
        original: &str,
        segments: Vec<Segment>,
    ) -> Result<JsonValue, ContextError> {
        if segments.is_empty() {
            return Ok(JsonValue::Array(self.snapshot_slots()));
        }

        let mut iter = segments.into_iter();
        let slot_index = match iter.next().expect("segments not empty") {
            Segment::Index(idx) => idx,
            _ => {
                return Err(ContextError::InvalidJqExpression {
                    expression: original.to_string(),
                    reason: "expression must start with a slot index".to_string(),
                })
            }
        };

        let mut current = self
            .slots
            .get(&slot_index)
            .ok_or_else(|| ContextError::MissingSlot {
                expression: original.to_string(),
                slot: slot_index,
            })?;

        let mut path = String::new();

        for segment in iter {
            match &segment {
                Segment::Field(field) => {
                    let next_path = if path.is_empty() {
                        field.clone()
                    } else {
                        format!("{path}.{field}")
                    };

                    if !current.is_object() {
                        return Err(ContextError::TypeMismatch {
                            expression: original.to_string(),
                            slot: slot_index,
                            path: next_path.clone(),
                            expected: "object",
                            actual: value_type_name(current),
                        });
                    }

                    current = current
                        .get(field)
                        .ok_or_else(|| ContextError::MissingPath {
                            expression: original.to_string(),
                            slot: slot_index,
                            path: next_path.clone(),
                        })?;

                    path = next_path;
                }
                Segment::Index(index) => {
                    let next_path = if path.is_empty() {
                        format!("[{index}]")
                    } else {
                        format!("{path}[{index}]")
                    };

                    if !current.is_array() {
                        return Err(ContextError::TypeMismatch {
                            expression: original.to_string(),
                            slot: slot_index,
                            path: next_path.clone(),
                            expected: "array",
                            actual: value_type_name(current),
                        });
                    }

                    let array = current.as_array().expect("value verified as array");
                    current = array.get(*index).ok_or_else(|| ContextError::MissingPath {
                        expression: original.to_string(),
                        slot: slot_index,
                        path: next_path.clone(),
                    })?;

                    path = next_path;
                }
            }
        }

        Ok(current.clone())
    }

    fn snapshot_slots(&self) -> Vec<JsonValue> {
        if self.slots.is_empty() {
            return Vec::new();
        }

        let max_slot = self.slots.keys().copied().max().unwrap_or(0);
        let mut ordered = vec![JsonValue::Null; max_slot + 1];
        for (slot, value) in &self.slots {
            if *slot < ordered.len() {
                ordered[*slot] = value.clone();
            }
        }

        ordered
    }

    pub fn resolve_template(&self, value: &JsonValue) -> Result<JsonValue, ContextError> {
        match value {
            JsonValue::String(inner) => {
                if let Some(resolved) = self.maybe_resolve(inner)? {
                    Ok(resolved)
                } else if let Some(resolved) = self.maybe_resolve_jq(inner)? {
                    Ok(resolved)
                } else {
                    let expanded = self.resolve_inline_placeholders(inner)?;
                    Ok(JsonValue::String(expanded))
                }
            }
            JsonValue::Object(map) => {
                let mut resolved = JsonMap::with_capacity(map.len());
                for (key, val) in map {
                    resolved.insert(key.clone(), self.resolve_template(val)?);
                }
                Ok(JsonValue::Object(resolved))
            }
            JsonValue::Array(items) => Ok(JsonValue::Array(
                items
                    .iter()
                    .map(|item| self.resolve_template(item))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            _ => Ok(value.clone()),
        }
    }

    pub fn resolve_to_string(&self, value: &JsonValue) -> Result<String, ContextError> {
        let resolved = self.resolve_template(value)?;
        Ok(match resolved {
            JsonValue::Null => String::new(),
            JsonValue::Bool(flag) => flag.to_string(),
            JsonValue::Number(num) => num.to_string(),
            JsonValue::String(inner) => inner,
            other => other.to_string(),
        })
    }

    fn resolve_inline_placeholders(&self, text: &str) -> Result<String, ContextError> {
        fn is_expr_char(ch: char) -> bool {
            ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '[' | ']')
        }

        fn is_boundary_char(ch: char) -> bool {
            ch.is_whitespace() || matches!(ch, '(' | '{' | '[' | '<' | '>' | '"' | '\'' | ':' | ',')
        }

        fn value_to_string(value: JsonValue) -> String {
            match value {
                JsonValue::Null => String::new(),
                JsonValue::Bool(flag) => flag.to_string(),
                JsonValue::Number(num) => num.to_string(),
                JsonValue::String(inner) => inner,
                other => other.to_string(),
            }
        }

        let mut output = String::with_capacity(text.len());
        let chars: Vec<char> = text.chars().collect();
        let mut index = 0;

        while index < chars.len() {
            if chars[index] == '.' && (index == 0 || is_boundary_char(chars[index - 1])) {
                let start = index;
                let mut end = index + 1;
                while end < chars.len() && is_expr_char(chars[end]) {
                    end += 1;
                }

                let expr: String = chars[start..end].iter().collect();
                match self.maybe_resolve_jq(&expr)? {
                    Some(value) => {
                        output.push_str(&value_to_string(value));
                        index = end;
                        continue;
                    }
                    None => {
                        // fall through to emit the raw '.' character
                    }
                }
            }

            output.push(chars[index]);
            index += 1;
        }

        Ok(output)
    }

    /// Extracts canonical observability metadata from the initial slot.
    pub fn observability(&self) -> ExecutionObservability {
        ExecutionObservability {
            trace_id: self.first_matching_selector(&TRACE_POINTERS),
            record_id: self.first_matching_selector(&RECORD_POINTERS),
        }
    }

    fn payload_value_at(&self, selector: &str) -> Option<String> {
        self.slot_value(OBSERVABILITY_SLOT)
            .and_then(|value| value.pointer(selector).and_then(JsonValue::as_str))
            .map(|value| value.to_string())
    }

    fn first_matching_selector(&self, selectors: &[&str]) -> Option<String> {
        selectors
            .iter()
            .find_map(|selector| self.payload_value_at(selector))
    }

    fn canonical_expression(expression: &str) -> String {
        if let Some(stripped) = expression.strip_prefix("$.") {
            format!(".{stripped}")
        } else {
            expression.to_string()
        }
    }

    fn canonicalize_template(template: &JsonValue) -> JsonValue {
        match template {
            JsonValue::String(value) => JsonValue::String(Self::canonical_expression(value)),
            JsonValue::Object(map) => JsonValue::Object(
                map.iter()
                    .map(|(key, value)| (key.clone(), Self::canonicalize_template(value)))
                    .collect(),
            ),
            JsonValue::Array(items) => {
                JsonValue::Array(items.iter().map(Self::canonicalize_template).collect())
            }
            other => other.clone(),
        }
    }

    /// Resolve a template after normalizing canonical selectors (e.g., `$.foo` → `.foo`).
    pub fn resolve_template_canonical(
        &self,
        template: &JsonValue,
    ) -> Result<JsonValue, ContextError> {
        let canonical = Self::canonicalize_template(template);
        self.resolve_template(&canonical)
    }

    /// Resolve a template to string after applying canonical normalization.
    pub fn resolve_to_string_canonical(
        &self,
        template: &JsonValue,
    ) -> Result<String, ContextError> {
        let canonical = Self::canonicalize_template(template);
        self.resolve_to_string(&canonical)
    }

    pub fn into_vec(self) -> Vec<JsonValue> {
        if self.slots.is_empty() {
            return Vec::new();
        }

        let max_slot = self.slots.keys().copied().max().unwrap_or(0);
        let mut ordered = vec![JsonValue::Null; max_slot + 1];
        for (slot, value) in self.slots {
            if slot < ordered.len() {
                ordered[slot] = value;
            }
        }
        ordered
    }

    pub fn last_slot_index(&self) -> Option<usize> {
        self.slots.keys().next_back().copied()
    }

    pub fn last_slot_value(&self) -> Option<&JsonValue> {
        let slot = self.last_slot_index()?;
        self.slots.get(&slot)
    }
}

/// Represents the canonical observability metadata emitted with each trigger.
#[derive(Clone, Debug, Default)]
pub struct ExecutionObservability {
    pub trace_id: Option<String>,
    pub record_id: Option<String>,
}

/// Flatten a JSON value into the provided flat context using the given slot index.
pub fn flatten_json_into(slot: usize, value: &JsonValue, out: &mut FlatContext) {
    let base = format!(".[{slot}]");
    flatten_value(out, &base, value);
}

/// Convenience helper that flattens a JSON value and returns a new flat context.
pub fn flatten_json(slot: usize, value: &JsonValue) -> FlatContext {
    let mut flat = FlatContext::new();
    flatten_json_into(slot, value, &mut flat);
    flat
}

/// Flatten header key/value pairs into the provided flat context.
pub fn flatten_headers_into<'a>(
    slot: usize,
    headers: impl IntoIterator<Item = (&'a str, &'a str)>,
    out: &mut FlatContext,
) {
    let mut seen = HashMap::<String, usize>::new();
    for (name, value) in headers.into_iter() {
        let normalised = name.trim().to_ascii_lowercase();
        let entry = seen.entry(normalised.clone()).or_insert(0);
        let suffix = if *entry == 0 {
            String::new()
        } else {
            format!(".{}", entry)
        };
        *entry += 1;

        let headers_key = format!(".[{slot}].headers.{normalised}{suffix}");
        out.insert(headers_key, JsonValue::String(value.to_string()));

        let legacy_key = format!(".[{slot}].header.{normalised}{suffix}");
        out.insert(legacy_key, JsonValue::String(value.to_string()));
    }
}

/// Encode binary data as base64 and insert it into the flat context at the given slot/path.
pub fn insert_bytes(slot: usize, path: &str, bytes: &[u8], out: &mut FlatContext) {
    let encoded = BASE64_STANDARD.encode(bytes);
    let key = format_path(slot, path);
    out.insert(key, JsonValue::String(encoded));
}

/// Rebuild a JSON value for the given slot from the flat context.
pub fn unflatten_slot(slot: usize, flat: &FlatContext) -> JsonValue {
    let mut root = JsonValue::Object(JsonMap::new());
    let prefix = format!(".[{slot}]");

    for (key, value) in flat {
        if key == &prefix {
            root = value.clone();
            continue;
        }

        let Some(stripped) = key.strip_prefix(&format!("{prefix}.")) else {
            continue;
        };

        insert_nested_json(&mut root, stripped, value.clone());
    }

    root
}

fn flatten_value(target: &mut FlatContext, prefix: &str, value: &JsonValue) {
    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            target.insert(prefix.to_string(), value.clone());
        }
        JsonValue::Array(items) => {
            target.insert(prefix.to_string(), JsonValue::Array(items.clone()));
            for (index, item) in items.iter().enumerate() {
                let key = format!("{prefix}[{index}]");
                flatten_value(target, &key, item);
            }
        }
        JsonValue::Object(map) => {
            target.insert(prefix.to_string(), JsonValue::Object(map.clone()));
            for (key, item) in map {
                let child = format!("{prefix}.{key}");
                flatten_value(target, &child, item);
            }
        }
    }
}

fn insert_nested_json(target: &mut JsonValue, raw_key: &str, value: JsonValue) {
    let segments = parse_segments(raw_key);
    if segments.is_empty() {
        return;
    }

    let mut cursor = target;

    for (idx, segment) in segments.iter().enumerate() {
        let is_last = idx == segments.len() - 1;
        let next_segment = segments.get(idx + 1);

        match segment {
            Segment::Field(name) => {
                let object = ensure_object(cursor);
                if is_last {
                    object.insert(name.clone(), value);
                    return;
                }

                cursor = object
                    .entry(name.clone())
                    .or_insert_with(|| match next_segment {
                        Some(Segment::Index(_)) => JsonValue::Array(Vec::new()),
                        _ => JsonValue::Object(JsonMap::new()),
                    });
            }
            Segment::Index(index) => {
                let array = ensure_array(cursor);
                if array.len() <= *index {
                    array.resize(index + 1, JsonValue::Null);
                }

                if is_last {
                    array[*index] = value;
                    return;
                }

                let next_is_array = matches!(next_segment, Some(Segment::Index(_)));
                if array[*index].is_null()
                    || (!array[*index].is_array() && !array[*index].is_object())
                {
                    array[*index] = if next_is_array {
                        JsonValue::Array(Vec::new())
                    } else {
                        JsonValue::Object(JsonMap::new())
                    };
                }

                cursor = array.get_mut(*index).expect("index within bounds");
            }
        }
    }
}

fn format_path(slot: usize, path: &str) -> String {
    let trimmed = path.trim_matches('.');
    if trimmed.is_empty() {
        format!(".[{slot}]")
    } else {
        format!(".[{slot}].{trimmed}")
    }
}

fn parse_segments(raw: &str) -> Vec<Segment> {
    let expression = if raw.starts_with('.') {
        raw.to_string()
    } else {
        format!(".{raw}")
    };

    parse_jq_segments(&expression).expect("flattened context key must be valid jq expression")
}

fn ensure_object(value: &mut JsonValue) -> &mut JsonMap<String, JsonValue> {
    if !value.is_object() {
        *value = JsonValue::Object(JsonMap::new());
    }

    match value {
        JsonValue::Object(map) => map,
        _ => unreachable!("value ensured to be object"),
    }
}

fn ensure_array(value: &mut JsonValue) -> &mut Vec<JsonValue> {
    if !value.is_array() {
        *value = JsonValue::Array(Vec::new());
    }

    match value {
        JsonValue::Array(array) => array,
        _ => unreachable!("value ensured to be array"),
    }
}

fn value_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn convert_dollar_expression(expression: &str) -> Result<String, ContextError> {
    let trimmed = expression.trim();
    if !trimmed.starts_with("$.") {
        return Err(ContextError::InvalidExpression {
            expression: expression.to_string(),
        });
    }

    let remainder = trimmed
        .trim_start_matches('$')
        .trim_start_matches('.')
        .trim();

    if remainder.is_empty() {
        return Err(ContextError::InvalidExpression {
            expression: expression.to_string(),
        });
    }

    let mut segments = remainder.splitn(2, '.');
    let slot_segment = segments.next().unwrap_or_default();

    let slot = slot_segment
        .parse::<usize>()
        .map_err(|_| ContextError::InvalidExpression {
            expression: expression.to_string(),
        })?;

    let mut canonical = format!(".[{slot}]");

    if let Some(tail) = segments.next() {
        if !tail.is_empty() {
            canonical.push('.');
            canonical.push_str(tail);
        }
    }

    Ok(canonical)
}

#[derive(Debug)]
enum Segment {
    Field(String),
    Index(usize),
}

fn parse_jq_segments(expression: &str) -> Result<Vec<Segment>, ContextError> {
    let trimmed = expression.trim();
    let mut chars = trimmed.chars().peekable();

    match chars.next() {
        Some('.') => {}
        _ => {
            return Err(ContextError::InvalidJqExpression {
                expression: expression.to_string(),
                reason: "expression must start with `.`".to_string(),
            });
        }
    }

    let mut segments = Vec::new();

    loop {
        while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
            chars.next();
        }

        let Some(&next) = chars.peek() else {
            break;
        };

        if next == '.' {
            chars.next();
            continue;
        }

        if next == '[' {
            chars.next();

            while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
                chars.next();
            }

            let mut digits = String::new();
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() {
                    digits.push(c);
                    chars.next();
                } else {
                    break;
                }
            }

            if digits.is_empty() {
                return Err(ContextError::InvalidJqExpression {
                    expression: expression.to_string(),
                    reason: "array index missing in `[]`".to_string(),
                });
            }

            while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
                chars.next();
            }

            match chars.next() {
                Some(']') => {}
                _ => {
                    return Err(ContextError::InvalidJqExpression {
                        expression: expression.to_string(),
                        reason: "array index segment missing closing `]`".to_string(),
                    });
                }
            }

            let index =
                digits
                    .parse::<usize>()
                    .map_err(|err| ContextError::InvalidJqExpression {
                        expression: expression.to_string(),
                        reason: format!("failed to parse array index `{digits}`: {err}"),
                    })?;

            segments.push(Segment::Index(index));
            continue;
        }

        let mut name = String::new();
        while let Some(&c) = chars.peek() {
            if c == '.' || c == '[' {
                break;
            }
            if c.is_whitespace() {
                break;
            }
            name.push(c);
            chars.next();
        }

        if name.is_empty() {
            return Err(ContextError::InvalidJqExpression {
                expression: expression.to_string(),
                reason: "unexpected token in expression".to_string(),
            });
        }

        segments.push(Segment::Field(name));
    }

    Ok(segments)
}

fn parse_literal_string(expression: &str) -> Option<String> {
    let trimmed = expression.trim();
    if trimmed.len() < 2 {
        return None;
    }

    let first = trimmed.chars().next()?;
    let last = trimmed.chars().last()?;

    if first != last || (first != '"' && first != '\'') {
        return None;
    }

    let inner = &trimmed[1..trimmed.len() - 1];

    if first == '"' {
        Some(unescape_double_quoted(inner))
    } else {
        Some(unescape_single_quoted(inner))
    }
}

fn unescape_double_quoted(inner: &str) -> String {
    let mut output = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                match next {
                    '\\' => output.push('\\'),
                    '"' => output.push('"'),
                    'n' => output.push('\n'),
                    'r' => output.push('\r'),
                    't' => output.push('\t'),
                    other => {
                        output.push(other);
                    }
                }
            }
        } else {
            output.push(ch);
        }
    }

    output
}

fn unescape_single_quoted(inner: &str) -> String {
    let mut output = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                match next {
                    '\\' => output.push('\\'),
                    '\'' => output.push('\''),
                    other => output.push(other),
                }
            }
        } else {
            output.push(ch);
        }
    }

    output
}

#[derive(Debug, Error)]
pub enum ContextError {
    #[error("expression `{expression}` is not a valid slot reference")]
    InvalidExpression { expression: String },
    #[error("slot `{slot}` not found while resolving `{expression}`")]
    MissingSlot { expression: String, slot: usize },
    #[error("path `{path}` missing in slot `{slot}` while resolving `{expression}`")]
    MissingPath {
        expression: String,
        slot: usize,
        path: String,
    },
    #[error(
        "path `{path}` in slot `{slot}` expected {expected} but found {actual} while resolving `{expression}`"
    )]
    TypeMismatch {
        expression: String,
        slot: usize,
        path: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("jq expression `{expression}` is invalid: {reason}")]
    InvalidJqExpression { expression: String, reason: String },
    #[error("jq expression `{expression}` missing value at segment `{segment}`")]
    JqMissingPath { expression: String, segment: String },
    #[error(
        "jq expression `{expression}` expected {expected} at segment `{segment}` but found {actual}"
    )]
    JqTypeMismatch {
        expression: String,
        segment: String,
        expected: &'static str,
        actual: &'static str,
    },
    #[error("jaq expression `{expression}` returned no results")]
    JaqNoResults { expression: String },
    #[error("jaq expression `{expression}` produced multiple results")]
    JaqMultipleResults { expression: String },
    #[error("jaq expression `{expression}` failed at runtime: {error}")]
    JaqRuntime { expression: String, error: String },
}

impl ContextError {
    pub fn expression(&self) -> &str {
        match self {
            ContextError::InvalidExpression { expression }
            | ContextError::MissingSlot { expression, .. }
            | ContextError::MissingPath { expression, .. }
            | ContextError::TypeMismatch { expression, .. }
            | ContextError::InvalidJqExpression { expression, .. }
            | ContextError::JqMissingPath { expression, .. }
            | ContextError::JqTypeMismatch { expression, .. }
            | ContextError::JaqNoResults { expression }
            | ContextError::JaqMultipleResults { expression }
            | ContextError::JaqRuntime { expression, .. } => expression,
        }
    }
}
