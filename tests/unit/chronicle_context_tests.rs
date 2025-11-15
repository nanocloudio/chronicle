
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use chronicle::chronicle::context::{
    flatten_headers_into, flatten_json, insert_bytes, unflatten_slot, ContextError, ExecutionContext,
    FlatContext,
};
use serde_json::{json, Value as JsonValue};

    fn record_fixture() -> JsonValue {
        json!({
            "record": {
                "id": "rec-123",
                "attributes": {
                    "category": "telemetry",
                    "tier": "gold"
                },
                "metrics": {
                    "latency_ms": 42,
                    "error_rate": 0.01
                },
                "observed_at": "2025-10-25T12:00:00Z"
            }
        })
    }

    #[test]
    fn flattens_nested_json() {
        let value = json!({
            "body": {
                "user": {
                    "id": 42,
                    "email": "user@example.com"
                }
            }
        });

        let flat = flatten_json(0, &value);

        assert_eq!(flat.get(".[0].body.user.id"), Some(&json!(42)));
        assert_eq!(
            flat.get(".[0].body.user.email"),
            Some(&json!("user@example.com"))
        );
    }

    #[test]
    fn flattens_arrays_with_indices() {
        let value = json!({
            "items": [
                { "id": "a" },
                { "id": "b" }
            ]
        });

        let flat = flatten_json(1, &value);

        assert_eq!(flat.get(".[1]"), Some(&value));
        let items = value
            .get("items")
            .cloned()
            .expect("array fixture contains items");
        assert_eq!(flat.get(".[1].items"), Some(&items));
        assert_eq!(flat.get(".[1].items[0].id"), Some(&json!("a")));
        assert_eq!(flat.get(".[1].items[1].id"), Some(&json!("b")));
    }

    #[test]
    fn flattens_headers_and_deduplicates() {
        let mut out = FlatContext::new();
        flatten_headers_into(
            0,
            [
                ("X-Request-ID", "abc"),
                ("X-Request-ID", "def"),
                ("Content-Type", "json"),
            ],
            &mut out,
        );

        assert_eq!(out.get(".[0].headers.x-request-id"), Some(&json!("abc")));
        assert_eq!(
            out.get(".[0].headers.x-request-id.1"),
            Some(&json!("def"))
        );
        assert_eq!(
            out.get(".[0].headers.content-type"),
            Some(&json!("json"))
        );
    }

    #[test]
    fn encodes_bytes_as_base64() {
        let mut out = FlatContext::new();
        insert_bytes(2, "body.raw", b"\x00\x01\x02", &mut out);

        assert_eq!(
            out.get(".[2].body.raw"),
            Some(&json!(BASE64_STANDARD.encode(b"\x00\x01\x02")))
        );
    }

    #[test]
    fn unflattens_back_to_json() {
        let mut flat = FlatContext::new();
        flat.insert(".[0].body.id".into(), json!(123));
        flat.insert(".[0].body.email".into(), json!("user@example.com"));
        flat.insert(".[0].items[0]".into(), json!("first"));
        flat.insert(".[0].items[1]".into(), json!("second"));

        let rebuilt = unflatten_slot(0, &flat);

        assert_eq!(
            rebuilt,
            json!({
                "body": {
                    "id": 123,
                    "email": "user@example.com"
                },
                "items": ["first", "second"]
            })
        );
    }

    #[test]
    fn invalid_expressions_return_error() {
        let context = ExecutionContext::new();
        let err = context
            .resolve_expression("not.an.expression")
            .expect_err("invalid expressions should fail");

        match err {
            ContextError::InvalidExpression { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn jq_expressions_resolve_against_slots() {
        let mut context = ExecutionContext::new();
        context.insert_slot(0, record_fixture());

        let record_id = context
            .resolve_template(&json!(".[0].record.id"))
            .expect("record id should resolve");
        assert_eq!(record_id, json!("rec-123"));

        let tier = context
            .resolve_template(&json!(".[0].record.attributes.tier"))
            .expect("tier resolves");
        assert_eq!(tier, json!("gold"));
    }

    #[test]
    fn quoted_strings_are_returned_literal() {
        let mut context = ExecutionContext::new();
        context.insert_slot(0, record_fixture());

        let literal = context
            .resolve_template(&json!("\".[0].record.id\""))
            .expect("literal resolves");
        assert_eq!(literal, json!(".[0].record.id"));

        let single = context
            .resolve_template(&json!("'.[0].record.id'"))
            .expect("single quote literal");
        assert_eq!(single, json!(".[0].record.id"));
    }

    #[test]
    fn resolve_to_string_formats_numbers_and_bools() {
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "count": 42,
                "flag": true,
            }),
        );

        let number = context
            .resolve_to_string(&json!(".[0].count"))
            .expect("number to resolve");
        let flag = context
            .resolve_to_string(&json!(".[0].flag"))
            .expect("bool to resolve");

        assert_eq!(number, "42");
        assert_eq!(flag, "true");
    }

    #[test]
    fn resolve_template_handles_arrays_and_literals() {
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "items": ["alpha", "beta"],
                "metadata": { "trace": "abc123" }
            }),
        );

        let template = json!([".[0].items[0]", "literal", ".[0].metadata.trace"]);
        let resolved = context
            .resolve_template(&template)
            .expect("template to resolve");

        assert_eq!(resolved, json!(["alpha", "literal", "abc123"]));
    }

    #[test]
    fn flatten_json_handles_record_fixture() {
        let fixture = record_fixture();
        let flat = flatten_json(0, &fixture);

        assert_eq!(flat.get(".[0]"), Some(&fixture));
        let record = fixture
            .get("record")
            .cloned()
            .expect("fixture contains record");
        assert_eq!(flat.get(".[0].record"), Some(&record));
        assert_eq!(
            flat.get(".[0].record.attributes.category"),
            Some(&json!("telemetry"))
        );
        assert_eq!(flat.get(".[0].record.metrics.latency_ms"), Some(&json!(42)));
        assert_eq!(
            flat.get(".[0].record.observed_at"),
            Some(&json!("2025-10-25T12:00:00Z"))
        );
    }

    #[test]
    fn execution_context_resolves_record_paths() {
        let mut context = ExecutionContext::new();
        context.insert_slot(0, record_fixture());

        let category = context
            .resolve_expression(".[0].record.attributes.category")
            .expect("category path");
        let latency = context
            .resolve_expression(".[0].record.metrics.latency_ms")
            .expect("metrics path");
        let observed = context
            .resolve_expression(".[0].record.observed_at")
            .expect("timestamp path");

        assert_eq!(category, json!("telemetry"));
        assert_eq!(latency, json!(42));
        assert_eq!(observed, json!("2025-10-25T12:00:00Z"));
    }

    #[test]
    fn selector_reports_type_mismatch_for_numeric_segment_on_object() {
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "record": {
                    "attributes": {
                        "category": "telemetry"
                    }
                }
            }),
        );

        let err = context
            .resolve_expression(".[0].record.attributes[0]")
            .expect_err("type mismatch expected");
        match err {
            ContextError::TypeMismatch {
                slot,
                path,
                expected,
                actual,
                ..
            } => {
                assert_eq!(slot, 0);
                assert_eq!(path, "record.attributes[0]");
                assert_eq!(expected, "array");
                assert_eq!(actual, "object");
            }
            other => panic!("unexpected error: {other:?}"),
        };
    }

    #[test]
    fn selector_reports_type_mismatch_for_field_segment_on_array() {
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "items": [
                    { "value": 1 }
                ]
            }),
        );

        let err = context
            .resolve_expression(".[0].items.value")
            .expect_err("type mismatch expected");

        match err {
            ContextError::TypeMismatch {
                slot,
                path,
                expected,
                actual,
                ..
            } => {
                assert_eq!(slot, 0);
                assert_eq!(path, "items.value");
                assert_eq!(expected, "object");
                assert_eq!(actual, "array");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn jaq_expressions_support_complex_projection() {
        let mut context = ExecutionContext::new();
        context.insert_slot(0, record_fixture());

        let projection = context
            .resolve_expression(
                ".[0].record | {id: .id, summary: {category: .attributes.category, metrics: [.metrics.latency_ms, .metrics.error_rate]}}",
            )
            .expect("projection resolves");

        assert_eq!(
            projection,
            json!({
                "id": "rec-123",
                "summary": {
                    "category": "telemetry",
                    "metrics": [42, 0.01]
                }
            })
        );
    }

    #[test]
    fn jaq_expressions_detect_multiple_results() {
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "items": [1, 2, 3]
            }),
        );

        let err = context
            .resolve_expression(".[0].items[]")
            .expect_err("iteration should yield multiple results");

        match err {
            ContextError::JaqMultipleResults { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn jaq_expressions_detect_no_results() {
        let mut context = ExecutionContext::new();
        context.insert_slot(
            0,
            json!({
                "items": []
            }),
        );

        let err = context
            .resolve_expression(".[0].items[]")
            .expect_err("iteration over empty array should fail");

        match err {
            ContextError::JaqNoResults { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn jaq_invalid_expression_reports_parse_error() {
        let context = ExecutionContext::new();
        let err = context
            .resolve_expression(".[0] | {")
            .expect_err("invalid filter should fail");

        match err {
            ContextError::InvalidJqExpression { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn jaq_missing_key_reports_runtime_error() {
        let mut context = ExecutionContext::new();
        context.insert_slot(0, record_fixture());

        let err = context
            .resolve_expression(".[0].record.missing_field")
            .expect_err("missing field should error");

        match err {
            ContextError::MissingPath { slot, path, .. } => {
                assert_eq!(slot, 0);
                assert_eq!(path, "record.missing_field");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn missing_slot_reports_slot_index() {
        let context = ExecutionContext::new();

        let err = context
            .resolve_expression("$.1.record")
            .expect_err("missing slot should error");

        match err {
            ContextError::MissingSlot { slot, .. } => {
                assert_eq!(slot, 1);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn missing_path_reports_details() {
        let mut context = ExecutionContext::new();
        context.insert_slot(0, json!({ "record": {} }));

        let err = context
            .resolve_expression(".[0].record.id")
            .expect_err("missing field should error");

        match err {
            ContextError::MissingPath { slot, path, .. } => {
                assert_eq!(slot, 0);
                assert_eq!(path, "record.id");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
