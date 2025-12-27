use chronicle::codec::http::RouteTemplate;

#[test]
fn parse_simple_template() {
    let tmpl = RouteTemplate::parse("/v2/").unwrap();
    // Simple template with no params is exact
    assert!(tmpl.is_exact());
}

#[test]
fn parse_template_with_params() {
    let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
    // Template with params is not exact
    assert!(!tmpl.is_exact());
}

#[test]
fn match_exact_path() {
    let tmpl = RouteTemplate::parse("/v2/").unwrap();
    let params = tmpl.match_path("/v2/").unwrap();
    assert!(params.is_empty());
}

#[test]
fn match_path_with_params() {
    let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
    let params = tmpl
        .match_path("/v2/library/alpine/manifests/latest")
        .unwrap();
    assert_eq!(params.get("namespace").unwrap(), "library");
    assert_eq!(params.get("name").unwrap(), "alpine");
    assert_eq!(params.get("reference").unwrap(), "latest");
}

#[test]
fn match_path_mismatch() {
    let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
    assert!(tmpl
        .match_path("/v2/library/alpine/blobs/sha256:abc")
        .is_none());
}

#[test]
fn match_path_url_encoded() {
    let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
    let params = tmpl
        .match_path("/v2/my%2Fnamespace/alpine/manifests/v1.0")
        .unwrap();
    assert_eq!(params.get("namespace").unwrap(), "my/namespace");
}

#[test]
fn to_axum_pattern() {
    let tmpl = RouteTemplate::parse("/v2/{namespace}/{name}/manifests/{reference}").unwrap();
    assert_eq!(
        tmpl.to_axum_pattern(),
        "/v2/:namespace/:name/manifests/:reference"
    );
}
