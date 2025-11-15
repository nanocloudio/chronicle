#![allow(dead_code)]

pub fn enable_optional_feature_flags(yaml: &str) -> String {
    let has_app_section = yaml
        .lines()
        .any(|line| line.trim_start().starts_with("app:"));

    if has_app_section {
        yaml.to_string()
    } else {
        let trimmed = yaml.trim_start_matches('\n');
        format!(
            "app:\n  feature_flags:\n    - rabbitmq\n    - mqtt\n    - mongodb\n    - redis\n    - parallel_phase\n    - serialize_phase\n\n{trimmed}"
        )
    }
}
