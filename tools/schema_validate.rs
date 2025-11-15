use std::{
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{bail, Context, Result};
use jsonschema::{Draft, JSONSchema};
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;

fn load_schema(path: &Path) -> Result<JSONSchema> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read JSON schema at {}", path.display()))?;
    let schema_json: JsonValue = serde_json::from_str(&raw)
        .with_context(|| format!("invalid JSON schema {}", path.display()))?;
    let leaked_schema: &'static JsonValue = Box::leak(Box::new(schema_json));
    JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(leaked_schema)
        .with_context(|| format!("failed to compile JSON schema {}", path.display()))
}

fn validate_file(compiled: &JSONSchema, config_path: &Path) -> Result<()> {
    let raw = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let yaml: YamlValue = serde_yaml::from_str(&raw)
        .with_context(|| format!("{} is not valid YAML", config_path.display()))?;
    let json = serde_json::to_value(yaml).context("failed to convert YAML to JSON")?;
    if let Err(errors) = compiled.validate(&json) {
        let mut message = String::new();
        for error in errors {
            use std::fmt::Write as _;
            let _ = writeln!(message, "- {}: {}", config_path.display(), error);
        }
        bail!("schema validation failed:\n{}", message.trim_end());
    }
    Ok(())
}

fn parse_args() -> Result<(PathBuf, Vec<PathBuf>)> {
    let mut args = env::args().skip(1);
    let mut schema_path = PathBuf::from("docs/reference/chronicle.schema.json");
    let mut configs = Vec::new();

    while let Some(arg) = args.next() {
        if arg == "--schema" {
            let path = args.next().context("expected a path after --schema")?;
            schema_path = PathBuf::from(path);
        } else {
            configs.push(PathBuf::from(arg));
        }
    }

    if configs.is_empty() {
        bail!(
            "Usage: schema_validate [--schema path/to/schema.json] <config.yaml> [more configs...]"
        );
    }

    Ok((schema_path, configs))
}

fn run() -> Result<()> {
    let (schema_path, configs) = parse_args()?;
    let compiled = load_schema(&schema_path)?;

    for config in configs {
        validate_file(&compiled, &config)?;
        eprintln!("validated {}", config.display());
    }

    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err:?}");
            ExitCode::FAILURE
        }
    }
}
