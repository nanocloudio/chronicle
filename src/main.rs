#![allow(clippy::result_large_err)]

use anyhow::{anyhow, Context};
use chronicle::config::{integration::known_feature_flags, ChronicleConfig, IntegrationConfig};
use chronicle::telemetry;
use jsonschema::{Draft, JSONSchema};
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::warn;

const EMBEDDED_SCHEMA: &str = include_str!("../docs/reference/chronicle.schema.json");
const DEFAULT_SCHEMA_PATH: &str = "docs/reference/chronicle.schema.json";

enum CliCommand {
    Run {
        integration_path: Option<String>,
    },
    Validate {
        schema_path: Option<String>,
        configs: Vec<String>,
    },
    ListFeatureFlags,
    Help,
    ValidateHelp,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing().context("failed to initialise telemetry")?;

    match parse_cli_args()? {
        CliCommand::Run { integration_path } => {
            let mut config = ChronicleConfig::load().context("failed to load configuration")?;
            if let Some(path) = integration_path {
                config.integration_config_path = Some(path);
            }

            let app = chronicle::app::ChronicleApp::initialise(config)
                .await
                .context("failed to construct application")?;

            app.run().await.context("application runtime error")
        }
        CliCommand::Validate {
            schema_path,
            configs,
        } => {
            run_validate_command(schema_path, configs)?;
            Ok(())
        }
        CliCommand::ListFeatureFlags => {
            print_feature_flags();
            Ok(())
        }
        CliCommand::Help => {
            print_help();
            Ok(())
        }
        CliCommand::ValidateHelp => {
            print_validate_help();
            Ok(())
        }
    }
}

fn parse_cli_args() -> anyhow::Result<CliCommand> {
    let mut args = std::env::args().skip(1);
    let Some(first) = args.next() else {
        return Ok(CliCommand::Run {
            integration_path: None,
        });
    };

    if first == "validate" {
        return parse_validate_args(args);
    }

    let mut integration_path = None;
    let mut pending = Some(first);

    loop {
        let arg = match pending.take() {
            Some(value) => value,
            None => match args.next() {
                Some(value) => value,
                None => break,
            },
        };

        match arg.as_str() {
            "-c" | "--config" => {
                if integration_path.is_some() {
                    anyhow::bail!("integration config path specified multiple times");
                }
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("expected path after {arg}"))?;
                integration_path = Some(value);
            }
            "-h" | "--help" => return Ok(CliCommand::Help),
            "--list-feature-flags" => return Ok(CliCommand::ListFeatureFlags),
            other => anyhow::bail!("unrecognised argument `{other}`"),
        }
    }

    Ok(CliCommand::Run { integration_path })
}

fn parse_validate_args<I>(args: I) -> anyhow::Result<CliCommand>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut schema_path = None;
    let mut configs = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--schema" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("expected path after --schema"))?;
                schema_path = Some(value);
            }
            "-h" | "--help" => return Ok(CliCommand::ValidateHelp),
            other => configs.push(other.to_string()),
        }
    }

    if configs.is_empty() {
        anyhow::bail!("chronicle validate requires at least one config path");
    }

    Ok(CliCommand::Validate {
        schema_path,
        configs,
    })
}

fn print_help() {
    println!(
        "\
Usage: chronicle [OPTIONS]
       chronicle validate [--schema <PATH>] <CONFIG>...

Options:
  -c, --config <PATH>    Path to chronicle integration YAML file
      --list-feature-flags
                          Print the supported app.feature_flags entries
  -h, --help             Print this help message

Validate:
      --schema <PATH>    Override path to docs/reference/chronicle.schema.json
  -h, --help             Print this help message
"
    );
}

fn print_feature_flags() {
    println!("Supported feature flags:");
    for flag in known_feature_flags() {
        println!("  - {flag}");
    }
}

fn print_validate_help() {
    println!(
        "\
Usage: chronicle validate [OPTIONS] <CONFIG>...

Options:
      --schema <PATH>    Override docs/reference/chronicle.schema.json
  -h, --help             Print this help message
"
    );
}

fn run_validate_command(
    schema_override: Option<String>,
    configs: Vec<String>,
) -> anyhow::Result<()> {
    let schema = compile_json_schema(schema_override.as_deref())?;
    let mut had_error = false;

    for config in configs {
        let path = PathBuf::from(&config);
        if let Err(err) = validate_with_schema(&schema, &path) {
            eprintln!("{err}");
            had_error = true;
            continue;
        }

        match IntegrationConfig::from_path(&path) {
            Ok(_) => println!("validated {}", path.display()),
            Err(err) => {
                eprintln!("{err}");
                had_error = true;
            }
        }
    }

    if had_error {
        Err(anyhow!("one or more configs failed validation"))
    } else {
        Ok(())
    }
}

fn compile_json_schema(override_path: Option<&str>) -> anyhow::Result<JSONSchema> {
    if let Some(path) = override_path {
        return load_schema_from_path(Path::new(path));
    }

    let default_path = Path::new(DEFAULT_SCHEMA_PATH);
    match load_schema_from_path(default_path) {
        Ok(schema) => Ok(schema),
        Err(err) => {
            warn!(
                schema = DEFAULT_SCHEMA_PATH,
                error = %err,
                "falling back to embedded chronicle.schema.json"
            );
            compile_embedded_schema()
        }
    }
}

fn compile_embedded_schema() -> anyhow::Result<JSONSchema> {
    let parsed: JsonValue =
        serde_json::from_str(EMBEDDED_SCHEMA).context("embedded JSON schema is invalid JSON")?;
    compile_schema_from_json(parsed, "embedded schema")
}

fn load_schema_from_path(path: &Path) -> anyhow::Result<JSONSchema> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read JSON schema at {}", path.display()))?;
    let parsed: JsonValue = serde_json::from_str(&raw)
        .with_context(|| format!("invalid JSON schema {}", path.display()))?;
    compile_schema_from_json(parsed, &path.display().to_string())
}

fn compile_schema_from_json(value: JsonValue, label: &str) -> anyhow::Result<JSONSchema> {
    let leaked: &'static JsonValue = Box::leak(Box::new(value));
    JSONSchema::options()
        .with_draft(Draft::Draft202012)
        .compile(leaked)
        .with_context(|| format!("failed to compile JSON schema {label}"))
}

fn validate_with_schema(schema: &JSONSchema, path: &Path) -> anyhow::Result<()> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let yaml: YamlValue = serde_yaml::from_str(&raw)
        .with_context(|| format!("{} is not valid YAML", path.display()))?;
    let json =
        serde_json::to_value(yaml).context("failed to convert YAML configuration to JSON")?;

    if let Err(errors) = schema.validate(&json) {
        let mut message = String::new();
        for error in errors {
            use std::fmt::Write as _;
            let _ = writeln!(message, "- {}: {}", path.display(), error);
        }
        anyhow::bail!("schema validation failed:\n{}", message.trim_end());
    }

    Ok(())
}
