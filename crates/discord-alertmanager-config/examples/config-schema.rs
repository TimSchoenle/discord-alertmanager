//! Generates the configuration reference from the types that load it.
//!
//! Six renderings come out of one schema. CI regenerates `docs/config.md`, `docs/config.json` and
//! `config.example.toml` on every pull request and fails on any difference, so a key cannot be
//! added without its documentation.
//!
//! ```bash
//! cargo run -p discord-alertmanager-config --features config-schema --example config-schema \
//!     -- --format markdown > docs/config.md
//! ```
//!
//! Nothing here reads the environment, so the output is identical on a runner where none of the
//! variables it describes are set.

use std::process::ExitCode;

use dam_config::{Config, ENV_PREFIX, LOG_FORMAT_VAR, LOG_LEVEL_VAR};
use terrace_config::Terrace;
use terrace_config::schema::cli::Cli;
use terrace_config::schema::{App, Docs, JsonSchema, TomlExample};

fn main() -> ExitCode {
    // Not `dam_config::layers()`: that one is shared with the loader, and reserving a variable
    // there for the sake of this generator would put it in the running service's dialect too.
    // The two reserved names below are the same ones, listed here so they reach the loader table.
    let schema = Terrace::new(ENV_PREFIX)
        .reserve(LOG_FORMAT_VAR)
        .reserve(LOG_LEVEL_VAR)
        .schema::<Config>()
        .with_defaults_from(&Config::default())
        .expect("the default configuration serialises");

    Cli::new(
        // `v0.1.0`, not `0.1.0`: the field exists to be compared against an image tag.
        App::new("discord-alertmanager")
            .version(concat!("v", env!("CARGO_PKG_VERSION")))
            .source("https://github.com/TimSchoenle/discord-alertmanager"),
    )
    .json_schema(
        JsonSchema::new()
            .title("discord-alertmanager configuration")
            .id("https://github.com/TimSchoenle/discord-alertmanager/config.schema.json"),
    )
    // `Docs::Full`, because `config.example.toml` is the only documentation an operator gets
    // before they find `docs/config.md`.
    .toml_example(TomlExample::new().docs(Docs::Full))
    .main(schema)
}
