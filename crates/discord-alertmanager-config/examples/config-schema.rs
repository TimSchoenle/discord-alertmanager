//! Generates the configuration reference from the types that load it.
//!
//! Nine renderings come out of one schema. CI regenerates `docs/config.md`, `docs/config.json`,
//! `docs/config.contract.json` and `config.example.toml` on every pull request and fails on any
//! difference, so a key cannot be added without its documentation.
//!
//! The `contract` and `labels` renderings are the pipeline's half of the same schema. The
//! container build runs both in one stage, so the labels an image carries and the document it
//! publishes cannot describe different configurations.
//!
//! ```bash
//! cargo run -p discord-alertmanager-config --features config-schema --example config-schema \
//!     -- --format markdown > docs/config.md
//! ```
//!
//! Nothing here reads the environment, so the output is identical on a runner where none of the
//! variables it describes are set.

use std::process::ExitCode;

use dam_config::{Config, ENV_PREFIX};
use terrace_config::Terrace;
use terrace_config::schema::cli::Cli;
use terrace_config::schema::{App, Docs, JsonSchema, TomlExample};

fn main() -> ExitCode {
    // Not `dam_config::layers()`, though the two now agree: the loader's builder is shared with
    // the running service, and anything added here for the generator's sake would land in that
    // service's dialect too. The dialect this renders is the prefix and nothing else.
    let schema = Terrace::new(ENV_PREFIX)
        .schema::<Config>()
        .with_defaults_from(&Config::default())
        .expect("the default configuration serialises");

    Cli::new(
        // No `.version()` here. The release is passed as `--version` by the container build,
        // which is the only place that knows one. Deriving it from `CARGO_PKG_VERSION` would put
        // the previous release into the committed `docs/config.contract.json` the moment
        // release-please opened a pull request bumping the manifests, and the drift gate would
        // then fail the release pull request itself, every release, over a field that describes
        // no configuration key.
        App::new("discord-alertmanager")
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
