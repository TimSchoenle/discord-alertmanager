| Variable | Role | Default | Purpose |
|---|---|---|---|
| `DAM_CONFIG` | config | `config.toml` | Names the TOML layer: a file, or a directory whose `*.toml` files are all merged in name order. |
| `DAM_SECRETS_DIR` | secrets dir | — | Names a directory of key-named files — a mounted Kubernetes `Secret` volume. Each file supplies the key its name spells. |
| `DAM_LOG_FORMAT` | reserved | — | Read directly from the environment before the layered config exists, so no file may supply it. |
| `DAM_LOG_LEVEL` | reserved | — | Read directly from the environment before the layered config exists, so no file may supply it. |

| TOML | Type | Environment | Default | Flags | Purpose |
|---|---|---|---|---|---|
| `discord.token` | `SecretString` | `DAM_DISCORD__TOKEN` | unset | secret | Bot token. Supply it through `DAM_DISCORD__TOKEN_FILE` or the secrets directory. |
| `discord.dev_guild_id` | `u64` | `DAM_DISCORD__DEV_GUILD_ID` | unset | — | Guild to register slash commands into. Registration is global when unset. |
| `discord.capture_reply_text` | `bool` | `DAM_DISCORD__CAPTURE_REPLY_TEXT` | `false` | — | Capture the text of thread replies, which needs the privileged `MESSAGE_CONTENT` intent. |
| `discord.capabilities.view` | `Vec<String>` | `DAM_DISCORD__CAPABILITIES__VIEW` | `[@everyone]` | — | Read alerts, silences and routes. Grants no mutation of any kind. |
| `discord.capabilities.operate` | `Vec<String>` | `DAM_DISCORD__CAPABILITIES__OPERATE` | `[]` | — | Acknowledge, assign, and add or remove bot-local ignores. |
| `discord.capabilities.silence` | `Vec<String>` | `DAM_DISCORD__CAPABILITIES__SILENCE` | `[]` | — | Create, extend and expire Alertmanager silences, which affects every receiver. |
| `discord.capabilities.admin` | `Vec<String>` | `DAM_DISCORD__CAPABILITIES__ADMIN` | `[]` | — | Manage routes and read the effective configuration. |
| `alertmanager.endpoints` | `Vec<Url>` | `DAM_ALERTMANAGER__ENDPOINTS` | `[]` | — | Base URLs of the Alertmanager peers, tried in order. |
| `alertmanager.bearer_token` | `SecretString` | `DAM_ALERTMANAGER__BEARER_TOKEN` | unset | secret | Bearer token sent to Alertmanager. Supply it through the secrets directory or `_FILE`. |
| `alertmanager.basic_username` | `String` | `DAM_ALERTMANAGER__BASIC_USERNAME` | unset | — | Username for basic authentication. Ignored when `bearer_token` is set. |
| `alertmanager.basic_password` | `SecretString` | `DAM_ALERTMANAGER__BASIC_PASSWORD` | unset | secret | Password for basic authentication. Supply it through the secrets directory or `_FILE`. |
| `alertmanager.ca_bundle` | `PathBuf` | `DAM_ALERTMANAGER__CA_BUNDLE` | unset | — | PEM bundle of certificate authorities to trust in addition to the system roots. |
| `alertmanager.timeout_secs` | `u64` | `DAM_ALERTMANAGER__TIMEOUT_SECS` | `10` | — | Seconds to wait for a whole request before giving up. |
| `alertmanager.connect_timeout_secs` | `u64` | `DAM_ALERTMANAGER__CONNECT_TIMEOUT_SECS` | `2` | — | Seconds to wait for a connection before trying the next endpoint. |
| `alertmanager.retry.initial_backoff_ms` | `u64` | `DAM_ALERTMANAGER__RETRY__INITIAL_BACKOFF_MS` | `200` | — | Milliseconds to wait before the first retry. |
| `alertmanager.retry.max_backoff_secs` | `u64` | `DAM_ALERTMANAGER__RETRY__MAX_BACKOFF_SECS` | `10` | — | Ceiling on a single wait, in seconds. |
| `alertmanager.retry.max_elapsed_secs` | `u64` | `DAM_ALERTMANAGER__RETRY__MAX_ELAPSED_SECS` | `45` | — | Seconds to keep retrying one request before giving up on it. |
| `storage.backend` | `Backend`: `sqlite` \| `postgres` | `DAM_STORAGE__BACKEND` | `sqlite` | — | Which backend the bot connects to. |
| `storage.sqlite.path` | `PathBuf` | `DAM_STORAGE__SQLITE__PATH` | `discord-alertmanager.db` | — | Path to the database file, created on first start if it does not exist. |
| `storage.sqlite.max_connections` | `u32` | `DAM_STORAGE__SQLITE__MAX_CONNECTIONS` | `4` | — | Size of the read pool. The writer is always one connection. |
| `storage.sqlite.acquire_timeout_secs` | `u64` | `DAM_STORAGE__SQLITE__ACQUIRE_TIMEOUT_SECS` | `5` | — | Seconds to wait for a connection from the pool before failing the operation. |
| `storage.sqlite.migrate_on_start` | `bool` | `DAM_STORAGE__SQLITE__MIGRATE_ON_START` | `true` | — | Run pending migrations during startup. |
| `storage.postgres.url` | `SecretString` | `DAM_STORAGE__POSTGRES__URL` | unset | secret | Connection URL. Supply it through `DAM_STORAGE__POSTGRES__URL_FILE` or the secrets directory, since it carries the password. |
| `storage.postgres.max_connections` | `u32` | `DAM_STORAGE__POSTGRES__MAX_CONNECTIONS` | `16` | — | Maximum pooled connections. |
| `storage.postgres.acquire_timeout_secs` | `u64` | `DAM_STORAGE__POSTGRES__ACQUIRE_TIMEOUT_SECS` | `5` | — | Seconds to wait for a connection from the pool before failing the operation. |
| `storage.postgres.migrate_on_start` | `bool` | `DAM_STORAGE__POSTGRES__MIGRATE_ON_START` | `true` | — | Run pending migrations during startup. |
| `ingest.bind` | `SocketAddr` | `DAM_INGEST__BIND` | `0.0.0.0:9099` | — | Address and port to listen on. |
| `ingest.webhook_path` | `String` | `DAM_INGEST__WEBHOOK_PATH` | `/webhook` | — | Path Alertmanager posts the version-4 envelope to. |
| `ingest.webhook_token` | `SecretString` | `DAM_INGEST__WEBHOOK_TOKEN` | unset | secret | Bearer token every webhook request has to carry. |
| `ingest.body_limit_bytes` | `usize` | `DAM_INGEST__BODY_LIMIT_BYTES` | `1048576` | — | Largest accepted request body, in bytes. |
| `ingest.request_timeout_secs` | `u64` | `DAM_INGEST__REQUEST_TIMEOUT_SECS` | `10` | — | Seconds a request may take before the listener abandons it. |
| `ingest.max_concurrent_requests` | `usize` | `DAM_INGEST__MAX_CONCURRENT_REQUESTS` | `64` | — | Requests handled at once. Further requests queue rather than being rejected. |
| `ingest.shutdown_drain_secs` | `u64` | `DAM_INGEST__SHUTDOWN_DRAIN_SECS` | `10` | — | Seconds to let in-flight requests finish during shutdown. |
| `engine.dispatchers` | `u32` | `DAM_ENGINE__DISPATCHERS` | `4` | — | Outbox dispatcher workers. |
| `engine.outbox_lease_secs` | `u64` | `DAM_ENGINE__OUTBOX_LEASE_SECS` | `30` | — | Seconds a claimed outbox row stays claimed before a janitor may reclaim it. |
| `engine.outbox_batch_size` | `u32` | `DAM_ENGINE__OUTBOX_BATCH_SIZE` | `16` | — | Outbox rows one worker claims per pass. |
| `engine.reconcile_interval_secs` | `u64` | `DAM_ENGINE__RECONCILE_INTERVAL_SECS` | `60` | — | Seconds between reconciler polls of the Alertmanager alert set. |
| `engine.silence_sync_interval_secs` | `u64` | `DAM_ENGINE__SILENCE_SYNC_INTERVAL_SECS` | `30` | — | Seconds between silence syncs. |
| `engine.escalation_interval_secs` | `u64` | `DAM_ENGINE__ESCALATION_INTERVAL_SECS` | `15` | — | Seconds between escalation timer sweeps. |
| `engine.prune_interval_secs` | `u64` | `DAM_ENGINE__PRUNE_INTERVAL_SECS` | `3600` | — | Seconds between retention sweeps. |
| `engine.deadman_window_secs` | `u64` | `DAM_ENGINE__DEADMAN_WINDOW_SECS` | `1800` | — | Seconds of webhook silence that, combined with an unreachable Alertmanager, trips the deadman. |
| `engine.regroup_window_secs` | `u64` | `DAM_ENGINE__REGROUP_WINDOW_SECS` | `1800` | — | Seconds within which a re-fire reuses the existing card and thread. |
| `engine.persist_events` | `bool` | `DAM_ENGINE__PERSIST_EVENTS` | `true` | — | Record a row in `alert_events` for every state transition. |
| `engine.storm.threshold` | `u32` | `DAM_ENGINE__STORM__THRESHOLD` | `50` | — | Alerts on one route inside the window that trigger digest mode. |
| `engine.storm.window_secs` | `u64` | `DAM_ENGINE__STORM__WINDOW_SECS` | `60` | — | Length of the window, in seconds. |
| `engine.storm.forum_threshold` | `u32` | `DAM_ENGINE__STORM__FORUM_THRESHOLD` | `20` | — | Threshold for forum routes, which is lower. |
| `engine.retention.events_days` | `u32` | `DAM_ENGINE__RETENTION__EVENTS_DAYS` | `30` | — | Days of `alert_events` history. This is the expensive table. |
| `engine.retention.resolved_days` | `u32` | `DAM_ENGINE__RETENTION__RESOLVED_DAYS` | `30` | — | Days a resolved alert and its notification are kept. |
| `engine.retention.audit_days` | `u32` | `DAM_ENGINE__RETENTION__AUDIT_DAYS` | `365` | — | Days of `audit_log`. |
| `render.debounce_secs` | `u64` | `DAM_RENDER__DEBOUNCE_SECS` | `3` | — | Seconds to coalesce edits to one card before sending them. |
| `render.description_budget` | `usize` | `DAM_RENDER__DESCRIPTION_BUDGET` | `1500` | — | Characters of annotation text a card may carry before it is truncated. |
| `render.key_labels` | `Vec<String>` | `DAM_RENDER__KEY_LABELS` | `[namespace, instance, job]` | — | Labels promoted to their own inline field on the card, in order. |
| `render.thread_archive_after_minutes` | `u32` | `DAM_RENDER__THREAD_ARCHIVE_AFTER_MINUTES` | `1440` | — | Minutes of inactivity after which an alert thread archives. |
| `render.show_fingerprint` | `bool` | `DAM_RENDER__SHOW_FINGERPRINT` | `true` | — | Show a short fingerprint in the card footer. |
| `links.prometheus_base` | `Url` | `DAM_LINKS__PROMETHEUS_BASE` | unset | — | Prometheus base URL, available to templates as `links.prometheus_base`. |
| `links.grafana_base` | `Url` | `DAM_LINKS__GRAFANA_BASE` | unset | — | Grafana base URL, available to templates as `links.grafana_base`. |
| `links.allowed_hosts` | `Vec<String>` | `DAM_LINKS__ALLOWED_HOSTS` | `[]` | — | Hosts a rendered button may point at. |
| `links.window_lead_secs` | `u64` | `DAM_LINKS__WINDOW_LEAD_SECS` | `900` | — | Seconds of graph shown before the alert started. |
| `links.window_trail_secs` | `u64` | `DAM_LINKS__WINDOW_TRAIL_SECS` | `300` | — | Seconds of graph shown after the alert ended, or after now while it is firing. |
| `links.buttons` | `Vec<LinkButton>` | `DAM_LINKS__BUTTONS` | `[]` | — | The buttons themselves, rendered in order. |
| `observability.metrics_enabled` | `bool` | `DAM_OBSERVABILITY__METRICS_ENABLED` | `true` | — | Serve Prometheus metrics at `/metrics` on the ingest listener. |
| `observability.admin_channel_id` | `u64` | `DAM_OBSERVABILITY__ADMIN_CHANNEL_ID` | unset | — | Channel the deadman and route-health notices post to. |
| `routes` | `Vec<RouteConfig>` | `DAM_ROUTES` | `[]` | — | Routes declared in the file, which cannot be edited or deleted from Discord. |
