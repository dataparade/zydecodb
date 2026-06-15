use clap::{Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};
use tracing_subscriber::EnvFilter;
use zydecodb::security::keys::KeyRole;

#[derive(Parser)]
#[command(name = "zydecodb", about = "ZydecoDB database server")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Load config and start the TCP server.
    Serve {
        #[arg(long, short)]
        config: PathBuf,
        /// Run as a read-only replica, ingesting sha256-verified WAL segments
        /// shipped by a primary into this directory. Overrides `[replica].from`.
        #[arg(long, value_name = "DIR")]
        replica_from: Option<PathBuf>,
        /// How often (ms) to poll for newly shipped segments in replica mode.
        #[arg(long, value_name = "MS")]
        replica_poll_ms: Option<u64>,
    },
    /// Print build version.
    Version,
    /// Administrative commands.
    Admin {
        #[command(subcommand)]
        command: AdminCommands,
    },
    /// Read-replica operations: health probe and manual promotion.
    Replica {
        #[command(subcommand)]
        command: ReplicaCommands,
    },
}

#[derive(Subcommand)]
enum ReplicaCommands {
    /// Print replica lag + primary liveness. Exits non-zero when the primary's
    /// heartbeat is older than `--max-stale-secs` (use it as a health probe).
    Status {
        #[arg(long, short)]
        config: PathBuf,
        /// Override the shipped-stream source directory (`[replica].from`).
        #[arg(long, value_name = "DIR")]
        from: Option<PathBuf>,
        /// Emit JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
        /// Heartbeat age (seconds) beyond which the primary is considered dead.
        #[arg(long, default_value = "10")]
        max_stale_secs: u64,
    },
    /// Promote this replica to primary: drain the stream, then bump the epoch.
    /// Run with the replica stopped; restart `serve` without a replication
    /// source afterwards.
    Promote {
        #[arg(long, short)]
        config: PathBuf,
        /// Override the shipped-stream source directory (`[replica].from`).
        #[arg(long, value_name = "DIR")]
        from: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum AdminCommands {
    /// Manage API keys.
    Keys {
        #[command(subcommand)]
        command: KeysCommands,
    },
    /// Offboard a tenant: delete all its data and catalog entries. Run with the
    /// node stopped (the data_dir lock enforces exclusive access).
    DropTenant {
        #[arg(long, short)]
        config: PathBuf,
        /// 32-char hex tenant id to remove.
        #[arg(long)]
        tenant: String,
        /// Reclaim tombstoned space before returning instead of leaving it to
        /// background compaction.
        #[arg(long)]
        compact: bool,
    },
    /// Manage per-tenant resource limits (byte cap, request rate).
    Tenant {
        #[command(subcommand)]
        command: TenantCommands,
    },
    /// Capture a base snapshot of the database (offline, or against a replica).
    Snapshot {
        #[arg(long, short)]
        config: PathBuf,
        /// Output directory for the snapshot (SSTables + MANIFEST + SNAPMETA).
        #[arg(long)]
        out: PathBuf,
    },
    /// Rewrite on-disk SSTables to the current format (offline). Accelerates the
    /// migration that background compaction performs over time; legacy-format
    /// files remain readable in the meantime.
    Upgrade {
        #[arg(long, short)]
        config: PathBuf,
    },
    /// Restore from a base snapshot + shipped WAL to a point in time.
    Restore {
        /// Base snapshot directory (from `admin snapshot`).
        #[arg(long)]
        base: PathBuf,
        /// Shipped-WAL directory (the primary's `ship_dir`).
        #[arg(long)]
        wal: PathBuf,
        /// Exact write sequence to stop at (precise).
        #[arg(long)]
        to_seq: Option<u64>,
        /// Wall-clock target in unix milliseconds (best-effort, heartbeat
        /// granularity; resolved via the shipped time index).
        #[arg(long)]
        to_time: Option<u64>,
        /// Output data directory for the restored database.
        #[arg(long)]
        out: PathBuf,
    },
}

#[derive(Subcommand)]
enum TenantCommands {
    /// Set or update a tenant's limits (omit a flag to leave it unchanged). A
    /// running server applies the change on SIGHUP.
    SetLimit {
        #[arg(long)]
        tenant: String,
        /// Maximum stored bytes for this tenant (0 = unlimited).
        #[arg(long)]
        max_bytes: Option<u64>,
        /// Maximum requests per second across this tenant's connections.
        #[arg(long)]
        rate_rps: Option<u32>,
        #[arg(long, default_value = "/etc/zydecodb/keys.toml")]
        keys_file: PathBuf,
    },
    /// List configured per-tenant limits.
    List {
        #[arg(long, default_value = "/etc/zydecodb/keys.toml")]
        keys_file: PathBuf,
    },
}

#[derive(Subcommand)]
enum KeysCommands {
    /// Create a new API key (secret printed once).
    Create {
        #[arg(long)]
        id: String,
        #[arg(long, value_enum, default_value = "read_write")]
        role: RoleArg,
        #[arg(long, default_value = "00000000000000000000000000000000")]
        tenant: String,
        #[arg(long)]
        prefix: Vec<String>,
        #[arg(long, default_value = "/etc/zydecodb/keys.toml")]
        keys_file: PathBuf,
    },
    /// List key ids.
    List {
        #[arg(long, default_value = "/etc/zydecodb/keys.toml")]
        keys_file: PathBuf,
    },
    /// Revoke a key by id.
    Revoke {
        #[arg(long)]
        id: String,
        #[arg(long, default_value = "/etc/zydecodb/keys.toml")]
        keys_file: PathBuf,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum RoleArg {
    ReadOnly,
    ReadWrite,
    Admin,
}

impl From<RoleArg> for KeyRole {
    fn from(r: RoleArg) -> Self {
        match r {
            RoleArg::ReadOnly => KeyRole::ReadOnly,
            RoleArg::ReadWrite => KeyRole::ReadWrite,
            RoleArg::Admin => KeyRole::Admin,
        }
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Serve {
            config,
            replica_from,
            replica_poll_ms,
        } => {
            let mut cfg = load_config(&config);
            if let Some(from) = replica_from {
                cfg.replica.from = Some(from);
            }
            if let Some(poll) = replica_poll_ms {
                cfg.replica.poll_ms = poll;
            }
            let server = zydecodb::server::Server::new();
            server.run(cfg).map_err(|e| e.to_string())
        }
        Commands::Version => {
            println!("zydecodb {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Commands::Admin { command } => match command {
            AdminCommands::Keys { command } => match command {
                KeysCommands::Create {
                    id,
                    role,
                    tenant,
                    prefix,
                    keys_file,
                } => zydecodb::admin::keys_create(keys_file, id, role.into(), tenant, prefix)
                    .map_err(|e| e.to_string()),
                KeysCommands::List { keys_file } => {
                    zydecodb::admin::keys_list(keys_file).map_err(|e| e.to_string())
                }
                KeysCommands::Revoke { id, keys_file } => {
                    zydecodb::admin::keys_revoke(keys_file, id).map_err(|e| e.to_string())
                }
            },
            AdminCommands::DropTenant {
                config,
                tenant,
                compact,
            } => zydecodb::admin::drop_tenant(&config, &tenant, compact),
            AdminCommands::Tenant { command } => match command {
                TenantCommands::SetLimit {
                    tenant,
                    max_bytes,
                    rate_rps,
                    keys_file,
                } => zydecodb::admin::tenant_set_limit(keys_file, tenant, max_bytes, rate_rps)
                    .map_err(|e| e.to_string()),
                TenantCommands::List { keys_file } => {
                    zydecodb::admin::tenant_list(keys_file).map_err(|e| e.to_string())
                }
            },
            AdminCommands::Snapshot { config, out } => zydecodb::admin::snapshot(&config, &out),
            AdminCommands::Upgrade { config } => zydecodb::admin::upgrade(&config),
            AdminCommands::Restore {
                base,
                wal,
                to_seq,
                to_time,
                out,
            } => zydecodb::admin::restore(&base, &wal, to_seq, to_time, &out),
        },
        Commands::Replica { command } => match command {
            ReplicaCommands::Status {
                config,
                from,
                json,
                max_stale_secs,
            } => {
                let cfg = load_config(&config);
                let from = resolve_from(from, cfg.replica.from.clone());
                match zydecodb::replica::status(&from, &cfg.wal_dir, max_stale_secs) {
                    Ok(report) => {
                        if json {
                            println!("{}", report.render_json());
                        } else {
                            println!("{}", report.render_human());
                        }
                        if report.is_ok() {
                            Ok(())
                        } else {
                            std::process::exit(1);
                        }
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            ReplicaCommands::Promote { config, from } => {
                let cfg = load_config(&config);
                let from = resolve_from(from, cfg.replica.from.clone());
                match zydecodb::replica::promote(&from, &cfg.wal_dir, &cfg.data_dir) {
                    Ok(out) => {
                        println!(
                            "promoted: drained {} segment(s), epoch {} -> {} (applied_seq {})",
                            out.drained.len(),
                            out.previous_epoch,
                            out.new_epoch,
                            out.applied_max_seq
                        );
                        println!(
                            "next: restart as primary without a replication source -> \
                             `zydecodb serve --config {}` (remove [replica].from)",
                            config.display()
                        );
                        Ok(())
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
        },
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn load_config(path: &Path) -> zydecodb::config::Config {
    zydecodb::config::Config::from_file(path).unwrap_or_else(|e| {
        eprintln!("failed to load config {}: {e}", path.display());
        std::process::exit(1);
    })
}

/// Resolve the shipped-stream source: an explicit `--from` overrides the config;
/// otherwise fall back to `[replica].from`. Exits if neither is set.
fn resolve_from(flag: Option<PathBuf>, config_from: Option<PathBuf>) -> PathBuf {
    flag.or(config_from).unwrap_or_else(|| {
        eprintln!("no replication source: set [replica].from in the config or pass --from <DIR>");
        std::process::exit(2);
    })
}
