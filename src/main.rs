// SPDX-License-Identifier: GPL-3.0-or-later

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use dialoguer::{Confirm, Input, Select};
use pdrive_sync::{
    CliDrive, Config, ConflictPolicy, DeletePolicy, SyncConfig, SyncMode, default_config_path,
    load_config, open_database, optimize_cli_cache, resolved_state_paths, sync_all,
    validate_config, write_success_file,
};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(
    name = "pdrive-sync",
    version,
    about = "Sync local folders through Proton Drive's official CLI and SDK"
)]
struct Cli {
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run configured syncs or one sync described entirely by flags.
    Sync(SyncArgs),
    /// Create a configuration interactively.
    Setup,
    /// Install and start a user service.
    Install(ServiceArgs),
    /// Show service state and the last successful run.
    Status(ServiceTarget),
    /// Stop and remove the user service. Configuration and state are preserved.
    Uninstall(ServiceTarget),
    /// Inspect or validate the configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Run continuously for service managers without timers.
    #[command(hide = true)]
    Daemon(DaemonArgs),
}

#[derive(Debug, Args)]
struct SyncArgs {
    /// Names of configured syncs to run. Runs all when omitted.
    names: Vec<String>,
    /// Local root for a one-off sync.
    #[arg(long, requires = "remote", conflicts_with = "names")]
    local: Option<PathBuf>,
    /// Absolute Proton Drive path for a one-off sync.
    #[arg(long, requires = "local", conflicts_with = "names")]
    remote: Option<String>,
    #[arg(long, value_enum, default_value = "push")]
    mode: SyncMode,
    #[arg(long, value_enum, default_value = "keep")]
    delete: DeletePolicy,
    #[arg(long, value_enum, default_value = "fail")]
    conflict: ConflictPolicy,
    /// Proton Drive CLI executable or absolute path.
    #[arg(long, default_value = "proton-drive")]
    proton_drive: PathBuf,
}

impl Default for SyncArgs {
    fn default() -> Self {
        Self {
            names: Vec::new(),
            local: None,
            remote: None,
            mode: SyncMode::Push,
            delete: DeletePolicy::Keep,
            conflict: ConflictPolicy::Fail,
            proton_drive: PathBuf::from("proton-drive"),
        }
    }
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Print the configuration path.
    Path,
    /// Parse and validate the configuration.
    Validate,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
enum InitSystem {
    #[default]
    Auto,
    Systemd,
    Dinit,
    Openrc,
}

#[derive(Debug, Args)]
struct ServiceArgs {
    #[arg(long, value_enum, default_value = "auto")]
    init: InitSystem,
    /// Wait this long after a completed run before starting the next.
    #[arg(long, default_value = "1h", value_parser = parse_duration)]
    interval: Duration,
}

#[derive(Debug, Args)]
struct ServiceTarget {
    #[arg(long, value_enum, default_value = "auto")]
    init: InitSystem,
}

#[derive(Clone, Debug, Args)]
struct DaemonArgs {
    #[arg(long, default_value = "1h", value_parser = parse_duration)]
    interval: Duration,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("[pdrive-sync] ERROR: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let config_path = cli.config.map_or_else(default_config_path, Ok)?;
    match cli.command.unwrap_or(Command::Sync(SyncArgs::default())) {
        Command::Sync(args) => run_sync(&config_path, args),
        Command::Setup => setup(&config_path),
        Command::Install(args) => install_service(&config_path, args),
        Command::Status(target) => service_status(&config_path, target.init),
        Command::Uninstall(target) => uninstall_service(target.init),
        Command::Daemon(args) => run_daemon(&config_path, args),
        Command::Config { command } => match command {
            ConfigCommand::Path => {
                println!("{}", config_path.display());
                Ok(())
            }
            ConfigCommand::Validate => {
                let config = load_config(&config_path)?;
                println!(
                    "{}: valid ({} sync{})",
                    config_path.display(),
                    config.syncs.len(),
                    if config.syncs.len() == 1 { "" } else { "s" }
                );
                Ok(())
            }
        },
    }
}

fn run_sync(config_path: &Path, args: SyncArgs) -> Result<()> {
    let config = if let (Some(local), Some(remote)) = (args.local, args.remote) {
        let name = one_off_name(&local, &remote, args.mode);
        Config {
            proton_drive_bin: args.proton_drive,
            optimize_cli_cache: true,
            state_db: None,
            success_file: None,
            syncs: vec![SyncConfig {
                name,
                mode: args.mode,
                local,
                remote,
                ready_marker: None,
                delete: args.delete,
                conflict: args.conflict,
                exclude: Vec::new(),
            }],
        }
    } else {
        let mut config = load_config(config_path)?;
        if !args.names.is_empty() {
            let requested = args.names.into_iter().collect::<HashSet<_>>();
            let configured = config
                .syncs
                .iter()
                .map(|sync| sync.name.as_str())
                .collect::<HashSet<_>>();
            let mut missing = requested
                .iter()
                .filter(|name| !configured.contains(name.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            missing.sort();
            if !missing.is_empty() {
                bail!("unknown configured syncs: {}", missing.join(", "));
            }
            config.syncs.retain(|sync| requested.contains(&sync.name));
        }
        config
    };

    validate_config(&config)?;
    optimize_cli_cache(&config)?;
    let (database_path, success_path) = resolved_state_paths(&config)?;
    let connection = open_database(&database_path)?;
    let mut drive = CliDrive::new(config.proton_drive_bin.clone());
    let summaries = sync_all(&config, &connection, &mut drive)
        .with_context(|| format!("sync configured by {} failed", config_path.display()))?;

    for (name, summary) in summaries {
        println!(
            "[pdrive-sync] {name}: scanned={} unchanged={} matched_remote={} uploaded={} downloaded={} trashed_remote={} trashed_local={} skipped_symlinks={}",
            summary.scanned,
            summary.unchanged,
            summary.matched_remote,
            summary.uploaded,
            summary.downloaded,
            summary.trashed,
            summary.trashed_local,
            summary.skipped_symlinks
        );
    }
    write_success_file(&success_path)?;
    Ok(())
}

fn setup(config_path: &Path) -> Result<()> {
    if config_path.exists()
        && !Confirm::new()
            .with_prompt(format!(
                "{} already exists. Replace it?",
                config_path.display()
            ))
            .default(false)
            .interact()?
    {
        println!("Configuration was not changed.");
        return Ok(());
    }

    let proton_drive_bin = Input::<String>::new()
        .with_prompt("Proton Drive CLI executable")
        .default("proton-drive".to_owned())
        .interact_text()?;
    let name = Input::<String>::new()
        .with_prompt("Sync name")
        .default("default".to_owned())
        .interact_text()?;
    let mode_index = Select::new()
        .with_prompt("Direction")
        .items(["Push local to Proton Drive", "Pull to local", "Two-way"])
        .default(0)
        .interact()?;
    let mode = [SyncMode::Push, SyncMode::Pull, SyncMode::TwoWay][mode_index];
    let local = Input::<String>::new()
        .with_prompt("Local folder")
        .interact_text()?;
    let remote = Input::<String>::new()
        .with_prompt("Proton Drive folder")
        .default("/my-files/Sync".to_owned())
        .interact_text()?;
    let exact = Confirm::new()
        .with_prompt("Move files deleted from the source into the other side's Trash?")
        .default(false)
        .interact()?;
    let conflict = if mode == SyncMode::TwoWay {
        let index = Select::new()
            .with_prompt("When both sides changed")
            .items([
                "Stop without changing either side",
                "Local wins",
                "Remote wins",
            ])
            .default(0)
            .interact()?;
        [
            ConflictPolicy::Fail,
            ConflictPolicy::LocalWins,
            ConflictPolicy::RemoteWins,
        ][index]
    } else {
        ConflictPolicy::Fail
    };
    let marker = Input::<String>::new()
        .with_prompt("Required marker inside the local folder (leave empty for none)")
        .allow_empty(true)
        .interact_text()?;
    let config = Config {
        proton_drive_bin: PathBuf::from(proton_drive_bin),
        optimize_cli_cache: true,
        state_db: None,
        success_file: None,
        syncs: vec![SyncConfig {
            name,
            mode,
            local: PathBuf::from(local),
            remote,
            ready_marker: (!marker.is_empty()).then(|| PathBuf::from(marker)),
            delete: if exact {
                DeletePolicy::Trash
            } else {
                DeletePolicy::Keep
            },
            conflict,
            exclude: Vec::new(),
        }],
    };
    validate_config(&config)?;
    let contents = toml::to_string_pretty(&config).context("failed to serialize configuration")?;
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let temporary = config_path.with_extension(format!("tmp.{}", std::process::id()));
    fs::write(&temporary, contents)?;
    fs::rename(&temporary, config_path)?;
    println!("Wrote {}", config_path.display());
    println!("Run `pdrive-sync config validate` before enabling a service.");
    Ok(())
}

fn run_daemon(config_path: &Path, args: DaemonArgs) -> Result<()> {
    loop {
        if let Err(error) = run_sync(config_path, SyncArgs::default()) {
            eprintln!("[pdrive-sync] sync failed: {error:#}");
        }
        thread::sleep(args.interval);
    }
}

fn install_service(config_path: &Path, args: ServiceArgs) -> Result<()> {
    load_config(config_path)?;
    let init = resolve_init(args.init)?;
    let binary = std::env::current_exe().context("failed to locate the current executable")?;
    match init {
        InitSystem::Systemd => install_systemd(&binary, config_path, args.interval),
        InitSystem::Dinit => install_dinit(&binary, config_path, args.interval),
        InitSystem::Openrc => install_openrc(&binary, config_path, args.interval),
        InitSystem::Auto => unreachable!(),
    }?;
    println!(
        "Installed pdrive-sync.service for {}.",
        init_system_name(init)
    );
    Ok(())
}

fn service_status(config_path: &Path, requested: InitSystem) -> Result<()> {
    if let Ok(config) = load_config(config_path) {
        let (_, success_path) = resolved_state_paths(&config)?;
        match fs::read_to_string(&success_path) {
            Ok(timestamp) => println!("Last successful run: {}", timestamp.trim()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                println!("Last successful run: never")
            }
            Err(error) => return Err(error.into()),
        }
    }

    let init = resolve_init(requested)?;
    let status = match init {
        InitSystem::Systemd => ProcessCommand::new("systemctl")
            .args([
                "--user",
                "status",
                "pdrive-sync.service",
                "pdrive-sync.timer",
                "--no-pager",
            ])
            .status(),
        InitSystem::Dinit => ProcessCommand::new("dinitctl")
            .args(["--user", "status", "pdrive-sync"])
            .status(),
        InitSystem::Openrc => ProcessCommand::new("rc-service")
            .args(["--user", "pdrive-sync", "status"])
            .status(),
        InitSystem::Auto => unreachable!(),
    }
    .with_context(|| format!("failed to query {}", init_system_name(init)))?;
    let healthy = if matches!(init, InitSystem::Systemd) {
        command_succeeds(
            "systemctl",
            &["--user", "is-active", "--quiet", "pdrive-sync.timer"],
        ) && !command_succeeds(
            "systemctl",
            &["--user", "is-failed", "--quiet", "pdrive-sync.service"],
        )
    } else {
        status.success()
    };
    if !healthy {
        bail!(
            "{} reported an inactive or failed service",
            init_system_name(init)
        );
    }
    Ok(())
}

fn uninstall_service(requested: InitSystem) -> Result<()> {
    let init = resolve_init(requested)?;
    let config_dir = user_config_dir()?;
    match init {
        InitSystem::Systemd => {
            let _ = ProcessCommand::new("systemctl")
                .args([
                    "--user",
                    "disable",
                    "--now",
                    "pdrive-sync.timer",
                    "pdrive-sync.service",
                ])
                .status();
            remove_if_exists(&config_dir.join("systemd/user/pdrive-sync.service"))?;
            remove_if_exists(&config_dir.join("systemd/user/pdrive-sync.timer"))?;
            checked_command("systemctl", &["--user", "daemon-reload"])?;
        }
        InitSystem::Dinit => {
            let _ = ProcessCommand::new("dinitctl")
                .args(["--user", "stop", "pdrive-sync"])
                .status();
            let _ = ProcessCommand::new("dinitctl")
                .args(["--user", "disable", "pdrive-sync"])
                .status();
            remove_if_exists(&config_dir.join("dinit.d/pdrive-sync"))?;
        }
        InitSystem::Openrc => {
            let _ = ProcessCommand::new("rc-service")
                .args(["--user", "pdrive-sync", "stop"])
                .status();
            let _ = ProcessCommand::new("rc-update")
                .args(["--user", "delete", "pdrive-sync", "default"])
                .status();
            remove_if_exists(&config_dir.join("rc/init.d/pdrive-sync"))?;
        }
        InitSystem::Auto => unreachable!(),
    }
    println!("Removed pdrive-sync.service. Configuration and sync state were preserved.");
    Ok(())
}

fn install_systemd(binary: &Path, config: &Path, interval: Duration) -> Result<()> {
    let directory = user_config_dir()?.join("systemd/user");
    let (service, timer) = systemd_units(binary, config, interval);
    write_atomic(&directory.join("pdrive-sync.service"), service.as_bytes())?;
    write_atomic(&directory.join("pdrive-sync.timer"), timer.as_bytes())?;
    checked_command("systemctl", &["--user", "daemon-reload"])?;
    checked_command(
        "systemctl",
        &["--user", "enable", "--now", "pdrive-sync.timer"],
    )
}

fn install_dinit(binary: &Path, config: &Path, interval: Duration) -> Result<()> {
    let path = user_config_dir()?.join("dinit.d/pdrive-sync");
    let service = dinit_service(binary, config, interval);
    write_atomic(&path, service.as_bytes())?;
    checked_command("dinitctl", &["--user", "enable", "pdrive-sync"])?;
    checked_command("dinitctl", &["--user", "start", "pdrive-sync"])
}

fn dinit_service(binary: &Path, config: &Path, interval: Duration) -> String {
    let command = [
        binary.as_os_str().to_string_lossy().into_owned(),
        "--config".to_owned(),
        config.as_os_str().to_string_lossy().into_owned(),
        "daemon".to_owned(),
        "--interval".to_owned(),
        humantime::format_duration(interval).to_string(),
    ]
    .into_iter()
    .map(|argument| dinit_quote(&argument))
    .collect::<Vec<_>>()
    .join(" ");
    format!("type = process\ncommand = {command}\nrestart = true\nsmooth-recovery = true\n")
}

fn install_openrc(binary: &Path, config: &Path, interval: Duration) -> Result<()> {
    let path = user_config_dir()?.join("rc/init.d/pdrive-sync");
    let service = openrc_service(binary, config, interval);
    write_atomic(&path, service.as_bytes())?;
    #[cfg(unix)]
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
    checked_command("rc-update", &["--user", "add", "pdrive-sync", "default"])?;
    checked_command("rc-service", &["--user", "pdrive-sync", "start"])
}

fn openrc_service(binary: &Path, config: &Path, interval: Duration) -> String {
    let arguments = format!(
        "--config {} daemon --interval {}",
        shell_quote(&config.as_os_str().to_string_lossy()),
        shell_quote(&humantime::format_duration(interval).to_string())
    );
    format!(
        "#!/sbin/openrc-run\ndescription=\"Sync configured folders with Proton Drive\"\nsupervisor=supervise-daemon\ncommand={}\ncommand_args={}\nrespawn_delay=5\n",
        shell_quote(&shell_quote(&binary.as_os_str().to_string_lossy())),
        shell_quote(&arguments)
    )
}

fn systemd_units(binary: &Path, config: &Path, interval: Duration) -> (String, String) {
    let service = format!(
        "[Unit]\nDescription=Sync configured folders with Proton Drive\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=oneshot\nEnvironment=PATH=%h/.local/bin:%h/.cargo/bin:/home/linuxbrew/.linuxbrew/bin:/usr/local/bin:/usr/bin:/bin\nEnvironment=PROTON_DRIVE_LOG_LEVEL=WARNING\nExecStart={} --config {} sync\nNice=10\nCPUWeight=25\nIOWeight=25\nMemoryHigh=512M\n",
        systemd_quote(binary),
        systemd_quote(config)
    );
    let timer = format!(
        "[Unit]\nDescription=Run pdrive-sync\n\n[Timer]\nOnBootSec=5min\nOnUnitInactiveSec={}s\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n",
        interval.as_secs()
    );
    (service, timer)
}

fn resolve_init(requested: InitSystem) -> Result<InitSystem> {
    if !matches!(requested, InitSystem::Auto) {
        return Ok(requested);
    }
    if command_succeeds("systemctl", &["--user", "show-environment"]) {
        return Ok(InitSystem::Systemd);
    }
    if command_succeeds("dinitctl", &["--user", "list"]) {
        return Ok(InitSystem::Dinit);
    }
    if command_succeeds("rc-status", &["--user"]) {
        return Ok(InitSystem::Openrc);
    }
    bail!("no running systemd, dinit, or OpenRC user service manager was detected")
}

fn init_system_name(init: InitSystem) -> &'static str {
    match init {
        InitSystem::Auto => "automatic detection",
        InitSystem::Systemd => "systemd",
        InitSystem::Dinit => "dinit",
        InitSystem::Openrc => "OpenRC",
    }
}

fn parse_duration(value: &str) -> std::result::Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok(duration)
}

fn command_succeeds(program: &str, args: &[&str]) -> bool {
    ProcessCommand::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn checked_command(program: &str, args: &[&str]) -> Result<()> {
    let status = ProcessCommand::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to run {program}"))?;
    if !status.success() {
        bail!("{program} exited with {status}");
    }
    Ok(())
}

fn user_config_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home).join(".config"))
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension(format!("tmp.{}", std::process::id()));
    fs::write(&temporary, contents)?;
    fs::rename(&temporary, path)?;
    Ok(())
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn systemd_quote(path: &Path) -> String {
    let value = path.as_os_str().to_string_lossy();
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace('$', "$$")
    )
}

fn dinit_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn one_off_name(local: &Path, remote: &str, mode: SyncMode) -> String {
    let mut digest = Sha1::new();
    digest.update(local.as_os_str().as_encoded_bytes());
    digest.update([0]);
    digest.update(remote.as_bytes());
    digest.update([0, mode as u8]);
    format!("one-off-{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_commands_are_top_level() {
        assert!(matches!(
            Cli::try_parse_from(["pdrive-sync-rs", "status"])
                .unwrap()
                .command,
            Some(Command::Status(_))
        ));
        assert!(matches!(
            Cli::try_parse_from(["pdrive-sync-rs", "install", "--interval", "30m"])
                .unwrap()
                .command,
            Some(Command::Install(ServiceArgs { interval, .. }))
                if interval == Duration::from_secs(30 * 60)
        ));
    }

    #[test]
    fn generated_service_files_use_the_public_service_name() {
        let binary = Path::new("/home/user name/.local/bin/pdrive-sync-rs");
        let config = Path::new("/home/user name/.config/pdrive-sync/config.toml");
        let (systemd_service, systemd_timer) =
            systemd_units(binary, config, Duration::from_secs(3600));
        let dinit = dinit_service(binary, config, Duration::from_secs(3600));
        let openrc = openrc_service(binary, config, Duration::from_secs(3600));

        assert!(systemd_service.contains("pdrive-sync-rs"));
        assert!(systemd_service.contains("\"/home/user name/.local/bin/pdrive-sync-rs\""));
        assert!(systemd_service.contains("%h/.local/bin"));
        assert!(systemd_timer.contains("OnUnitInactiveSec=3600s"));
        assert!(systemd_service.contains("MemoryHigh=512M"));
        assert!(dinit.contains("\"/home/user name/.local/bin/pdrive-sync-rs\""));
        assert!(dinit.contains("daemon"));
        assert!(openrc.contains("supervisor=supervise-daemon"));
        assert!(openrc.contains("pdrive-sync-rs"));
    }
}
