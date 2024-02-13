use crate::{Options, PgDump, PgRestore, Psql};

use log::{debug, info, trace};
use semver::{BuildMetadata, Prerelease, Version};
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    process::{Output, Stdio},
    result,
};
use tokio::process::Command;
use url::Url;

const API_SCHEMA_DIRECTORY: &str = "api";
const DATA_SCHEMA: &str = "data";
const MIGRATION_DIRECTORY: &str = "migration";
const DEFAULT_VERSION: Version = Version {
    major: 0,
    minor: 0,
    patch: 0,
    pre: Prerelease::EMPTY,
    build: BuildMetadata::EMPTY,
};

pub type Result = result::Result<(), String>;

#[derive(Debug)]
pub struct Database {
    version: Version,
    api_schema: String,
    connection: Url,
    psql: Psql,
    pg_dump: PgDump,
    pg_restore: PgRestore,
    sql_directory: PathBuf,
}

impl Database {
    pub fn new(
        app_version: &str,
        options: Options,
    ) -> result::Result<Self, String> {
        Ok(Self {
            version: Version::parse(app_version).map_err(|err| {
                format!("invalid version '{}': {err}", app_version)
            })?,
            api_schema: options
                .connection
                .params()
                .get("user")
                .ok_or_else(|| {
                    String::from("connection parameters must contain a user")
                })?
                .clone(),
            connection: options.connection.as_url(),
            psql: options.psql.clone(),
            pg_dump: options.pg_dump.clone(),
            pg_restore: options.pg_restore.clone(),
            sql_directory: options.sql_directory.to_owned(),
        })
    }

    pub async fn check_schema_version(&self) -> Result {
        match self.schema_version().await? {
            Some(version) if version == self.version => {
                debug!("Data schema up to date");
                return Ok(());
            }
            Some(version) => {
                info!(
                    "Data schema out of date (v{version}): \
                    starting migration..."
                );
            }
            _ => info!("Data schema not initialized: starting migration..."),
        }

        self.migrate().await
    }

    pub async fn dump(&self, path: &Path) -> Result {
        let mut command = self.pg_dump.command();
        command
            .arg("--format")
            .arg("custom")
            .arg("--file")
            .arg(path);

        self.exec(command).await.map_err(|err| {
            format!(
                "failed to create database dump at '{}': {err}",
                path.display()
            )
        })?;

        Ok(())
    }

    pub async fn init(&self) -> Result {
        self.create_data_schema().await?;

        let mut path = self.sql_directory.join(DATA_SCHEMA);
        path.set_extension("sql");

        self.psql([
            OsStr::new("--command"),
            OsStr::new(&format!("SET search_path TO {DATA_SCHEMA}")),
            OsStr::new("--file"),
            path.as_os_str(),
        ])
        .await?;

        self.update().await?;

        Ok(())
    }

    pub async fn migrate(&self) -> Result {
        self.drop_api_schema().await?;
        self.migrate_data().await?;
        self.update().await?;

        Ok(())
    }

    pub async fn reset(&self) -> Result {
        self.drop_api_schema().await?;
        self.drop_data_schema().await?;
        self.init().await?;

        Ok(())
    }

    pub async fn restore(&self, path: &Path) -> Result {
        let mut command = self.pg_restore.command();
        command
            .arg("--clean")
            .arg("--create")
            .arg("--if-exists")
            .arg(path);

        self.exec(command).await?;
        self.analyze().await?;

        Ok(())
    }

    async fn exec(
        &self,
        mut command: Command,
    ) -> result::Result<String, String> {
        let program =
            command.as_std().get_program().to_str().unwrap().to_string();

        let Output {
            status,
            stdout,
            stderr,
        } = command
            .arg("--dbname")
            .arg(self.connection.as_str())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| {
                format!("failed to spawn child process '{program}': {err}")
            })?
            .wait_with_output()
            .await
            .map_err(|err| {
                format!("failed to run program '{program}': {err}")
            })?;

        trace!("Program '{program}' exited with status: {status}");

        if status.success() {
            let output = String::from_utf8_lossy(&stdout);
            let output = output.trim();
            return Ok(output.to_string());
        }

        let error = String::from_utf8_lossy(&stderr);
        let error = error.trim();

        Err(format!("program '{program}' failed: {status}: {error}"))
    }

    async fn analyze(&self) -> Result {
        self.query("ANALYZE")
            .await
            .map_err(|err| format!("failed to run ANALYZE: {err}"))?;

        Ok(())
    }

    async fn create_schema(&self, schema: &str) -> Result {
        self.query(&format!("CREATE SCHEMA {schema}"))
            .await
            .map_err(|err| {
                format!("failed to create schema '{schema}': {err}")
            })?;

        debug!("Created {schema} schema");

        Ok(())
    }

    async fn create_api_schema(&self) -> Result {
        self.create_schema(&self.api_schema).await
    }

    async fn create_data_schema(&self) -> Result {
        self.create_schema(DATA_SCHEMA).await
    }

    async fn drop_schema(&self, schema: &str) -> Result {
        self.query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .await
            .map_err(|err| {
                format!("failed to drop schema '{schema}': {err}")
            })?;

        debug!("Dropped {schema} schema");

        Ok(())
    }

    async fn drop_api_schema(&self) -> Result {
        self.drop_schema(&self.api_schema).await
    }

    async fn drop_data_schema(&self) -> Result {
        self.drop_schema(DATA_SCHEMA).await
    }

    async fn migrate_data(&self) -> Result {
        let schema_version =
            self.schema_version().await?.unwrap_or(DEFAULT_VERSION);

        if schema_version == self.version {
            debug!(
                "Schema version and app version are equal: nothing to migrate"
            );
            return Ok(());
        }

        if schema_version > self.version {
            return Err(format!(
                "schema version ({schema_version}) is greater than \
                app version ({}): downgrades are not supported",
                self.version
            ));
        }

        let migration_directory = self.sql_directory.join(MIGRATION_DIRECTORY);

        if !migration_directory.exists() {
            debug!(
                "No migrations to run: directory '{}' does not exist",
                migration_directory.display()
            );
            return Ok(());
        }

        if !migration_directory.is_dir() {
            return Err(format!(
                "{}: not a directory",
                migration_directory.display()
            ));
        }

        let sql_extension = Some(OsStr::new("sql"));
        let mut migrations = Vec::new();

        for entry in migration_directory.read_dir().map_err(|err| {
            format!(
                "failed to read migration directory '{}': {err}",
                migration_directory.display()
            )
        })? {
            let entry = entry.map_err(|err| {
                format!(
                    "failed to read migration directory entry '{}': {err}",
                    migration_directory.display()
                )
            })?;

            let path = entry.path();

            if !(path.is_file() && path.extension() == sql_extension) {
                debug!("Skipping '{}': not a SQL file", path.display());
                continue;
            }

            let file_version =
                path.file_stem().unwrap().to_str().ok_or_else(|| {
                    format!(
                        "file name contains invalid version '{}'",
                        path.display()
                    )
                })?;

            let file_version = Version::parse(file_version).map_err(|err| {
                format!(
                    "file name contains invalid version '{}': {err}",
                    path.display()
                )
            })?;

            if schema_version > file_version {
                debug!(
                    "Skipping '{}': schema version is greater",
                    path.display()
                );
                continue;
            }

            if file_version >= self.version {
                debug!(
                    "Skipping '{}': greater than or equal to target",
                    path.display()
                );
                continue;
            }

            debug!("Adding migration: {}", path.display());
            migrations.push((file_version, path));
        }

        if migrations.is_empty() {
            debug!("No migrations to run");
            return Ok(());
        } else {
            debug!(
                "Applying {} migration{}",
                migrations.len(),
                match migrations.len() {
                    1 => "",
                    _ => "s",
                }
            );
        }

        migrations.sort_by(|a, b| a.0.cmp(&b.0));

        let set_search_path = format!("SET search_path TO {DATA_SCHEMA}");
        let set_search_path = OsStr::new(&set_search_path);

        let mut iter = migrations.iter().peekable();

        while let Some((version, path)) = iter.next() {
            info!("Migrating from v{version}");

            self.psql([
                OsStr::new("--command"),
                set_search_path,
                OsStr::new("--single-transaction"),
                OsStr::new("--file"),
                path.as_os_str(),
            ])
            .await
            .map_err(|err| {
                format!(
                    "failed to apply migration script '{}': {err}",
                    path.display()
                )
            })?;

            let next = match iter.peek() {
                Some((version, _)) => version,
                None => &self.version,
            };

            self.set_schema_version(next).await?;
        }

        Ok(())
    }

    async fn psql<I, S>(&self, args: I) -> result::Result<String, String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = self.psql.command();

        command.args([
            "--quiet",
            "--tuples-only",
            "--no-align",
            "--no-psqlrc",
            "--set=ON_ERROR_STOP=1",
        ]);

        command.args(args);

        self.exec(command).await
    }

    async fn query(&self, query: &str) -> result::Result<String, String> {
        self.psql([OsStr::new("--command"), OsStr::new(query)])
            .await
    }

    async fn schema_version(&self) -> result::Result<Option<Version>, String> {
        let result = self
            .query(
                "SELECT exists(\
                    SELECT * FROM pg_proc WHERE proname = 'schema_version'\
                )",
            )
            .await?;

        let version_exists = match result.as_str() {
            "t" => true,
            "f" => false,
            _ => {
                return Err(format!(
                    "unexpected psql output when checking \
                    if schema version exists: {result}"
                ))
            }
        };

        if !version_exists {
            return Ok(None);
        }

        let version = self.query("SELECT data.schema_version()").await?;
        let version = Version::parse(&version).map_err(|err| {
            format!("invalid data schema version '{version}': {err}")
        })?;

        Ok(Some(version))
    }

    async fn set_schema_version(&self, version: &Version) -> Result {
        self.query(&format!(
            "CREATE OR REPLACE FUNCTION data.schema_version() \
            RETURNS text AS $$ \
            BEGIN \
                RETURN '{version}'; \
            END; $$ \
            IMMUTABLE \
            LANGUAGE plpgsql"
        ))
        .await
        .map_err(|err| {
            format!("failed to set schema version to {version}: {err}")
        })?;

        Ok(())
    }

    async fn update(&self) -> Result {
        self.create_api_schema().await?;

        let mut path = self.sql_directory.join(API_SCHEMA_DIRECTORY);
        path.set_extension("sql");

        self.psql([OsStr::new("--file"), path.as_os_str()]).await?;

        self.set_schema_version(&self.version).await?;

        Ok(())
    }
}
