use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path};
use tokio::process::Command;
use url::Url;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Psql(String);

impl Psql {
    pub(crate) fn command(&self) -> Command {
        Command::new(&self.0)
    }
}

impl Default for Psql {
    fn default() -> Self {
        Self(option_env!("PGTOOLS_PSQL").unwrap_or("psql").into())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PgDump(String);

impl PgDump {
    pub(crate) fn command(&self) -> Command {
        Command::new(&self.0)
    }
}

impl Default for PgDump {
    fn default() -> Self {
        Self(option_env!("PGTOOLS_PG_DUMP").unwrap_or("pg_dump").into())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PgRestore(String);

impl PgRestore {
    pub(crate) fn command(&self) -> Command {
        Command::new(&self.0)
    }
}

impl Default for PgRestore {
    fn default() -> Self {
        Self(
            option_env!("PGTOOLS_PG_RESTORE")
                .unwrap_or("pg_restore")
                .into(),
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ConnectionParameters(HashMap<String, String>);

impl ConnectionParameters {
    pub fn new(params: HashMap<String, String>) -> Self {
        Self(params)
    }

    pub fn as_url(&self) -> Url {
        Url::parse_with_params("postgresql://", &self.0).unwrap()
    }

    pub fn params(&self) -> &HashMap<String, String> {
        &self.0
    }

    pub fn params_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Options<'a> {
    pub connection: &'a ConnectionParameters,
    pub psql: &'a Psql,
    pub pg_dump: &'a PgDump,
    pub pg_restore: &'a PgRestore,
    pub sql_directory: &'a Path,
}
