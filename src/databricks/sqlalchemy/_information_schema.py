from sqlalchemy import MetaData, Table, Column, String, DateTime

metadata = MetaData()

tables = Table(
    "tables",
    metadata,
    Column("table_catalog", String),
    Column("table_schema", String),
    Column("table_name", String),
    Column("table_type", String),
    Column("is_insertable_into", String),
    Column("commit_action", String),
    Column("table_owner", String),
    Column("comment", String),
    Column("created", DateTime),
    Column("created_by", String),
    Column("last_altered", DateTime),
    Column("last_altered_by", String),
    Column("data_source_format", String),
    Column("storage_sub_directory", String),
    schema="information_schema",
)
