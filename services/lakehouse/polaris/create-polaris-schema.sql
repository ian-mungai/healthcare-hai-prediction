-- Polaris does not create its own schema (a privileged step), so PostgreSQL creates it on first start.
create schema if not exists polaris_schema;
