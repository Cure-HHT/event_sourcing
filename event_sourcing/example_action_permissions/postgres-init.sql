-- Run once by the Postgres image when it initializes an empty data volume
-- (docker-compose.yml mounts it into /docker-entrypoint-initdb.d). Creates
-- the runtime role the demo's servers connect as: it owns nothing and holds
-- no attribute that lets it become the owner. `evs`, the image's superuser,
-- owns the schema and provisions it (`--provision`), which declares this
-- role and grants it its privileges.
CREATE ROLE evs_runtime LOGIN PASSWORD 'evs'
  NOSUPERUSER NOCREATEDB NOCREATEROLE;
