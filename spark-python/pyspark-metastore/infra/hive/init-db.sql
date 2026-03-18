-- Hive Metastore schema will be initialized by the Hive Metastore service.
-- This file ensures the database is ready with proper permissions.

GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
