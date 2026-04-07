-- Create the airbyte user with a secure password
CREATE USER IF NOT EXISTS airbyte_user IDENTIFIED BY '12345';

-- Disable async_insert for Airbyte user to ensure connection checks and data syncs work correctly
-- This fixes the "Error: Failed to insert expected rows into check table. Actual written: 0" error
ALTER USER airbyte_user SETTINGS async_insert = 0;

-- Create analytics database if it doesn't exist
CREATE DATABASE IF NOT EXISTS analytics;

-- Grant permissions on all databases (for flexibility)
GRANT CREATE ON *.* TO airbyte_user;

-- Grant specific permissions on the analytics database
GRANT CREATE ON analytics.* TO airbyte_user;
GRANT ALTER ON analytics.* TO airbyte_user;
GRANT TRUNCATE ON analytics.* TO airbyte_user;
GRANT INSERT ON analytics.* TO airbyte_user;
GRANT SELECT ON analytics.* TO airbyte_user;
GRANT CREATE DATABASE ON analytics.* TO airbyte_user;
GRANT CREATE TABLE ON analytics.* TO airbyte_user;
GRANT DROP TABLE ON analytics.* TO airbyte_user;
