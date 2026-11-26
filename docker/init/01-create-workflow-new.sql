-- Provision the destination library used by archive migration (迁库).
-- Runs only on first initialization of an empty MySQL data volume.
CREATE DATABASE IF NOT EXISTS `workflow_new`
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- The demo application account is created by the entrypoint before init scripts
-- run; grant it access to the destination library as well.
GRANT ALL PRIVILEGES ON `workflow_new`.* TO 'app'@'%';
FLUSH PRIVILEGES;
