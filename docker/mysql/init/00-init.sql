-- MySQL 初始化脚本
-- 此文件会在 MySQL 容器首次启动时自动执行

ALTER DATABASE engine_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- engine 用户：仅用于 migration 等管理操作（人工使用）
GRANT ALL PRIVILEGES ON engine_db.* TO 'engine'@'%';

-- agent 用户：供 AI agent 使用，仅允许安全的 DML 操作
CREATE USER IF NOT EXISTS 'agent'@'%' IDENTIFIED BY 'agent_safe_pass';
GRANT SELECT, INSERT, UPDATE, DELETE ON engine_db.* TO 'agent'@'%';
-- 明确禁止：DROP, CREATE, ALTER, TRUNCATE, INDEX, REFERENCES, GRANT 等全部不授予

FLUSH PRIVILEGES;
