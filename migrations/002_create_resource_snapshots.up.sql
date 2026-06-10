CREATE TABLE IF NOT EXISTS resource_snapshots (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    pod_name     VARCHAR(255)          NOT NULL,
    name_space   VARCHAR(255)          NOT NULL,
    cpu_usage    VARCHAR(50),
    memory_usage VARCHAR(50),
    recorded_at  DATETIME              NOT NULL
);