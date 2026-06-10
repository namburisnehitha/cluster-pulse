CREATE TABLE IF NOT EXISTS analyses (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    pod_name        VARCHAR(255)          NOT NULL,
    name_space      VARCHAR(255)          NOT NULL,
    severity        VARCHAR(50),
    confidence      VARCHAR(50),
    is_recurring    BOOLEAN,
    failure_time    DATETIME,
    analyzed_at     DATETIME             NOT NULL,
    details         JSON
);