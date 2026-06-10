CREATE PROCEDURE IF NOT EXISTS prune_old_data(IN retention_days INT)
BEGIN
    DELETE FROM analyses WHERE analyzed_at < NOW() - INTERVAL retention_days DAY;
    DELETE FROM resource_snapshots WHERE recorded_at < NOW() - INTERVAL retention_days DAY;
END;