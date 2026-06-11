CREATE EVENT IF NOT EXISTS prune_old_data_event
ON SCHEDULE EVERY 1 DAY
DO CALL prune_old_data(30);