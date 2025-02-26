CREATE TABLE IF NOT EXISTS `sync_metadata.derivative_queries` (
    tablename STRING,
    query STRING,
    enable BOOL,
    priority INT64
);
