WITH partition_sizes AS (
    SELECT parent.relname AS relation_name,
           sum(pg_total_relation_size(child.oid)) AS total_bytes,
           sum(pg_indexes_size(child.oid)) AS index_bytes,
           sum(stats.n_live_tup) AS estimated_rows,
           sum(stats.n_dead_tup) AS estimated_dead_rows
    FROM pg_inherits inheritance
    JOIN pg_class parent ON parent.oid = inheritance.inhparent
    JOIN pg_class child ON child.oid = inheritance.inhrelid
    JOIN pg_namespace namespace ON namespace.oid = parent.relnamespace
    JOIN pg_stat_user_tables stats ON stats.relid = child.oid
    WHERE namespace.nspname = 'public'
      AND parent.relname IN ('events', 'deliveries', 'delivery_attempts',
          'authentication_attempts', 'replay_requests')
    GROUP BY parent.relname
), ordinary_sizes AS (
    SELECT relname AS relation_name,
           pg_total_relation_size(relid) AS total_bytes,
           pg_indexes_size(relid) AS index_bytes,
           n_live_tup AS estimated_rows,
           n_dead_tup AS estimated_dead_rows
    FROM pg_stat_user_tables
    WHERE schemaname = 'public'
      AND relname IN ('ingestion_outbox', 'delivery_outbox')
), sizes AS (
    SELECT * FROM partition_sizes
    UNION ALL
    SELECT * FROM ordinary_sizes
)
SELECT relation_name, total_bytes, index_bytes, estimated_rows,
       round(total_bytes::numeric / nullif(estimated_rows, 0), 1)
           AS estimated_bytes_per_row,
       estimated_dead_rows
FROM sizes ORDER BY relation_name;

SELECT pg_database_size(current_database()) AS database_bytes;
SELECT wal_bytes AS cluster_wal_bytes FROM pg_stat_wal;
