-- Basic smoke test for the experimental ReaderExecutor read path.
-- Reads a local MergeTree table with `use_reader_executor = 1`, checks the data
-- comes back correct (full scan, point lookup, range, string column), and proves
-- the executor path was actually taken via `system.text_log`.

DROP TABLE IF EXISTS t_reader_executor;

CREATE TABLE t_reader_executor
(
    id UInt64,
    v UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- Enough rows that column .bin files span many read blocks.
INSERT INTO t_reader_executor
SELECT number, number * 2, concat('row_', toString(number))
FROM numbers(300000);

SET use_reader_executor = 1;

-- Full scan over numeric columns. The `log_comment` marks this query so the
-- activation check below can find it in the logs by its query id.
SELECT count(), sum(id), sum(v) FROM t_reader_executor SETTINGS log_comment = '04316_reader_executor_probe';

-- Point lookup: seek to a single granule and read one row.
SELECT id, v, s FROM t_reader_executor WHERE id = 150000;

-- Bounded range read.
SELECT count(), min(id), max(id) FROM t_reader_executor WHERE id BETWEEN 100000 AND 100099;

-- String column read at the tail of the data.
SELECT s FROM t_reader_executor WHERE id = 299999;

-- Force a full scan of the string column.
SELECT sum(length(s)) FROM t_reader_executor;

-- Activation check: `ReadPipeline::build` logs `using ReaderExecutor ...` at
-- DEBUG when the executor path is chosen. Confirm at least one such line was
-- emitted for the marked query (correlated by query id, scoped to this test's
-- own database so parallel tests can't interfere). Prints 1 when the executor
-- was active.
SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0
FROM system.text_log
WHERE logger_name = 'ReadPipeline'
  AND message LIKE '%using ReaderExecutor%'
  AND query_id IN (
      SELECT query_id
      FROM system.query_log
      WHERE log_comment = '04316_reader_executor_probe'
        AND type = 'QueryFinish'
        AND current_database = currentDatabase()
  );

DROP TABLE t_reader_executor;
