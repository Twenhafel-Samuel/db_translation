START TRANSACTION;

WITH
	latest_times AS (
    SELECT
      jsonb_object_keys(raw_json -> 'Time Series FX (5min)') AS keys
    FROM fx_5min
  ),
  times_and_values AS (
    SELECT
      keys,
      (SELECT raw_json -> 'Time Series FX (5min)' -> keys FROM fx_5min) AS values
    FROM latest_times
	),
  to_add AS (
    SELECT
      (keys::timestamp) AT TIME ZONE 'UTC',
      (values ->> '1. open')::FLOAT AS open,
      (values ->> '2. high')::FLOAT AS high,
      (values ->> '3. low')::FLOAT AS low,
      (values ->> '4. close')::FLOAT AS close
    FROM times_and_values
  )
INSERT INTO ex_rates 
SELECT * FROM to_add
ON CONFLICT (market_time)
DO NOTHING
;

DELETE FROM fx_5min;


COMMIT;
