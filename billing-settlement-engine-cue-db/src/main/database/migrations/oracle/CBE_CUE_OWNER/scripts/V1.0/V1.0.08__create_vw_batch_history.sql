CREATE OR REPLACE VIEW vw_batch_history (batch_code, attempt) AS
  SELECT batch_code,
         MAX(attempt) AS attempt
  FROM batch_history
  WHERE state = 'COMPLETED'
  GROUP BY batch_code;