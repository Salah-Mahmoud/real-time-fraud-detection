-- 📌 Total Fraud Cases Detected (Last 24H) (Stat)
SELECT COUNT(*) AS fraud_cases
FROM processed_transactions
WHERE is_fraud = 1
AND trans_date_trans_time > NOW() - INTERVAL '24 HOURS';


-- 📌 Fraud Rate (%) (Stat)
SELECT
    (COUNT(*) FILTER (WHERE is_fraud = 1) * 100.0) / COUNT(*) AS fraud_rate
FROM processed_transactions
WHERE trans_date_trans_time > NOW() - INTERVAL '24 HOURS';

-- 📌 Latest Fraudulent Transactions (Table)
SELECT trans_num, cc_num, amt, category, city, state, trans_date_trans_time
FROM processed_transactions
WHERE is_fraud = 1
ORDER BY trans_date_trans_time DESC
LIMIT 10;

-- 📌 Fraud Cases Per Hour (Line Chart)
SELECT
    DATE_TRUNC('hour', trans_date_trans_time) AS txn_hour,
    COUNT(*) AS fraud_count
FROM processed_transactions
WHERE is_fraud = 1
AND trans_date_trans_time >= NOW() - INTERVAL '24 HOURS'
GROUP BY txn_hour
ORDER BY txn_hour ASC;



