SELECT MAX(tip_amount) as max_tip
  FROM yellow_taxi_trips
 GROUP BY tpep_pickup_datetime
 ORDER BY 1 DESC
