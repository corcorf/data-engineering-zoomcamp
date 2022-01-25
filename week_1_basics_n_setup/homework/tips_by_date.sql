SELECT DATE(tpep_pickup_datetime) as dt
     , MAX(tip_amount) as max_tip 
  FROM yellow_taxi_trips
 GROUP BY 1
 ORDER BY 2 DESC
