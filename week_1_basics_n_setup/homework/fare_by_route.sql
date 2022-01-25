SELECT CONCAT(COALESCE(zpu."Zone", 'UNKNOWN'), ' / ', COALESCE(z."Zone", 'UNKNOWN')) as pudo
     , AVG(fare_amount) as mean_price
  FROM yellow_taxi_trips t
  JOIN zones z
       ON t."DOLocationID" = z."LocationID"
  JOIN zones zpu
       ON t."PULocationID" = zpu."LocationID"
 GROUP BY 1
 ORDER BY 2 DESC
