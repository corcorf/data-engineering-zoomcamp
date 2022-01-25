SELECT "Zone"
     , COUNT(*) as ntrips
  FROM yellow_taxi_trips t
  JOIN zones z
       ON t."DOLocationID" = z."LocationID"
 GROUP BY 1
 ORDER BY 2 DESC
