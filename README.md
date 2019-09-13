# sql_project
## Some sql project based on real-world dataset.


### 1.NYC Collision Patterns  
I use two datasets:  
a) NYPD Motor Vehicle Collisions (collision)  
I only use data from 2017, as the total size is too big. A downloaded copy of 2017 data can be downloaded [here](https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions-Crashes/h9gi-nx95).   

b) Population data for zip codes in New York State (census)  
The data can be downloaded at files list. (columns: rank, populaiton_density, zip, population)

Find hourly and monthly counts of collision using CUBE based OLAP query 
```sql
SELECT HOUR(time) AS hour1, 
       MONTH(date) AS month1, 
	   count(unique_key) AS collision
FROM cse532.collision
GROUP BY CUBE(HOUR(time), MONTH(date))
ORDER BY collision DESC;

-- From the result, 16 is the hour in the day that has the peak of collision(16973).
```

For zip codes with top 10 populations, find if any of them has collision count among top 10 counts
```sql
WITH zipcensusrnk AS 
(
  SELECT *, 
  RANK() OVER ( ORDER BY population DESC) rnk
  FROM cse532.zipcensus
  LIMIT 10
),
collisionrnk AS
(
   SELECT zip_code, count(unique_key) AS collision
   FROM cse532.collision
   GROUP BY zip_code
   ORDER BY collision DESC
   LIMIT 11  --Since the first row is total collisions, so use the first 11 row here.
)
SELECT zipcensusrnk.zip,zipcensusrnk.population,zipcensusrnk.rnk AS RNK,
CASE
     WHEN
      zipcensusrnk.zip = collisionrnk.zip_code
        THEN
          'True'
     ELSE
      'False'
  END AS colisionTOF
FROM zipcensusrnk
LEFT JOIN collisionrnk
ON zipcensusrnk.zip = collisionrnk.zip_code
ORDER BY RNK
;
```

For top 10 most dangerous locations (latitude, longitude) with highest collision counts, find their zip codes.  
```sql
WITH dangerregion AS
(  SELECT latitude, longitude,COUNT(unique_key) as collision
   FROM cse532.collision
   WHERE (latitude IS NOT NULL) AND (longitude IS NOT NULL)
   GROUP BY latitude,longitude
   ORDER BY COUNT(unique_key) DESC
),

location2zip AS
( SELECT DISTINCT latitude,longitude,zip_code
  FROM cse532.collision
  WHERE (latitude IS NOT NULL) AND (longitude IS NOT NULL) AND (zip_code IS NOT NULL) AND (latitude <>0) AND (longitude <>0)
)

SELECT dangerregion.latitude,dangerregion.longitude,location2zip.zip_code,dangerregion.collision as collision
FROM dangerregion
LEFT JOIN location2zip
ON dangerregion.latitude = location2zip.latitude AND dangerregion.longitude = location2zip.longitude
WHERE location2zip.zip_code IS NOT NULL
ORDER BY collision DESC
FETCH FIRST 10 ROWS ONLY;
```

### 2.Compute Histogram (Stored Procedure)  
A histogram is the probability distribution of a continuous variable, represented by frequencies of the variables falling into a bin. This histogram program takes an initial value (inclusive), an end value (exclusive), and the number of bins, e.g., histogram(start, end, number), and it returns a set of bins with bin number, frequency, bin's start value, and bin's end value, i.e., (binnum, frequency, binstart, binend).
```sql
DROP PROCEDURE gen_salary_histogram@

CREATE PROCEDURE gen_salary_histogram
  (IN start1 INT, IN end1 INT, IN num1 SMALLINT)
  LANGUAGE SQL
  BEGIN
  CREATE TABLE HISTOGRAM9 (binnum INTEGER, 
                           frequency INTEGER, 
                           binstart  INTEGER,
                           binend    INTEGER);
  
  INSERT INTO HISTOGRAM9 SELECT   s2.row1, COALESCE(cnt,0), 
                                  start1 + ((s2.row1-1) * (end1-start1)/num1),
                                  start1 + (s2.row1 * (end1-start1)/num1)
                           FROM((SELECT WIDTH_BUCKET(salary, start1, end1, num1) AS bucket, 
                                COUNT(*) AS cnt
                                FROM CSE532.employee
                                group by WIDTH_BUCKET(salary, start1, end1, num1)
                                ORDER by WIDTH_BUCKET(salary, start1, end1, num1))) AS s1
                           FULL OUTER JOIN (select ROW_NUMBER() OVER (ORDER BY empno) as row1
                                            from cse532.employee
                                            LIMIT num1) s2
                                       ON s2.row1 = s1.bucket
                           ORDER BY 1;            
  END@
```
Implement the same function with User Defined Function in PL/SQL
'''sql
/*
CREATE FUNCTION EMPLOYEESAL (start1 INT, end1 INT, num1 INT)
     RETURNS TABLE (binnum INTEGER, 
                    frequency INTEGER, 
                    binstart  INTEGER,
                    binend    INTEGER)
     LANGUAGE SQL
     READS SQL DATA
     NO EXTERNAL ACTION
     DETERMINISTIC

     RETURN
            select * from(
			SELECT  s2.row1, COALESCE(cnt,0), 
                    start1 + ((s2.row1-1) * (end1-start1)/num1),
                    start1 + (s2.row1 * (end1-start1)/num1)
            FROM((SELECT WIDTH_BUCKET(salary, start1, end1, num1) AS bucket, 
                    COUNT(*) AS cnt
                    FROM CSE532.employee
                    group by WIDTH_BUCKET(salary, start1, end1, num1)
                    ORDER by WIDTH_BUCKET(salary, start1, end1, num1))) AS s1
                    FULL OUTER JOIN (select ROW_NUMBER() OVER (ORDER BY cse532.employee.empno) as row1
                               from cse532.employee
                                ) AS s2
             ON s2.row1 = s1.bucket
             ORDER BY s2.row1)

*/

SELECT * FROM TABLE (EMPLOYEESAL(30000, 170000, 7)) LIMIT 7;
'''
### 3.Analysis of Starbucks in US with spatial queries  
Based on 2 spatial datasets: the first dataset is the list of Starbucks in US including location information (longitude, latitude), the second dataset is a shapefile that has a multipolygon object to represent the boundary of each county.   
Write spatial query to find some statistics of interest about those data, i.e., find which county has most Starbucks stores, find the nearest Starbucks to one location.  
Used Db2® Spatial Extender   
> find which county has most Starbucks stores
```sql
SELECT c.statefp AS statefp, c.countyfp AS countyfp, c.county_name AS county_name, count(h.store_number) as cnt
FROM cse532.starbucks AS h,cse532.counties AS c
WHERE db2gse.st_within(h.geolocation,c.shape) = 1 SELECTIVITY 0.0002
GROUP BY statefp, countyfp, county_name
ORDER BY cnt DESC
FETCH FIRST ROW ONLY;
```
> find the nearest Starbucks to one location
```sql
set current function path = current function path, db2gse;
WITH
  current_location(point) AS (
    VALUES(ST_Point(-73.1233890, 40.9123760, 1))
  )
SELECT
 s.location
,DECIMAL(ST_Distance(s.geolocation, point , 'STATUTE MILE'), 8, 2) AS distance
FROM cse532.starbucks s, current_location
WHERE
  ST_Intersects(s.geolocation,
    ST_Buffer(point, 30.0, 'STATUTE MILE')) = 1
  SELECTIVITY 0.0002
ORDER BY distance
FETCH FIRST 1 ROWS ONLY
;
```
