SET search_path = "public";
-- Drill Down

-- Drill Down 1
-- Number of crimes for neighbourhoods with "West" in the neighbourhood name and Number of crimes for neighbourhoods with "Central" in the neighbourhood name all in 2018
SELECT L.neighbourhood, F.is_fatal,D.calendar_year, COUNT(*) as number_of_crimes
FROM crime_fact as F,
     location as L,
     date as D
WHERE F.location_key = L.location_key
  and F.date_key = D.date_key
  and ((L.neighbourhood like '%West%' and D.calendar_year = 2018) or (L.neighbourhood like '%Central%' and D.calendar_year = 2018))
GROUP BY L.neighbourhood, F.is_fatal, D.calendar_year;

-- Drill Down 2
-- total number of crimes per holiday
SELECT d.holiday_name, COUNT(*) as number_of_crimes
FROM crime_fact as cf, date as d
WHERE cf.date_key = d.date_key AND d.holiday_name NOT LIKE ''
GROUP BY d.holiday_name;

-- Drill Down 3
-- Number of crimes that occurred in denver by categories
SELECT C.crime_category, COUNT(*)
FROM crime_fact as F natural join location as L natural join date as D natural join crime as C
WHERE D.calendar_month_name = 'December'
	AND D.calendar_year = 2018
    AND L.city = 'Denver'
GROUP BY C.crime_category;

-- Roll up

-- Roll up 1
-- total number of crimes per month, per year
SELECT d.calendar_year, d.calendar_month_name, COUNT(*) as number_of_crimes
FROM crime_fact as cf, date as d
WHERE cf.date_key = d.date_key
GROUP BY ROLLUP (d.calendar_month_name, d.calendar_month_number_in_year, d.calendar_year)
ORDER BY d.calendar_year, d.calendar_month_number_in_year;

-- Roll up 2
-- Count of crimes in each  neighbourhood, city, avg_houshold_income,crime_category
-- Count of crimes in each neighbourhood, city, avg_houshold_income.
-- Count of crimes in each neighbourhood and city
SELECT L.neighbourhood,L.city,L.avg_household_income,C.crime_Category,count(*) as numberOfCrimes
FROM crime_fact as F,
	 location as L,
	 crime as C
WHERE F.location_key = L.location_key AND
	  F.crime_key = C.crime_key
GROUP BY ROLLUP(L.neighbourhood,L.city,L.avg_household_income,C.crime_Category)
ORDER BY count(*) DESC;

-- Roll up 3
-- Number of crime in each neighbourhood and city at nighttime and daytime. 
-- Number of crimes in each neighbourhood and city. 
-- Number of crime in each neighbourhood
SELECT L.neighbourhood,L.city,F.is_nighttime,count(*) as numberOfCrimes
FROM crime_fact as F,
	 location as L,
	 crime as C
WHERE F.location_key = L.location_key AND
	  F.crime_key = C.crime_key
GROUP BY ROLLUP(L.neighbourhood,L.city,F.is_nighttime)
ORDER BY count(*) DESC;

-- Slice 

-- Slice 1
-- total number of each crime flag per event type, and all
SELECT 	e.event_name, 
		COUNT(*) FILTER (WHERE cf.is_fatal) as fatal_crimes, 
		COUNT(*) FILTER (WHERE cf.is_traffic) as traffic_crimes, 
		COUNT(*) FILTER (WHERE cf.is_nighttime) as night_time_crimes,
		COUNT(*) as all_crimes
FROM crime_fact as cf, event as e
WHERE cf.event_key = e.event_key
GROUP BY e.event_name
ORDER BY e.event_name;

-- Slice 2
-- Avg number of Crimes in areas of the city where the avg property value is between 600000 and 1000000 
SELECT L.city,L.avg_prop_value,cast(CAST(AVG(L.number_of_crimes) as decimal(18,5)) as float) as avgNumberOfCrimes
FROM location as L
WHERE L.location_name <> 'UNKNOWN' AND
	  L.location_name <> 'OFFSET TO PROTECT PRIVACY' AND
	  L.avg_prop_value between 600000 and 1000000
GROUP BY L.city,L.avg_prop_value;

-- Slice 3
-- Avg number of Crimes in areas of the city where the household income is below 50000
SELECT L.city,L.avg_household_income,cast(CAST(AVG(L.number_of_crimes) as decimal(18,5)) as float) as numberOfCrimes
FROM location as L
WHERE L.location_name <> 'UNKNOWN' AND
	  L.location_name <> 'OFFSET TO PROTECT PRIVACY' AND
	  L.avg_household_income < 50000
GROUP BY L.city,L.avg_household_income;

-- Dice

-- Dice 1
-- Look at number of crimes in denver during 2017 by months and categories
SELECT C.crime_category, D.calendar_month_name, COUNT(*) 
FROM crime_fact as F natural join location as L natural join date as D natural join crime as C
WHERE L.city = 'Denver'
	AND D.calendar_year = 2017
GROUP BY C.crime_category, D.calendar_month_name;

-- Dice 2
-- The Crime type with the highest amount of crimes in the Central area of Denver
SELECT L.neighbourhood, C.crime_type, L.city, COUNT(*) as number_of_crimes
FROM crime_fact as F,
     location as L,
     crime as C
WHERE F.location_key = L.location_key AND
	  F.crime_key = C.crime_key AND
      L.neighbourhood like '%Central%' 
	  AND L.city = 'Denver'
GROUP BY L.neighbourhood,C.crime_type,L.city
ORDER BY number_of_crimes DESC
limit 1;

-- Dice 3
-- The Crime type with the highest amount of crimes in the Central area of Vancouver
SELECT L.neighbourhood, C.crime_type, L.city, COUNT(*) as number_of_crimes
FROM crime_fact as F,
     location as L,
     crime as C
WHERE F.location_key = L.location_key AND
	  F.crime_key = C.crime_key AND
      L.neighbourhood like '%Central%' 
	  AND L.city = 'Vancouver'
GROUP BY L.neighbourhood,C.crime_type,L.city
ORDER BY number_of_crimes DESC
limit 1;

-- Combination

-- Combination 1
-- total number of crimes per event type, per month, comparing each city (excluding baseball)
SELECT e.event_name, d.calendar_month_name, e.city, COUNT(*) as number_of_crimes
FROM crime_fact as cf, event as e, date as d
WHERE 	cf.date_key = d.date_key AND cf.event_key = e.event_key 
		AND e.event_name NOT LIKE 'baseball'
		AND e.event_name NOT LIKE 'NO EVENT'
GROUP BY e.city, d.calendar_month_number_in_year, d.calendar_month_name, e.event_name
ORDER BY e.event_name, d.calendar_month_number_in_year, e.city;

-- Combination 2
-- Count of crimes in each urban neighbourhood in both cities on December day.
SELECT L.neighbourhood, L.city, COUNT(*) as number_of_crimes
FROM crime_fact as F, location as L, date as D
WHERE F.date_key = D.date_key 
     AND F.location_key = L.location_key 
	 AND L.neighbourhood_type = 'urban'
	 AND D.holiday_name = 'Christmas Day'
GROUP BY L.neighbourhood,L.city
ORDER BY number_of_crimes DESC;

-- Iceberg

-- Iceberg 1
-- The 4 highest occuring crime categories in denver in 2018
SELECT C.crime_category, COUNT(*) as total
FROM crime_fact as F natural join location as L natural join date as D natural join crime as C
WHERE D.calendar_year = 2018
    AND L.city = 'Denver'
GROUP BY C.crime_category
ORDER BY total DESC
LIMIT 4;

-- Iceberg 2
-- Top 10 most common crimes and the days of the year on which they occur
SELECT d.day_number_in_calendar_year, cr.crime_category, COUNT(*) as  occurence
FROM crime_fact as cf, date as d, crime as cr
WHERE cf.date_key = d.date_key AND cf.crime_key = cr.crime_key
GROUP BY d.day_number_in_calendar_year, cr.crime_category
ORDER BY occurence DESC
LIMIT 10;

-- Window Function

-- Window Function 1
-- Compare the average CSI by month in Denver in 2018 at nighttime
SELECT D.calendar_year, D.calendar_month_name, C.crime_severity_index, AVG(C.crime_severity_index) OVER (PARTITION BY D.calendar_month_name) as avg_csi_by_month
FROM crime_fact as F natural join location as L natural join date as D natural join crime as C
WHERE D.calendar_year = 2018
    AND L.city = 'Denver'
	AND F.is_nighttime = true;

-- Window Function 2
-- Compare the average CSI by neighborhood in Denver in January at nighttime on weekends
SELECT L.neighbourhood, C.crime_severity_index, AVG(C.crime_severity_index) OVER (PARTITION BY L.neighbourhood) as avg_csi_by_neighbourhood
FROM crime_fact as F natural join location as L natural join date as D natural join crime as C
WHERE D.calendar_year = 2018
    AND L.city = 'Denver'
	AND D.calendar_month_name = 'January'
	AND D.weekday_indicator = 'false'
	AND F.is_nighttime = true;

-- Window Clause

-- Window Clause 1
-- Compute the average crime_rate by Month in 2018 and show the num_of_precincts and crime_rate
SELECT D.calendar_month_name, L.neighbourhood, L.city, L.num_of_precincts, L.crime_rate, AVG(L.crime_rate) OVER m as avg_crim_rate_by_month
FROM crime_fact as F natural join location as L natural join date as D natural join crime as C 
WHERE D.calendar_year = 2018
WINDOW m AS (PARTITION BY D.calendar_month_name);

-- Window Clause 2
-- Compute the average crime_rate by City in October of 2018
SELECT L.neighbourhood, L.city, L.avg_prop_value, L.crime_rate, AVG(L.crime_rate) OVER city as avg_crim_rate_by_city
FROM crime_fact as F natural join location as L natural join date as D natural join crime as C
WHERE D.calendar_year = 2018
	AND D.calendar_month_name = 'October'
WINDOW city AS (PARTITION BY L.city);

