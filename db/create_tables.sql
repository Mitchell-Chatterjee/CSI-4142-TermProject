SET search_path = "CSI4142";

-- DROP TABLE "CSI4142".crime CASCADE;
-- DROP TABLE "CSI4142".date CASCADE;
-- DROP TABLE "CSI4142".event CASCADE;
-- DROP TABLE "CSI4142".location CASCADE;
-- DROP TABLE "CSI4142".crime_fact;

-- The date dimension
CREATE TABLE DATE
(
    date_key integer PRIMARY KEY,
    recorded_date date,
    full_date_description date,
    day_of_week integer,
    day_number_in_epoch integer,
    week_number_in_epoch integer,
    month_number_in_epoch integer,
    day_number_in_calendar_month integer,
    day_number_in_calendar_year integer,
    last_day_in_week_indicator boolean,
    last_day_in_month_indicator boolean,
    calendar_week_ending_date integer,
    calendar_week_number_in_year integer,
    calendar_month_number_in_year integer,
    calendar_month_name character varying(40) COLLATE pg_catalog."default",
    calendar_year_month character varying(7) COLLATE pg_catalog."default",
    calendar_quarter integer,
    calendar_year_quarter integer,
    calendar_half_year integer,
    calendar_year integer,
    fiscal_week character varying(20) COLLATE pg_catalog."default",
    fiscal_week_number_in_year integer,
    fiscal_month integer,
    fiscal_month_number_in_year integer,
    fiscal_year_month integer,
    fiscal_quarter integer,
    fiscal_year_quarter integer,
    fiscal_half_year integer,
    fiscal_year integer,
    holiday_indicator boolean,
	holiday_name VARCHAR(40),
    weekday_indicator boolean,
    selling_season boolean,
    major_event boolean,
    sql_date_stamp date
);

-- The location dimension
CREATE TABLE LOCATION 
(
	Location_key INTEGER PRIMARY KEY,
	Location_name VARCHAR(120),
	GeoX DOUBLE PRECISION,
	GeoY DOUBLE PRECISION,
	Neighbourhood VARCHAR(30),
	City VARCHAR(30),
	Crime_rate DOUBLE PRECISION,
	Number_Of_crimes integer,
	Avg_household_income integer,
	Avg_prop_value integer,
	Num_of_precincts integer
);

-- The event dimension
CREATE TABLE EVENT
(
	Event_key INTEGER PRIMARY KEY,
	Event_name VARCHAR(20),
	Event_type VARCHAR(20),
	Event_location VARCHAR(20), 			-- This may be a reference to the location dimension, unsure
	Event_attendance INTEGER,			
	Venue_size DOUBLE PRECISION,			-- Need to have a unit here to define size	
	Alcohol_allowed BOOLEAN,
	Target_age_group INTEGER
);

-- The crime dimension
CREATE TABLE CRIME
(
	Crime_key INTEGER PRIMARY KEY,
	Crime_report_time TIME NOT NULL,
	Crime_start_time TIME,
	Crime_end_time TIME,
	Crime_details VARCHAR(200),
	Crime_type VARCHAR(20) NOT NULL,			-- The following two fields will be defined types
	Crime_category VARCHAR(20) NOT NULL,
	Crime_severity_index INTEGER NOT NULL
);

-- The main fact table
CREATE TABLE CRIME_FACT
(
	Date_key INTEGER NOT NULL,
	Location_key INTEGER NOT NULL,
	Crime_key INTEGER NOT NULL,
	Event_key INTEGER NOT NULL,
	Is_traffic BOOLEAN,
	Is_fatal BOOLEAN,
	Is_Nighttime BOOLEAN,
	
	FOREIGN KEY(Date_key)
		REFERENCES DATE(Date_key),
	FOREIGN KEY(Location_key)
		REFERENCES Location(Location_key),
	FOREIGN KEY(Crime_key)
		REFERENCES CRIME(Crime_key),
	FOREIGN KEY(Event_key)
		REFERENCES EVENT(Event_key),
	PRIMARY KEY (Date_key, Location_key, Crime_key, Event_key)
);