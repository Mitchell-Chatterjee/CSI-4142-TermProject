SET search_path = "public";

-- DROP TABLE special_event_dimension;
-- DROP TABLE time_dimension;

create table public.special_event_dimension(
    special_event_key int,
    category varchar(20),
    type varchar(20),
    name varchar(20),
    attendance int,
    venue_area float,
    attracts_tourists boolean,
    alcohol_allowed boolean,
    target_age_group varchar(20),
    primary key (special_event_key)
);

create table public.time_dimension(
    time_band_key int,
    time_band_sort_order varchar(20),
    band_range_name varchar(20),
    band_am_pm varchar(2),
    band_lower_value int,
    band_upper_value int,
    primary key (time_band_key)
);


-- These are the tables the professor specifically placed in her conceptual design.
SET search_path = "CSI4142";

-- The date dimension
CREATE TABLE DATE
(
    Date_key INTEGER PRIMARY KEY,
    Day_of_week INTEGER NOT NULL,
    Month_of_year INTEGER NOT NULL,
	Year_calendar INTEGER NOT NULL,
	Weekend BOOLEAN NOT NULL,
	Holiday BOOLEAN NOT NULL,
	Holiday_name VARCHAR(30)
);

-- The location dimension
CREATE TABLE LOCATION 
(
	Location_key INTEGER PRIMARY KEY,
	Location_name VARCHAR(30),
	Longitude DOUBLE PRECISION,
	Latitude DOUBLE PRECISION,
	Neighbourhood VARCHAR(30),
	-- TODO: Add neighbourhood statistics here
	City VARCHAR(30),
	Crime_rate DOUBLE PRECISION
);

-- The event dimension
CREATE TABLE EVENT
(
	Event_key INTEGER PRIMARY KEY,
	Event_name VARCHAR(30),
	Event_type VARCHAR(30),
	Event_location VARCHAR(30), 			-- This may be a reference to the location dimension, unsure
	Event_location_size DOUBLE PRECISION 	-- Need to have a unit here to define size
	-- TODO: Add any other attributes here
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