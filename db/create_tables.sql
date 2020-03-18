SET search_path = "public";

-- DROP TABLE "public".crime CASCADE;
-- DROP TABLE "public".date CASCADE;
-- DROP TABLE "public".event CASCADE;
-- DROP TABLE "public".location CASCADE;
-- DROP TABLE "public".crime_fact;

-- The date dimension
CREATE TABLE DATE
(
    date_key integer NOT NULL,
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
    calendar_week_ending_date date,
    calendar_week_number_in_year integer,
    calendar_month_number_in_year integer,
    calendar_month_name character varying(40) COLLATE pg_catalog."default",
    calendar_year_month integer,
    calendar_quarter integer,
    calendar_year_quarter integer,
    calendar_half_year integer,
    calendar_year integer,
    holiday_indicator boolean,
    holiday_name character varying(40) COLLATE pg_catalog."default",
    weekday_indicator boolean,
    major_event boolean,
    sql_date_stamp date,
    CONSTRAINT date_pkey PRIMARY KEY (date_key)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE date
    OWNER to mchat022;

GRANT ALL ON TABLE date TO mchat022;

GRANT ALL ON TABLE date TO rchan086;

GRANT ALL ON TABLE date TO dalga082;

-- The location dimension
CREATE TABLE LOCATION 
(
	Location_key INTEGER PRIMARY KEY,
	Location_name VARCHAR(120),
	GeoX DOUBLE PRECISION,
	GeoY DOUBLE PRECISION,
	Neighbourhood VARCHAR(30),
	Neighbourhood_type VARCHAR(30),
	City VARCHAR(30),
	Crime_rate DOUBLE PRECISION,
	Number_Of_crimes integer,
	Avg_household_income integer,
	Avg_prop_value integer,
	Num_of_precincts integer
);

GRANT ALL ON TABLE location TO mchat022;

GRANT ALL ON TABLE location TO rchan086;

GRANT ALL ON TABLE location TO dalga082;

-- The event dimension
CREATE TABLE EVENT
(
    event_key integer NOT NULL,
    event_name character varying(20) COLLATE pg_catalog."default",
    event_type character varying(20) COLLATE pg_catalog."default",
	city character varying(20) COLLATE pg_catalog."default",
	event_attendance integer,
	event_date date,
	venue_size double precision,
	alcohol_allowed boolean,
    event_location character varying(40) COLLATE pg_catalog."default",
    percentage_capacity double precision,
    CONSTRAINT event_pkey PRIMARY KEY (event_key)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE event
    OWNER to mchat022;

GRANT ALL ON TABLE event TO mchat022;

GRANT ALL ON TABLE event TO rchan086;

GRANT ALL ON TABLE event TO dalga082;

-- Must insert an empty event into the event table
INSERT INTO EVENT
VALUES
(0, 'NO EVENT', 'NO EVENT', 'NO EVENT', 0, '2020-1-1', 0, false, 'NO EVENT', 0);

-- The crime dimension
CREATE TABLE CRIME
(
	Crime_key INTEGER PRIMARY KEY,
	Crime_report_time TIME,
	Crime_start_time TIME,
	Crime_end_time TIME,
	Crime_type VARCHAR(35) NOT NULL,			-- The following two fields will be defined types
	Crime_category VARCHAR(20) NOT NULL,
	Crime_severity_index INTEGER NOT NULL
);

GRANT ALL ON TABLE crime TO mchat022;

GRANT ALL ON TABLE crime TO rchan086;

GRANT ALL ON TABLE crime TO dalga082;

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

GRANT ALL ON TABLE crime_fact TO mchat022;

GRANT ALL ON TABLE crime_fact TO rchan086;

GRANT ALL ON TABLE crime_fact TO dalga082;
