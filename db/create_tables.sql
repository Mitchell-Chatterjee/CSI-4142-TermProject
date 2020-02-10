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