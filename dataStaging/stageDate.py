import pandas
import calendar
import holidays
import os
from enum import Enum
from datetime import datetime, timedelta
from pandas.tseries.offsets import *

class City(Enum):
    VANCOUVER = 1
    DENVER = 2

# Returns only the date in datetime format
def truncate_date(df, city, event_data, index):
    # parse date data into a datetime format
    trunc_df = pandas.DataFrame(columns=['Date', 'Event_keys'])
    if (city == City.VANCOUVER):
        trunc_df['Date'] = pandas.to_datetime(df[['YEAR','MONTH','DAY']])
    elif (city == City.DENVER):
        trunc_df['Date'] = pandas.to_datetime(df['FIRST_OCCURRENCE_DATE']).dt.date
        # must convert to date again after truncating
        trunc_df['Date'] = pandas.to_datetime(trunc_df['Date'])

    # set a key before merging
    trunc_df['Date_key'] = trunc_df.index + index

    # merge with event data to get overlap between dates and events
    event_indices = pandas.merge(trunc_df, event_data[['Event_key', 'Event_date']], how='left', left_on='Date', right_on='Event_date')
    event_indices = event_indices.drop(columns=['Date', 'Event_date'])
    # group the event_keys into their respective 
    grp = event_indices.groupby(['Date_key'])

    # assign the event_key values to the date_keys
    trunc_df['Event_keys'] = grp['Event_key']

    return trunc_df

# Method used to enrich date data to be uploaded to database
def enrich_date(df, city, event_data, index):
    # create the dataframe
    enriched_df = pandas.DataFrame(columns=['date_key', 'recorded_date','full_date_description','day_of_week','day_number_in_epoch', 
                                            'week_number_in_epoch','month_number_in_epoch', 'day_number_in_calendar_month','day_number_in_calendar_year', 
                                            'last_day_in_week_indicator', 'last_day_in_month_indicator','calendar_week_ending_date',
                                            'calendar_week_number_in_year','calendar_month_number_in_year','calendar_month_name','calendar_year_month',
                                            'calendar_quarter','calendar_year_quarter','calendar_half_year','calendar_year','holiday_indicator',
                                            'holiday_name', 'major_event', 'weekday_indicator','sql_date_stamp'])
    #print(df.head())
    # get holidays
    if(city == City.VANCOUVER):
        holiday_list = holidays.Canada()
    elif(city == City.DENVER):
        holiday_list = holidays.UnitedStates()

    enriched_df['date_key'] = df.index + index
    enriched_df['recorded_date'] = df['Date']
    enriched_df['full_date_description'] = df['Date']
    enriched_df['day_of_week'] = df['Date'].dt.dayofweek
    enriched_df['day_number_in_epoch'] = df['Date'].dt.dayofyear
    enriched_df['week_number_in_epoch'] = df['Date'].dt.weekofyear
    enriched_df['month_number_in_epoch'] = df['Date'].dt.month
    enriched_df['day_number_in_calendar_month'] = df['Date'].dt.day
    enriched_df['day_number_in_calendar_year'] = df['Date'].dt.dayofyear
    enriched_df['last_day_in_week_indicator'] = enriched_df['day_of_week'] == 6
    enriched_df['last_day_in_month_indicator'] = df['Date'].dt.is_month_end
    enriched_df['calendar_week_ending_date'] = df['Date'].where( df['Date'] == (( df['Date'] + Week(weekday=6) ) - Week()), df['Date'] + Week(weekday=6))
    enriched_df['calendar_week_number_in_year'] = df['Date'].dt.week
    enriched_df['calendar_month_number_in_year'] = df['Date'].dt.month
    enriched_df['calendar_month_name'] = df['Date'].dt.month_name()
    enriched_df['calendar_year_month'] = df['Date'].dt.month
    enriched_df['calendar_quarter'] = df['Date'].dt.quarter
    enriched_df['calendar_year_quarter'] = df['Date'].dt.quarter
    enriched_df['calendar_half_year'] = df['Date'].dt.quarter // 3 + 1
    enriched_df['calendar_year'] = df['Date'].dt.year
    enriched_df['holiday_indicator'] = df['Date'].isin(holiday_list)
    enriched_df['weekday_indicator'] = (df['Date'].dt.dayofweek % 6) != 0
    enriched_df['sql_date_stamp'] = datetime.now()
    enriched_df['major_event'] = df['Date'].isin(event_data['Event_date'])

    # must incrementally create the holiday_name row
    for i in range(len(enriched_df)):
        enriched_df.at[i, 'holiday_name'] = holiday_list.get((enriched_df.at[i, 'recorded_date']))
        if(enriched_df.at[i, 'holiday_name']):
            enriched_df.at[i, 'holiday_name'] = enriched_df.at[i, 'holiday_name'].replace(",", "")



    print(enriched_df.head())

    return enriched_df

def load_to_database(dbConn):
    try:
        print("Loading date data to database")
        with open('../data/transformed_data/all_enriched_date_data.csv', 'r') as f:
            next(f)
            dbConn.writeFiletoDB(f, 'date')
        print("Done loading to Database")
    except Exception as err:
        print("---------------")
        print("Exception: ", err)
        print("---------------")

def transform_date(dataframes, event_data, dbConn):


    # truncate and add events to vancouver data
    index = 1
    denv_data = pandas.DataFrame(truncate_date(dataframes['Denver'], City.DENVER, event_data, index))

    # truncate and add events to denver data
    index = len(denv_data) + 1
    van_data = pandas.DataFrame(truncate_date(dataframes['Vancouver'], City.VANCOUVER, event_data, index))

    # append the two dataframes for crime fact
    date_event_keys = denv_data.append(van_data, ignore_index=True)

    # enrich dates
    if(os.path.isfile('../data/transformed_data/all_enriched_date_data.csv')):
        all_enriched_date_data = pandas.read_csv('../data/transformed_data/all_enriched_date_data.csv',
                                   parse_dates = ['recorded_date', 'full_date_description'])
    else:
        # denver data transformation
        index = 1
        print("Enriching denver data")
        enriched_denv_data = enrich_date(denv_data, City.DENVER, event_data, index)
        print()
        # vancouver data transformation
        index = len(denv_data) + 1
        print("Enriching vancouver data")
        enriched_van_data = enrich_date(van_data, City.VANCOUVER, event_data, index)
        print()

        print(enriched_van_data.tail())
        print(enriched_denv_data.head())

        # compile enriched data
        all_enriched_date_data = enriched_denv_data.append(enriched_van_data, ignore_index=True)
        all_enriched_date_data.to_csv('../data/transformed_data/all_enriched_date_data.csv', index=False)

    # load to database
    load_to_database(dbConn)


    return date_event_keys
    
