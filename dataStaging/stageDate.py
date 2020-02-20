import pandas
import calendar
import holidays
import os
from enum import Enum
from datetime import datetime, timedelta

class City(Enum):
    VANCOUVER = 1
    DENVER = 2

# Returns only the date in datetime format
def truncate_date(df, city, event_data):
    # parse date data into a datetime format
    trunc_df = pandas.DataFrame(columns=['Date', 'Event_keys'])
    if (city == City.VANCOUVER):
        trunc_df['Date'] = pandas.to_datetime(df[['YEAR','MONTH','DAY']])
    elif (city == City.DENVER):
        trunc_df['Date'] = pandas.to_datetime(df['FIRST_OCCURRENCE_DATE']).dt.date

    # set a key before merging
    trunc_df['Date_key'] = trunc_df.index
    # merge with event data to get overlap between dates and events
    event_indices = pandas.merge(trunc_df, event_data[['Event_key', 'Event_date']], how='left', left_on='Date', right_on='Event_date')
    event_indices = event_indices.drop(columns=['Date', 'Event_date'])
    # group the event_keys into their respective 
    grp = event_indices.groupby(['Date_key'])

    # assign the event_key values to the date_keys
    trunc_df['Event_keys'] = grp['Event_key']
    print(trunc_df['Event_keys'])

    ## iterate over the groups and add the keys to the set of event keys for each date
    #for key, item in grp:
    #    trunc_df.at[item['Date_key'], 'Event_keys'] = item['Event_key']

    print(trunc_df)
    print("here")
    os.system("PAUSE")

    return trunc_df

# Method used to enrich date data to be uploaded to database
def enrich_date(enriched_df, df, city, event_data):
    #print(df.head())
    # get holidays
    if(city == City.VANCOUVER):
        holiday_list = holidays.Canada()
    elif(city == City.DENVER):
        holiday_list = holidays.UnitedStates()

    enriched_df['date_key'] = df.index + 1
    enriched_df['recorded_date'] = df['Date']
    enriched_df['full_date_description'] = df['Date']
    enriched_df['day_of_week'] = df['Date'].dt.dayofweek
    enriched_df['day_number_in_epoch'] = df['Date'].dt.dayofyear
    enriched_df['week_number_in_epoch'] = df['Date'].dt.weekofyear
    enriched_df['month_number_in_epoch'] = df['Date'].dt.month
    enriched_df['day_number_in_calendar_month'] = df['Date'].dt.day
    enriched_df['day_number_in_calendar_year'] = df['Date'].dt.dayofyear
    enriched_df['last_day_in_week_indicator'] = enriched_df['day_of_week'] == 6
    enriched_df['last_day_in_month_indicator'] == df['Date'].dt.is_month_end
    #enriched_df['calendar_week_ending_date'] = df['Date'] + timedelta(days = (7 - df['Date'].dt.dayofweek))
    enriched_df['calendar_week_number_in_year'] = df['Date'].dt.week
    enriched_df['calendar_month_number_in_year'] = df['Date'].dt.month
    #enriched_df['calendar_month_name'] = calendar.month_name[df['Date'].dt.month]
    enriched_df['calendar_year_month'] = df['Date'].dt.month
    enriched_df['calendar_quarter'] = df['Date'].dt.quarter
    enriched_df['calendar_year_quarter'] = df['Date'].dt.quarter
    enriched_df['calendar_half_year'] = df['Date'].dt.quarter // 3 + 1
    enriched_df['calendar_year'] = df['Date'].dt.year
    enriched_df['holiday_indicator'] = df['Date'].isin(holiday_list)
    #enriched_df['holiday_name'] = holiday_list.get(df['Date'])
    enriched_df['weekday_indicator'] = (df['Date'].dt.dayofweek % 6) != 0
    enriched_df['sql_date_stamp'] = datetime.now()
    enriched_df['major_event'] = df['Date'].isin(event_data['Event_date'])
    #enriched_df['event_keys'] = df['Date'].intersection(event_data['Event_date'])

    
    ## instead of going by row go by column
    #for idx, row in df.iterrows():
    #    # convert to date tuple, for useful info
    #    date_tuple = datetime.timetuple(row['Date'])
    #
    #    new_row =   [
    #                    len(enriched_df) + 1,                           # 'date_key'
    #                    row['Date'],                                    # 'recorded_date'
    #                    row['Date'],                                    # 'full_date_description'
    #                    date_tuple.tm_wday,                             # 'day_of_week'
    #                    date_tuple.tm_yday,                             # 'day_number_in_epoch'
    #                    datetime.isocalendar(row['Date'])[1] ,          # 'week_number_in_epoch'  
    #                    date_tuple.tm_mon,                              # 'month_number_in_epoch'
    #                    date_tuple.tm_mday,                             # 'day_number_in_calendar_month'
    #                    date_tuple.tm_yday,                             # 'day_number_in_calendar_year'
    #                    date_tuple.tm_wday == 7,                        # 'last_day_in_week_indicator'
    #                    date_tuple.tm_mday == calendar.monthrange(date_tuple.tm_year,date_tuple.tm_mon)[1],     # 'last_day_in_month_indicator'
    #                    row['Date'] + timedelta(days = (7 - date_tuple.tm_wday)),                                       # 'calendar_week_ending_date'
    #                    date_tuple.tm_yday // 7 + 1,                    # 'calendar_week_number_in_year'
    #                    date_tuple.tm_mon,                              # 'calendar_month_number_in_year'
    #                    calendar.month_name[date_tuple.tm_mon],         # 'calendar_month_name'
    #                    date_tuple.tm_mon,                              # 'calendar_year_month'
    #                    (date_tuple.tm_mon - 1) // 3 + 1,               # 'calendar_quarter'
    #                    (date_tuple.tm_mon - 1) // 3 + 1,               # 'calendar_year_quarter'
    #                    (date_tuple.tm_mon - 1) // 6 + 1,               # 'calendar_half_year'
    #                    date_tuple.tm_year,                             # 'calendar_year'
    #                    row['Date'].isin(holiday_list),                    # 'holiday_indicator'
    #                    holiday_list.get(row['Date']),                  # 'holiday_name'
    #                    1 < date_tuple.tm_wday < 6,                     # 'weekday_indicator'
    #                    datetime.now(),                                 # 'sql_date_stamp'
    #                    None                                            # 'major_event'
    #                ]
    #    enriched_df.loc[len(enriched_df)] = new_row

    print(enriched_df.head())

    return enriched_df

def transform_date(dataframes, event_data):
    # create the dataframe
    enriched_df = pandas.DataFrame(columns=['date_key', 'recorded_date','full_date_description','day_of_week','day_number_in_epoch', 
                                            'week_number_in_epoch','month_number_in_epoch', 'day_number_in_calendar_month','day_number_in_calendar_year', 
                                            'last_day_in_week_indicator', 'last_day_in_month_indicator','calendar_week_ending_date',
                                            'calendar_week_number_in_year','calendar_month_number_in_year','calendar_month_name','calendar_year_month',
                                            'calendar_quarter','calendar_year_quarter','calendar_half_year','calendar_year','holiday_indicator',
                                            'holiday_name', 'major_event', 'weekday_indicator','sql_date_stamp'])
    
    van_data = []
    denv_data = []

    # truncate dates to only include the main dates
    #if(os.path.isfile('data/transformed_data/transformed_van_date_data.csv')):
    #    print("Vancouver date data already exists. Reading.")
    #    van_data = pandas.read_csv('data/transformed_data/transformed_van_date_data.csv', parse_dates=[1])
    #else:
    #    van_data = pandas.DataFrame(truncate_date(dataframes['Vancouver'], City.VANCOUVER), columns=['Date'])
    #    van_data.to_csv('data/transformed_data/transformed_van_date_data.csv')

    #if(os.path.isfile('data/transformed_data/transformed_denv_date_data.csv')):
    #    print("Denver date data already exists. Reading.")
    #    denv_data = pandas.read_csv('data/transformed_data/transformed_denv_date_data.csv', parse_dates=[1])
    #else:
    #    denv_data = pandas.DataFrame(truncate_date(dataframes['Denver'], City.DENVER), columns=['Date'])
    #    denv_data.to_csv('data/transformed_data/transformed_denv_date_data.csv')

    # truncate the date and add an key column
    van_data = pandas.DataFrame(truncate_date(dataframes['Vancouver'], City.VANCOUVER, event_data))
    print("van: ", van_data)
    os.system("PAUSE")


    # enrich dates
    enriched_van_data = enrich_date(enriched_df, van_data, City.VANCOUVER, event_data)
    enriched_van_data.to_csv('data/transformed_data/enriched_van_date_data.csv')

    return van_data
    
