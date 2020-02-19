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
def truncate_date(df, city):
    if (city == City.VANCOUVER):
        trunc_df = pandas.to_datetime(df[['YEAR','MONTH','DAY']])
        print(trunc_df.head())
    elif (city == City.DENVER):
        trunc_df = pandas.to_datetime(df['FIRST_OCCURRENCE_DATE']).dt.date
        print(trunc_df.head())
    return trunc_df

# Method used to enrich date data to be uploaded to database
def enrich_date(enriched_df, df, city):    
    for idx, row in df.iterrows():
        # convert to date tuple, for useful info
        date_tuple = datetime.timetuple(row['Date'])
        # get holidays
        if(city == City.VANCOUVER):
            holiday_list = holidays.Canada()
        elif(city == City.DENVER):
            holiday_list = holidays.UnitedStates()

        new_row =   [
                        row['Date'],                                    # 'recorded_date'
                        row['Date'],                                    # 'full_date_description'
                        date_tuple.tm_wday,                             # 'day_of_week'
                        date_tuple.tm_yday,                             # 'day_number_in_epoch'
                        datetime.isocalendar(row['Date'])[1] ,          # 'week_number_in_epoch'  
                        date_tuple.tm_mon,                              # 'month_number_in_epoch'
                        date_tuple.tm_mday,                             # 'day_number_in_calendar_month'
                        date_tuple.tm_yday,                             # 'day_number_in_calendar_year'
                        date_tuple.tm_wday == 7,                        # 'last_day_in_week_indicator'
                        date_tuple.tm_mday == calendar.monthrange(date_tuple.tm_year,date_tuple.tm_mon)[1],     # 'last_day_in_month_indicator'
                        row + timedelta(days = (7 - date_tuple.tm_wday)),                                       # 'calendar_week_ending_date'
                        date_tuple.tm_yday // 7 + 1,                    # 'calendar_week_number_in_year'
                        date_tuple.tm_mon,                              # 'calendar_month_number_in_year'
                        calendar.month_name[date_tuple.tm_mon],         # 'calendar_month_name'
                        date_tuple.tm_mon,                              # 'calendar_year_month'
                        (date_tuple.tm_mon - 1) // 3 + 1,               # 'calendar_quarter'
                        (date_tuple.tm_mon - 1) // 3 + 1,               # 'calendar_year_quarter'
                        (date_tuple.tm_mon - 1) // 6 + 1,               # 'calendar_half_year'
                        date_tuple.tm_year,                             # 'calendar_year'
                        row['Date'] in holiday_list,                    # 'holiday_indicator'
                        holiday_list.get(row['Date']),                  # 'holiday_name'
                        1 < date_tuple.tm_wday < 6,                     # 'weekday_indicator'
                        datetime.now(),                                 # 'sql_date_stamp'
                        None                                            # 'major_event'
                    ]
        enriched_df.loc[len(enriched_df)] = new_row

    print(enriched_df.head())

    return enriched_df

def transform_date(dataframes):
    # create the dataframe
    enriched_df = pandas.DataFrame(columns=['recorded_date','full_date_description','day_of_week','day_number_in_epoch', 
                                            'week_number_in_epoch','month_number_in_epoch', 'day_number_in_calendar_month','day_number_in_calendar_year', 
                                            'last_day_in_week_indicator', 'last_day_in_month_indicator','calendar_week_ending_date',
                                            'calendar_week_number_in_year','calendar_month_number_in_year','calendar_month_name','calendar_year_month',
                                            'calendar_quarter','calendar_year_quarter','calendar_half_year','calendar_year','holiday_indicator',
                                            'holiday_name','weekday_indicator','sql_date_stamp', 'major_event'])
    
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

    van_data = pandas.DataFrame(truncate_date(dataframes['Vancouver'], City.VANCOUVER), columns=['Date'])

    # enrich dates
    enriched_van_data = enrich_date(enriched_df, van_data, City.VANCOUVER)
    enriched_van_data.to_csv('data/transformed_data/enriched_van_date_data.csv')
    
