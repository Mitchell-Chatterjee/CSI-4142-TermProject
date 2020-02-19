import pandas
import calendar
import holidays
from datetime import datetime, timedelta

# Method used to transform the data data
def transform_date(df, source):
    if(source):
        df = pandas.to_datetime(df[['YEAR','MONTH','DAY']])
    return df

# Method used to enrich date data to be uploaded to database
def enrich_date(df, source):
    enriched_df = pandas.DataFrame(columns=['recorded_date','full_date_description','day_of_week','day_number_in_epoch', 
                                            'week_number_in_epoch','month_number_in_epoch', 'day_number_in_calendar_month','day_number_in_calendar_year', 
                                            'last_day_in_week_indicator', 'last_day_in_month_indicator','calendar_week_ending_date',
                                            'calendar_week_number_in_year','calendar_month_number_in_year','calendar_month_name','calendar_year_month',
                                            'calendar_quarter','calendar_year_quarter','calendar_half_year','calendar_year','holiday_indicator',
                                            'holiday_name','weekday_indicator','sql_date_stamp'])
    
    for row in df:
        date_tuple = datetime.timetuple(row)
        if(source):
            holiday_list = holidays.Canada()
        new_row =   [
                        row,                                    # 'recorded_date'
                        row,                                    # 'full_date_description'
                        date_tuple.tm_wday,                     # 'day_of_week'
                        date_tuple.tm_yday,                     # 'day_number_in_epoch'
                        datetime.isocalendar(row)[1] ,          # 'week_number_in_epoch'  
                        date_tuple.tm_mon,                      # 'month_number_in_epoch'
                        date_tuple.tm_mday,                     # 'day_number_in_calendar_month'
                        date_tuple.tm_yday,                     # 'day_number_in_calendar_year'
                        date_tuple.tm_wday == 7,                # 'last_day_in_week_indicator'
                        date_tuple.tm_mday == calendar.monthrange(row.year,row.month)[1],           # 'last_day_in_month_indicator'
                        row + timedelta(days = (7 - date_tuple.tm_wday)),                           # 'calendar_week_ending_date'
                        date_tuple.tm_yday // 7 + 1,            # 'calendar_week_number_in_year'
                        date_tuple.tm_mon,                      # 'calendar_month_number_in_year'
                        calendar.month_name[date_tuple.tm_mon], # 'calendar_month_name'
                        date_tuple.tm_mon,                      # 'calendar_year_month'
                        (date_tuple.tm_mon - 1) // 3 + 1,       # 'calendar_quarter'
                        (date_tuple.tm_mon - 1) // 3 + 1,       # 'calendar_year_quarter'
                        (date_tuple.tm_mon - 1) // 6 + 1,       # 'calendar_half_year'
                        date_tuple.tm_year,                     # 'calendar_year'
                        row in holiday_list,                    # 'holiday_indicator'
                        holiday_list.get(row),                  # 'holiday_name'
                        1 < date_tuple.tm_wday < 6,             # 'weekday_indicator'
                        datetime.now()                          # 'sql_date_stamp'     
                    ]
        enriched_df.loc[len(enriched_df)] = new_row

    print(enriched_df.head())

    return enriched_df