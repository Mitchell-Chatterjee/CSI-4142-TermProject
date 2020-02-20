import pandas
import os

def stage_event_data(df, event_data, name, type):
    # add to the event_data dataframe row by row of each dataframe
    for idx, row in df.iterrows():
        new_row =   [
                        len(event_data) + 1,                    # 'Event_key'
                        name,                                   # 'Event_name'
                        type,                                   # 'Event_type'
                        row['Location'],                        # 'City'
                        row['Attendance'],                      # 'Event_attendance'
                        row['Date'],                            # 'Event_date'
                        row['Venue_capacity'],                  # 'Venue_size'
                        row['Alcohol_allowed'] == 'Yes',        # 'Alcohol_allowed'
                        row['Venue_location'],                  # 'Event_Location'
                        row['Percentage_capacity']              # 'Percentage_capacity'
                    ]
        event_data.loc[len(event_data)] = new_row

def transform_event_data():
    # create the dataframe
    event_data = pandas.DataFrame(columns=['Event_key', 'Event_name', 'Event_type', 'City',
                                           'Event_attendance','Event_date', 'Venue_size', 'Alcohol_allowed', 
                                           'Event_Location', 'Percentage_capacity'])

    # read in all the sports data
    directory = '../data/event_data/sports'
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            # get the file
            df = pandas.read_csv(directory + '/'+ filename)
            # call the method to enrich and create the dataframe for each
            stage_event_data(df, event_data, filename.split('_')[0], directory.split('/')[-1])
    # update the date to datetime format
    event_data['Event_date'] = pandas.to_datetime(event_data['Event_date'])

    return event_data
