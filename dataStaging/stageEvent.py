import pandas

def stage_event_data(df, name, type):
    pass

def collect_event_data():
    # read in all the different event data
    baseball_data = pandas.read_csv("data/event_data/baseball_data.csv")
    football_data = pandas.read_csv("data/event_data/football_data.csv")
    hockey_data = pandas.read_csv("data/event_data/hockey_data.csv")
    soccer_data = pandas.read_csv("data/event_data/soccer_data.csv")
    

    # create the dataframe
    event_data = pandas.DataFrame(columns=['Event_key', 'Event_name', 'Event_type', 'Event_Location',
                                           'Event_attendance', 'Venue_size', 'Alcohol_allowed', 
                                           'Percentage_capacity']


collect_event_data()