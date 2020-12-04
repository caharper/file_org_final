import dask.dataframe as dd
import dask
import numpy as np
from pathlib import Path
import os
import math
import pandas as pd
import glob
from shutil import copyfile
from datetime import datetime, timedelta
from bisect import bisect
from copy import deepcopy
import sys

class Driver():
    '''Driver class for checking assignment constraints.
    '''
    def __init__(self, hometown):
        # tuples with (src, dest, start time, finish time) of busy blocks
        self.day_busy_times = []
        days = ['M', 'T', 'W', 'U', 'F', 'S', 's']
        self.day_to_int = {day: i for i, day in enumerate(days)}
        self.day_to_day_start_min = {day: 24*i*60 for i, day in enumerate(days)}
        
        self.home_city, self.home_state = hometown
        self.hometown = hometown
        
    def _get_drive_and_break(self, start_day, start_hr, start_min, drive_hr, drive_min, dest_city,
                            dest_state):
        
        # cast all to ints
        start_hr, start_min = int(start_hr), int(start_min)
        drive_hr, drive_min = int(drive_hr), int(drive_min)
        
        
        drive_time_in_mins = 60*drive_hr + drive_min
        break_in_mins = math.ceil(drive_time_in_mins/2)
        
        # check if hometown is destination
        if (dest_city, dest_state) == self.hometown:
            # take the max of the break and 18hrs
            break_in_mins = max(break_in_mins, 18*60)
        
        busy_time_in_mins = drive_time_in_mins + break_in_mins
        
        start_time = self.day_to_day_start_min[start_day] + 60*start_hr + start_min
        end_time = (start_time + busy_time_in_mins)%(24*7*60)
        
        return start_time, end_time
    
    def _overlap_times(self, busy_start, busy_end, start_time, end_time):
        '''Returns True if the drive overlaps with a busy block of time'''
        
        # if start drive in the middle of a busy block
        if busy_start <= start_time <= busy_end:
            return True
        # if end of break is in the middle of a busy block
        if busy_start <= end_time <= busy_end:
            return True
        return False
        

    # now need to check if the insertion will cause issues for getting to source cities
    def _check_busy(self, start_day, start_hr, start_min, drive_hr, drive_min, dest_city,
                    dest_state, src_city, src_state):
        
        # get the time block that we want to allocate for the drive and break
        start_time, end_time = self._get_drive_and_break(start_day, start_hr, 
                                                         start_min, drive_hr, drive_min,
                                                        dest_city, dest_state)
        
        busy = False 
        
        # Sort busy blocks
        if len(self.day_busy_times) > 0:
            self.day_busy_times = sorted(self.day_busy_times, key=lambda x: x[2])
        
        # Check if there is overlap with times (can be more efficient since I am sorting drives)
        for busy_src, busy_dest, busy_start, busy_end in self.day_busy_times:
            
            # check if there is overlap with the times
            if self._overlap_times(busy_start, busy_end, start_time, end_time):
                busy = True
                break
                
        return busy

    def _are_48hr_apart(self, route_1, route_2):
        
        # need to check if negative (meaning check rollover into next week)
        if route_1[2] > route_2[2]:
            # make sure the end of route 2 is at least 48hr from start of route 1
            time_apart = (route_2[2] - route_1[3])%(24*7*60)
        else:
            # route_2 start minus route_1 end 
            time_apart = route_2[2] - route_1[3]
        
        # if at least 48hr apart 
        return time_apart >= 48*60
    
    def _dest_matches_src(self, route_1, route_2):
        route_1_dest = route_1[1]
        route_2_src = route_2[0]
        
        return route_1_dest == route_2_src
    
    def _is_valid_schedule(self, schedule, inserted_last=False):
        
        valids = []
        
        # If nothing to the left or right
        if len(schedule) == 1:
            return True # not necessary since there is a check in _valid_src_dest
        
        if len(schedule) == 2:
            # Possible inserted last or only 1 element is in day_busy_times
            if inserted_last:
                # Check if left and right blocks are possible
                if not(self._dest_matches_src(schedule[0], schedule[1]) or self._are_48hr_apart(schedule[0], 
                                                                                       schedule[1])):
                    return False
                
                # Check that rollover into next week isn't an issue
                if not(self._dest_matches_src(schedule[-1], self.day_busy_times[0]) 
                       or self._are_48hr_apart(schedule[-1], self.day_busy_times[0])):
                    return False
                
                
            else:
                # Check if left and right blocks are possible
                if not(self._dest_matches_src(schedule[0], schedule[1]) or self._are_48hr_apart(schedule[0], 
                                                                                       schedule[1])):
                    return False
                
                # Check if right doesn't cause issue with left
                if not (self._dest_matches_src(schedule[1], schedule[0]) or self._are_48hr_apart(schedule[1], 
                                                                                       schedule[0])):
                    return False
        
        if len(schedule) == 3:
        
            # Check left and middle
            if not (self._dest_matches_src(schedule[0], schedule[1]) or self._are_48hr_apart(schedule[0], 
                                                                                       schedule[1])):
                return False
        
            # Check middle and right
            if not (self._dest_matches_src(schedule[1], schedule[2]) or self._are_48hr_apart(schedule[1], 
                                                                                       schedule[2])):
                return False
          
        # If all are valid, then schedule is valid
        return True
        
    def _valid_src_dest(self, start_day, start_hr, start_min, drive_hr, drive_min, dest_city, dest_state,
                     src_city, src_state):
        
        ''' Checks if source and destination cities match up or are at least 48hr between assignments.
        '''
        
        # if no busy times added, route is ok
        if len(self.day_busy_times) == 0:
            return True
        
        src = (src_city, src_state)
        dest = (dest_city, dest_state)
        
        start_time, end_time = self._get_drive_and_break(start_day, start_hr, 
                                                         start_min, drive_hr, drive_min,
                                                        dest_city, dest_state)
        
        in_question = (src, dest, start_time, end_time)
                                                         
        # assumes day_busy_times is sorted on start time 
        # assumes is not busy
        left_time = None
        right_time = None
        
        # If only one element in blocked times, left and right times are the same 
        if len(self.day_busy_times) == 1:
            return self._is_valid_schedule([in_question, self.day_busy_times[0]])
        
        _, _, busy_start_times, _ = zip(*self.day_busy_times)
        busy_start_times = list(busy_start_times)
        
        # find left and right blocks of where we want to insert the drive (assumes not busy)
        insert_index = bisect(busy_start_times, start_time)
        
        
        # If inserting at the end
        if insert_index == len(self.day_busy_times):
            return self._is_valid_schedule([self.day_busy_times[insert_index-1], in_question],
                                           inserted_last=True)
            
        # If inserting in the middle of two allocated blocks
        left_time = self.day_busy_times[insert_index-1] 
        right_time = self.day_busy_times[insert_index] # block that currently occupies slot
        
        return self._is_valid_schedule([left_time, in_question, right_time])
        
    def is_availiable(self, start_day, start_hr, start_min, drive_hr, drive_min, dest_city, dest_state,
                     src_city, src_state):
        '''Checks if a drier is available for the timeslot'''
        
        # check if the drive overlaps with any busy blocks
        overlap_busy = self._check_busy(start_day, start_hr, start_min, drive_hr, drive_min,
                                        dest_city, dest_state, src_city, src_state)
        
        if overlap_busy == True:
            return False
        
        # check if the src and dest cities are ok or if 48hr apart
        valid_insert = self._valid_src_dest(start_day, start_hr, start_min, drive_hr, drive_min, 
                                            dest_city, dest_state, src_city, src_state)
        
        # no overlap and valid_insert equate to true
        return valid_insert
        
        
    def add_busy_time(self, start_day, start_hr, start_min, drive_hr, drive_min, dest_city, dest_state,
                     src_city, src_state):
        ''' Adds a datetime tuple (start time and end time) to day_busy_times
            Note: automatically adds in break times.
            
            Assumes that is_busy has returned False
        '''
        
        src = (src_city, src_state)
        dest = (dest_city, dest_state)
        
        # get the time block that we want to allocate for the drive and break
        start_time, end_time = self._get_drive_and_break(start_day, start_hr, 
                                                         start_min, drive_hr, drive_min,
                                                        dest_city, dest_state)
        
        # if wrapping around weekend
        if end_time < start_time:
            self.day_busy_times.append((src, dest, 0, end_time))
            end_time = (24*7*60)-1 # end of week and 0 indexed
            
        # add busy block
        self.day_busy_times.append((src, dest, start_time, end_time))
        
        
class DaskReader():
    ''' Base class for reading csv files with Dask.  

    '''
    def __init__(self, csv_path):
        # Verify that the path extension is .csv
        self._verify_csv_format(csv_path)
        self.csv_path = csv_path
        
        self.state_codes = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT",
                            "DC", "DE", "FL", "GA", "HI", "ID", "IL", 
                            "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
                            "MA", "MI", "MN", "MS", "MO", "MT", "NE",
                            "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
                            "OH", "OK", "OR", "PA", "RI", "SC", "SD", 
                            "TN", "TX", "UT", "VT", "VA", "WA", "WV", 
                            "WI", "WY"]
        
    def _verify_csv_format(self, file):
        if Path(file).suffix == '.csv':
            return True
        return False

    def _is_nan(self, x):
        if x != x or x is None:
            return True
        return False
    
    def _verify_str_len(self, x, min_len, max_len):
        if self._is_nan(x):
            return False
        
        if len(x) < min_len or len(x) > max_len:
            return False
        
        return True
    
    def _verify_id(self, x):
        if self._is_nan(x):
            return False
        
        if not x.isalnum() or len(x) != 5:
            return False
        
        return True
    
    def _verify_int_value(self, x, min_x, max_x):
        # make sure is an int
        if not x.isdigit():
            return False
        
        int_x = int(x)
        if int_x < min_x or int_x > max_x:
            return False
        return True
    
    def _is_empty(self, df):
        if len(df.index) == 0:
            return True
        return False
    
    def _delete_files(self, file_list):
        for file in file_list:
            os.remove(file)
                
    def _read_df(self, file_type, names=None):
        '''Get a dask dataframe from the file
        
        Parameters
        ----------
        names : list of str names for columns in a csv file.
            This assumes that the correct amount of names is passed into this 
            function so that it matches up with the csv file. This also assumes
            that the csv files do not have a header with column names initially.
            By saving as a group of parquet files, we keep the operations from
            causing memory issues.
        '''
        df = dd.read_csv(self.csv_path, header=None, dtype='str', names=names)
        df.repartition(partition_size="100MB")
        
        # save as parquet files
        parquet_path = './parquet_processing/' + file_type + '/'
        df.to_parquet(parquet_path)
        return dd.read_parquet(parquet_path)

    def _concat_files(self, files, dest):
            
        # only append to avoid memory issues
        with open(dest, 'a') as f:
            for csvFile in files:
                for line in open(csvFile, 'r'):
                    f.write(line)
    
        
class DriverReader(DaskReader):
    ''' Reads driver csv files.
    
    Expected Columns
    ----------------
    driver_id (unique)
    last_name
    first_name
    age (years)
    home_city
    home_state(standard US state code, 2 characters)
    
    It is assumed that the company only hires drivers that are from a city
    where a bus goes to.  No need to verify that there is a route to the 
    home city based on the assumption given in the assignment.
    
    "The company only hire drivers from cities where there is a route that 
    serves as its destination (destination ONLY, not departure city)."
    '''
    
    def __init__(self, csv_path, dest_cities):
        super().__init__(csv_path)
        
        self.column_names = ['driver_id', 'last_name', 'first_name', 'age', 'home_city',
                            'home_state']
        
        self.dest_cities = dest_cities
        
        self.df = self.read_df()
        
        # delete existing intermediate files
        self._delete_files(glob.glob('./intermediate_csvs/drivers/*.csv'))
        
    def read_df(self):
        return self._read_df(file_type='drivers', names=self.column_names)
    
    def verify_df(self):
        self._verify_attributes()
        
    def _verify_dest_city(self, city, state):
        # Make sure the hometown is a destination city
        if (city, state) in self.dest_cities:
            return True
        return False
        
    def _check_duplicates_by(self, column_name):
        # adapted from: https://github.com/dask/dask/issues/2952
        self.df = self.df.reset_index().drop_duplicates([column_name]).set_index("index")
        
    def to_csv(self):
        save_root = './intermediate_csvs/drivers/clean_drivers-*.csv'
        
        # Save to csv (for each partition)
        self.df.to_csv(save_root, header=None, index=None)
        
        # Concatenate csvs
        concat_files = '/'.join(save_root.split('/')[:-1]) + '/*.csv'
        concat_files = glob.glob(concat_files)
        
        # Create empty csv
        dest = './processed_csvs/drivers/drivers.csv'
        with open(dest, 'w') as empty_csv:
            pass
        
        # if multiple files, concatenate
        if len(concat_files) > 1:
            self._concat_files(concat_files, dest)
        else:
            # copy full file over to the destination
            copyfile(concat_files[0], dest)
    
    def _verify_attributes(self):
        '''Verifies basic attributes in the table. 
        
        We verify that each of the attributes follows the datastructures we are placing on them,
        and we may want to add functionality to drop any rows that are duplicates.  Note: we 
        must check if the dataframe is empty so that we avoid KeyErrors.
        '''
        
        if self._is_empty(self.df):
            return self.df
        
        # driver_id must be 5 characters and alphanumeric
        self.df = self.df[self.df['driver_id'].apply(
            lambda x: self._verify_id(x), meta=pd.Series([], dtype='str', name='x'))]

        if self._is_empty(self.df):
            return self.df
        
        # last_name, first_name are a max of 80 characters
        self.df = self.df[self.df['last_name'].apply(
            lambda x: self._verify_str_len(x, 1, 80), meta=pd.Series([], dtype='str', name='x'))]

        if self._is_empty(self.df):
            return self.df
        
        self.df = self.df[self.df['first_name'].apply(
            lambda x: self._verify_str_len(x, 1, 80), meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # age is up to 3 characters and is an integer from 16-100
        self.df = self.df[self.df['age'].apply(
            lambda x: self._verify_str_len(x, 2, 3) and self._verify_int_value(x, 16, 100),
            meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # home_city is a max of 80 characters
        self.df = self.df[self.df['home_city'].apply(
            lambda x: self._verify_str_len(x, 1, 80), meta=pd.Series([], dtype='str', name='x'))]

        if self._is_empty(self.df):
            return self.df
        
        # home_state must be 2 characters and valid state code
        self.df = self.df[self.df['home_state'].apply(
            lambda x: self._verify_str_len(x, 2, 2) and x in self.state_codes,
            meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # (home_city, home_state) must be a destination city
        self.df = self.df[self.df[['home_city', 'home_state']].apply(
            lambda x: self._verify_dest_city(*x), meta=pd.Series([], dtype='str', name='x'), axis=1)]
        
        if self._is_empty(self.df):
            return self.df
        
        # remove duplicate IDs
        self._check_duplicates_by('driver_id')
        

class RouteReader(DaskReader):
    ''' Reads route csv files.
    
    Expected Columns
    ----------------
    Route number
    Route name (left empty if not present)
    Departure city name
    Departure city code (standard US state code, 2 characters)
    Destination city name
    Destination city code
    Route type code (0 for daily, 1 for weekdays only, 2for weekend only)
    Departure time (hours)
    Departure time (minutes)
    Travel time (hours)
    Travel time (minutes)
    
    I renamed them below so it is harder to accidentially call on the wrong
    name (i.e. departure is now src)
    
    Helpful link: https://stackoverflow.com/questions/47125665/simple-dask-map-partitions-example
    '''
    def __init__(self, csv_path):
        super().__init__(csv_path)
        
        self.column_names = ['route_id', 'route_name', 'src_city_name', 'src_state_code',
                            'dest_city_name', 'dest_state_code', 'route_type', 'dep_time_hr',
                             'dep_time_min', 'travel_time_hr', 'travel_time_min']
        
        self.df = self.read_df()
        
        # delete existing files
        self._delete_files(glob.glob('./intermediate_csvs/routes/*.csv'))
        
    def read_df(self):
        return self._read_df(file_type='routes', names=self.column_names)
    
    def verify_df(self):
        self._verify_attributes()
    
    def _verify_time_less_than(self, hr, min_, max_minutes):
        hr, min_ = int(hr), int(min_)
        
        if (hr*60) + min_ > max_minutes:
            return False
        return True
    
    def _check_duplicates_by(self, column_name):
        # adapted from: https://github.com/dask/dask/issues/2952
        self.df = self.df.reset_index().drop_duplicates([column_name]).set_index("index")
    
    def _verify_attributes(self):
        '''Verifies basic attributes in the table. 
        
        We verify that each of the attributes follows the datastructures we are placing on them,
        and we may want to add functionality to drop any rows that are duplicates.  Note: we 
        must check if the dataframe is empty so that we avoid KeyErrors.
        '''
        
        if self._is_empty(self.df):
            return self.df
        
        # route_ID must be 5 characters and alphanumeric
        # this could probably be improved, but I am still learning dask 
        self.df = self.df[self.df['route_id'].apply(
            lambda x: self._verify_id(x), meta=pd.Series([], dtype='str', name='x'))]

        if self._is_empty(self.df):
            return self.df

        # route_name is optional but a max of 80 characters
        self.df = self.df[self.df['route_name'].apply(
            lambda x: self._is_nan(x) or self._verify_str_len(x, 0, 80),
            meta=pd.Series([], dtype='str', name='x'))]

        if self._is_empty(self.df):
                return self.df
        
        # src_city_name, dest_city_name are a max of 80 characters
        self.df = self.df[self.df['src_city_name'].apply(
            lambda x: self._verify_str_len(x, 1, 80), meta=pd.Series([], dtype='str', name='x'))]

        if self._is_empty(self.df):
            return self.df
        
        self.df = self.df[self.df['dest_city_name'].apply(
            lambda x: self._verify_str_len(x, 1, 80), meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
            
        # route_type is 1 character and can be the integers 0, 1, or 2
        self.df = self.df[self.df['route_type'].apply(
            lambda x: self._verify_str_len(x, 1, 1) and x in ['0', '1', '2'],
            meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # dep_time_hr is up to 2 characters and is an integer from 0-23
        self.df = self.df[self.df['dep_time_hr'].apply(
            lambda x: self._verify_str_len(x, 1, 2) and self._verify_int_value(x, 0, 23),
            meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # travel_time_hr is up to 2 characters and is an integer from 0-72
        self.df = self.df[self.df['travel_time_hr'].apply(
            lambda x: self._verify_str_len(x, 1, 2) and self._verify_int_value(x, 0, 72),
            meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # travel_time_min is up to 2 characters and is an integer from 0-59
        self.df = self.df[self.df['travel_time_min'].apply(
            lambda x: self._verify_str_len(x, 1, 2) and self._verify_int_value(x, 0, 59),
            meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # total travel time must not exceed 72 hrs
        self.df = self.df[self.df[['travel_time_hr', 'travel_time_min']].apply(
            lambda x: self._verify_time_less_than(*x, 72*60),
            meta=pd.Series([], dtype='str', name='x'), axis=1)]
        
        if self._is_empty(self.df):
            return self.df
        
        # src_state_code and dest_state_code must be 2 characters and valid state codes
        self.df = self.df[self.df['src_state_code'].apply(
            lambda x: self._verify_str_len(x, 2, 2) and x in self.state_codes,
            meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        self.df = self.df[self.df['dest_state_code'].apply(
            lambda x: self._verify_str_len(x, 2, 2) and x in self.state_codes,
            meta=pd.Series([], dtype='str', name='x'))]
        
        # remove duplicate IDs
        self._check_duplicates_by('route_id')
        
    def get_dest_city_names(self):
        '''Returns a list with destination city names (unique with state)'''
        cities_df = self.df[['dest_city_name', 'dest_state_code']]
        cities_df = cities_df.reset_index().drop_duplicates(['dest_city_name', 'dest_state_code']).set_index("index")
        return list(cities_df.compute().itertuples(index=False, name=None))
    
    def to_csv(self):
        save_root = './intermediate_csvs/routes/clean_routes-*.csv'
        
        # Save to csv (for each partition)
        self.df.to_csv(save_root, header=None, index=None)
        
        # Concatenate csvs
        concat_files = '/'.join(save_root.split('/')[:-1]) + '/*.csv'
        concat_files = glob.glob(concat_files)
        
        # Create empty csv
        dest = './processed_csvs/routes/routes.csv'
        with open(dest, 'w') as empty_csv:
            pass
        
        # if multiple files, concatenate
        if len(concat_files) > 1:
            self._concat_files(concat_files, dest)
        else:
            # copy full file over to the destination
            copyfile(concat_files[0], dest)
    
class AssignmentReader(DaskReader):
    '''Reads assignment csv files'''
    def __init__(self, csv_path, driver_ddf, route_ddf):
        super().__init__(csv_path)
        
        self.column_names = ['driver_id', 'route_id', 'day_of_week']
        
        self.valid_days = ['M', 'T', 'W', 'U', 'F', 'S', 's']
        
        self.df = self.read_df()
        
        self.driver_ddf = driver_ddf
        self.route_ddf = route_ddf
        
    def read_df(self):
        return self._read_df(file_type='assignments', names=self.column_names)
    
    def verify_df(self):
        self._verify_attributes()
        
    def _check_if_hometown_route_exists(self, home_city, home_state, route_ids):
        hometown_indexes = []
        remove_indexes = []
        
        for index, route_id in route_ids.items():
            # get the route destination 
            route_df = self.route_ddf[self.route_ddf['route_id'] == route_id]
            route_df = route_df[['dest_city_name', 'dest_state_code']].compute()
        
            # if the city does not exist, go on to the next and make note of index to remove
            if len(route_df) == 0:
                remove_indexes.append(index)
                continue
        
            dest_city, dest_state = list(route_df.itertuples(index=False, name=None))[0]
            
            # found hometown route and note the index 
            if (dest_city, dest_state) == (home_city, home_state):
                hometown_indexes.append(index)
                
        return hometown_indexes, remove_indexes
    
    def _get_route_info(self, route_id):
        route_df = self.route_ddf[self.route_ddf['route_id'] == route_id].compute()
        route_df = route_df.iloc[0]

        start_hr, start_min = route_df['dep_time_hr'], route_df['dep_time_min']
        drive_hr, drive_min = route_df['travel_time_hr'], route_df['travel_time_min']
        dest_city, dest_state = route_df['dest_city_name'], route_df['dest_state_code']
        src_city, src_state = route_df['src_city_name'], route_df['src_state_code']
        route_type = route_df['route_type']
        
        return [start_hr, start_min, drive_hr, drive_min, dest_city, dest_state, src_city, src_state, route_type]

    def _verify_day_of_week(self, route_day_int, assigned_day):
        '''Assuming that Friday is a weekday only'''
        
        # Weekday only
        if route_day_int == '1': return assigned_day in self.valid_days[:5]
        # Weekends only
        if route_day_int == '2': return assigned_day in self.valid_days[5:]

        # Runs daily will always return true since we have already verified the day is a real day
        return True

            
    def _check_person_schedule(self, assignments_df):
        # now have the dataframe in pandas
        driver_id = assignments_df['driver_id'].iloc[0]
        
        # get the home city and state of the driver
        driver_df = self.driver_ddf[self.driver_ddf['driver_id'] == driver_id][['home_city', 'home_state']].compute()
        
        # if the driver does not exist, return
        if len(driver_df) == 0:
            return
        
        home_city, home_state = list(driver_df.itertuples(index=False, name=None))[0]
        
        # if the driver does not have at least one route to their hometown, return 
        hometown_indexes, remove_indexes = self._check_if_hometown_route_exists(home_city, 
                                                                                home_state, 
                                                                                assignments_df['route_id'])
        
        # no hometown, return
        if len(hometown_indexes) == 0:
            return
        
        # remove routes that don't exist 
        assignments_df = assignments_df.drop(remove_indexes)
        
        # get the indexes of assignment dataframe
        index_values = assignments_df.index.values.tolist()
        # remove hometown indexes from list
        index_values = [val for val in index_values if val not in hometown_indexes]
        
        # float the hometown assignments to the top
        sorted_indexes = hometown_indexes + index_values
        assignments_df = assignments_df.reindex(sorted_indexes)
        
        # create a person object
        driver = Driver(hometown=(home_city, home_state)) # need to pass in hometown info
        remove_indexes = []
        
        # iterate over assignments and see if they can be inserted
        for index, row in assignments_df.iterrows():
            # get route ID
            route_id = row['route_id']
    
            # get route info
            day_of_week = row['day_of_week']
            route_info = self._get_route_info(route_id)
            route_type = route_info[-1]
            route_info = tuple([day_of_week] + route_info[:-1])

            # Check if the day is valid (matches when the route runs)
            day_is_valid = self._verify_day_of_week(route_type, day_of_week)
        
            # check if row can be inserted
            if driver.is_availiable(*route_info) and day_is_valid:
                # add busy time
                driver.add_busy_time(*route_info)
            else:
                # don't keep the assignment
                remove_indexes.append(index)
        
        assignments_df = assignments_df.drop(remove_indexes)
        
        # make intermediate saves 
        path_to_save = './intermediate_csvs/assignments/' + str(driver_id) + '.csv'
        assignments_df.to_csv(path_to_save, header=False, index=False)

    def _combine_csvs(self):
        # Concatenate csvs
        concat_files = glob.glob('./intermediate_csvs/assignments/*.csv')
        
        # Create empty csv
        dest = './processed_csvs/assignments/assignments.csv'
        with open(dest, 'w') as empty_csv:
            pass
        
        # if multiple files, concatenate
        if len(concat_files) > 1:
            self._concat_files(concat_files, dest)
        else:
            # copy full file over to the destination
            copyfile(concat_files[0], dest)
        
    def _verify_times_and_save(self):
        # Delete any old existing files 
        delete_file_list = glob.glob('./intermediate_csvs/assignments/*.csv')
        self._delete_files(delete_file_list)
        
        groups = self.df.groupby('driver_id')
        
        t = groups.apply(lambda x: self._check_person_schedule(x), meta=self.df._meta).compute()
        
        # save files
        self._combine_csvs()

    def _verify_attributes(self):
        '''Verifies basic attributes in the table. 
        
        We verify that each of the attributes follows the datastructures we are placing on them.
        Note: we must check if the dataframe is empty so that we avoid KeyErrors.
        '''
        
        if self._is_empty(self.df):
            return self.df
        
        # driver_id must be 5 characters and alphanumeric
        self.df = self.df[self.df['driver_id'].apply(
            lambda x: self._verify_id(x), meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # route_ID must be 5 characters and alphanumeric
        self.df = self.df[self.df['route_id'].apply(
            lambda x: self._verify_id(x), meta=pd.Series([], dtype='str', name='x'))]

        if self._is_empty(self.df):
            return self.df

        # day_of_week must be 1 characters and valid day of week
        self.df = self.df[self.df['day_of_week'].apply(
            lambda x: self._verify_str_len(x, 1, 1) and x in self.valid_days,
            meta=pd.Series([], dtype='str', name='x'))]
        
        if self._is_empty(self.df):
            return self.df
        
        # drop duplicates
        self.df = self.df.reset_index().drop_duplicates(['driver_id', 'route_id', 
                                                         'day_of_week']).set_index("index")


def main(files):
    files = [str(file[2:]) for file in files]
    routes_file, drivers_file, assignment_file = files
    
    # First, read in routes so we can get the destination cities
    routes_ddf = RouteReader(routes_file)
    routes_ddf.verify_df()
    # Save cleaned data
    routes_ddf.to_csv()
    dest_cities = routes_ddf.get_dest_city_names()

    # Read drivers and filter out (also considering that their hometown must be a destination)
    drivers_ddf = DriverReader(drivers_file, dest_cities=dest_cities)
    drivers_ddf.verify_df()
    # Save cleaned data
    drivers_ddf.to_csv()
    
    # Read assignments
    assignment_ddf = AssignmentReader(assignment_file, drivers_ddf.df, routes_ddf.df)
    assignment_ddf.verify_df()
    # Save cleaned data
    assignment_ddf._verify_times_and_save()

if __name__ == '__main__':
    '''
    Example usage:

    python validation.py --"./test_csvs/routes/edited_Lin_Routes.csv" --"./test_csvs/drivers/Lin_Driver2.csv" --"./test_csvs/assignments/Lin_Assignment2.csv"
    '''
    args = sys.argv[1:]
    main(args)