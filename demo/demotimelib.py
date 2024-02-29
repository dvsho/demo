from datetime import datetime
from datetime import date
import pytz
import re
import numpy as np
import pandas as pd


# 1000 milliseconds(ms) = 1 second
# 1000 microseconds(MS) = 1 millisecond

ONE_DAY = 86400
ONE_HOUR = 3600
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_MILLI = 0.001
ONE_MICRO = 0.000001

TIME_FORMAT_0 = '%Y-%m-%d %H:%M:%S.%f'
TIME_FORMAT_1 = '%m/%d/%Y %H:%M:%S.%f'
TIME_FORMAT_2 = '%m/%d/%Y %H:%M:%S'
TIME_FORMAT_3 = '%Y-%m-%d %H:%M:%S'
TIME_FORMAT_4 = '%m/%d/%Y %H:%M'
TIME_FORMAT_5 = '%Y-%m-%d %H:%M'
TIME_FORMAT_6 = '%H:%M:%S.%f'
TIME_FORMAT_7 = '%H:%M:%S'
TIME_FORMAT_8 = '%m/%d/%Y'
TIME_FORMAT_9 = '%Y-%m-%d'
TIME_FORMAT_10 = '%H:%M'
TIME_FORMAT_11 = '%m/%d'
TIME_FORMAT_12 = '%m/%Y'
TIME_FORMAT_13 = '%Y-%m'
TIME_FORMAT_DEFAULT = ''

def get_localtime():
    return datetime.now().astimezone(pytz.timezone('US/Central')).strftime('%Y-%m-%d_%H:%M:%S')


def get_timedelta_unit(df, datetime_column_name):
    new_df = df[datetime_column_name].diff()
    new_df = new_df.dropna()

    mean_value = new_df.mean().total_seconds()
    timedelta_unit = 'U'
    if mean_value >= ONE_DAY:
        timedelta_unit = 'D'
    elif mean_value >= ONE_HOUR:
        timedelta_unit = 'H'
    elif mean_value >= ONE_MINUTE:
        timedelta_unit = 'm'
    elif mean_value >= ONE_SECOND:
        timedelta_unit = 'S'
    elif mean_value >= ONE_MILLI:
        timedelta_unit = 'L'
    return timedelta_unit

def get_timedelta_value(timedelta, timedelta_unit):
	time_delta = timedelta.total_seconds()

	if timedelta_unit == 'D':
		return time_delta/ONE_DAY
	elif timedelta_unit == 'H':
		return time_delta/ONE_HOUR
	elif timedelta_unit == 'm':
		return time_delta/ONE_MINUTE
	elif timedelta_unit == 'S':
		return time_delta/ONE_SECOND
	elif timedelta_unit == 'L':
		return time_delta/ONE_MILLI
	else:
		return time_delta/ONE_MICRO	 

supported_time_formats = [TIME_FORMAT_0, TIME_FORMAT_1, TIME_FORMAT_2, TIME_FORMAT_3, TIME_FORMAT_4,
	TIME_FORMAT_5, TIME_FORMAT_6, TIME_FORMAT_7, TIME_FORMAT_8, TIME_FORMAT_9, TIME_FORMAT_10,
	TIME_FORMAT_11, TIME_FORMAT_12, TIME_FORMAT_13]

time_pattern_to_format_mapping = {
	'\d{4}-\d{1,2}-\d{1,2}[\s_T]\d{1,2}:\d{1,2}:\d{1,2}.\d{1,6}': TIME_FORMAT_0,
	'\d{1,2}\/\d{1,2}\/\d{4}[\s_T]\d{1,2}:\d{1,2}:\d{1,2}.\d{1,6}': TIME_FORMAT_1,
	'\d{1,2}\/\d{1,2}\/\d{4}[\s_T]\d{1,2}:\d{1,2}:\d{1,2}': TIME_FORMAT_2,
	'\d{4}-\d{1,2}-\d{1,2}[\s_T]\d{1,2}:\d{1,2}:\d{1,2}': TIME_FORMAT_3,
	'\d{1,2}\/\d{1,2}\/\d{4}[\s_T]\d{1,2}:\d{1,2}': TIME_FORMAT_4,
	'\d{4}-\d{1,2}-\d{1,2}[\s_T]\d{1,2}:\d{1,2}': TIME_FORMAT_5,
	'\d{1,2}:\d{1,2}:\d{1,2}.\d{1,6}': TIME_FORMAT_6,
	'\d{1,2}:\d{1,2}:\d{1,2}': TIME_FORMAT_7,
	'\d{1,2}\/\d{1,2}\/\d{4}': TIME_FORMAT_8,
	'\d{4}-\d{1,2}-\d{1,2}': TIME_FORMAT_9,
	'\d{1,2}:\d{1,2}': TIME_FORMAT_10,
	'\d{1,2}\/\d{1,2}': TIME_FORMAT_11,
	'\d{1,2}\/\d{4}': TIME_FORMAT_12,
	'\d{4}-\d{1,2}': TIME_FORMAT_13 		
}

time_shortname_to_format_mapping = {
	't0': TIME_FORMAT_0,
	# add 0 as prefix for the miscroseconds
	'bft0': TIME_FORMAT_0,
	't1': TIME_FORMAT_1,
	# add 0 as prefix for the miscroseconds
	'bft1': TIME_FORMAT_1,
	't2': TIME_FORMAT_2,
	't3': TIME_FORMAT_3,
	't4': TIME_FORMAT_4,
	't5': TIME_FORMAT_5,
	't6': TIME_FORMAT_6,
	# add 0 as prefix for the miscroseconds
	'bft6': TIME_FORMAT_6,
	't7': TIME_FORMAT_7,
	't8': TIME_FORMAT_8,
	't9': TIME_FORMAT_9,
	't10': TIME_FORMAT_10,
	't11': TIME_FORMAT_11,
	't12': TIME_FORMAT_12,
	't13': TIME_FORMAT_13			
}

def detect_time_format_of_df_column(df, column_name):
	# Find the first valid string in the column and detect its format
	new_df_column = df[column_name].loc[~df[column_name].isnull()]
	if new_df_column is not None and len(new_df_column) != 0:
		first_valid_str = new_df_column.iloc[0].strip()
		#print(first_valid_str)
		return detect_time_format(first_valid_str)
	return TIME_FORMAT_DEFAULT

def get_time_format(shortname):
	if shortname is not None and shortname in time_shortname_to_format_mapping.keys():
		return time_shortname_to_format_mapping[shortname]
	return TIME_FORMAT_DEFAULT

def get_shortname(time_format):
	shortnames = [i for i in time_shortname_to_format_mapping if time_shortname_to_format_mapping[i] == time_format]
	if len(shortnames) != 0:
		return shortnames[0]
	else:
		return None

def detect_time_format(time_string):
	if time_string != '':
		for pattern in time_pattern_to_format_mapping.keys():
			p = re.compile(pattern).fullmatch(time_string)
			if p is not None:
				#print(time_pattern_to_format_mapping[pattern])
				return time_pattern_to_format_mapping[pattern]
	return TIME_FORMAT_DEFAULT

def print_supported_format():
	for key, value in time_shortname_to_format_mapping.items():
		pattern = [i for i in self.time_pattern_to_format_mapping if self.time_pattern_to_format_mapping[i] == value]
		pattern = str(pattern[0]).replace('\\\\', '\\')
		print(key + ':', value, pattern)
			   
# Convert microsecond format by filling 0 as prefix but not suffix
def prefix_micro_6_digits(time_string):
	if time_string is np.nan or time_string == '':
		return time_string

	time_string = str(time_string)

	if time_string.count('.') == 1:
		part1 = time_string.split('.')[0]
		part2 = time_string.split('.')[1]		
		return part1 + '.' + '0' * (6 - len(part2)) + part2
	
	return time_string	

def convert_df_column_datetime_by_shortname(df, column_name, shortname):
	if shortname is None or shortname not in time_shortname_to_format_mapping.keys():
		time_format = detect_time_format_of_df_column(df, column_name)
	else:			
		if shortname in ['bft0', 'bft1', 'bft6']:
			df[column_name] = df[column_name].apply(prefix_micro_6_digits)
		time_format = time_shortname_to_format_mapping[shortname]
			
	if time_format != TIME_FORMAT_DEFAULT:
		df[column_name] = pd.to_datetime(df[column_name], format=time_format)
		return df
	print('Warning: failed to convert the time column for the dataframe')
	return df

def convert_df_column_datetime(df, column_name, time_format):
	if time_format != TIME_FORMAT_DEFAULT:
		df[column_name] = pd.to_datetime(df[column_name], format=time_format)
		return df
	print('Warning: failed to convert the time column for the dataframe')
	return df

def convert_df_column_str(df, column_name, time_format):
	#print(column_name)
	df[column_name] = df[column_name].apply(lambda x: x.strftime(time_format))
	return df

def set_df_time_gap(df, column_name, shortname, gap_format):
	if pd.api.types.is_string_dtype(df[column_name]):
		df = convert_df_column_datetime(df, column_name, shortname)

	if pd.api.types.is_datetime64_dtype(df[column_name]):
		gaplist = df[column_name].diff()
		if gap_format is None:
			gap_format = get_timedelta_unit(df, column_name)		
		
		if gap_format in ['D', 'H', 'M', 'S', 'L', 'U']:
			if gap_format == 'U':					
				gaplist = gaplist.apply(lambda x: x.total_seconds()/ONE_MICRO)
			elif gap_format == 'L':
				gaplist = gaplist.apply(lambda x: x.total_seconds()/ONE_MILLI)
			elif gap_format == 'S':
				gaplist = gaplist.apply(lambda x: x.total_seconds())
			elif gap_format == 'M':
				gaplist = gaplist.apply(lambda x: x.total_seconds()/ONE_MINUTE)
			elif gap_format == 'H':
				gaplist = gaplist.apply(lambda x: x.total_seconds()/ONE_HOUR)
			elif gap_format == 'D':
				gaplist = gaplist.apply(lambda x: x.total_seconds()/ONE_DAY)

			locIdx = df.columns.get_loc(column_name)
			gaplist = gaplist.fillna(0)
			df.insert(loc=locIdx + 1, column=column_name + "_gap" + '_' + gap_format, value=gaplist.tolist())
		#else:
			#print("Warning: Valid selection for gap -v option is D, H, M, S, L or U")	
	return df
	

def print_supported_format():
	for shortname, time_format in time_shortname_to_format_mapping.items():
		pattern = [pattern for pattern in time_pattern_to_format_mapping.keys() if time_pattern_to_format_mapping[pattern] == time_format ]
		print(shortname, time_format, pattern[0].replace('\/\/', ''))
			
if __name__ == '__main__':
	print(get_localtime())
	print_supported_format()
	time_format = detect_time_format('1949-08-22T22:22:22.1')
	print(time_format)
	print(str(pd.to_datetime('1949-08-22 22:22:22.1', format=time_format)))
	print(prefix_micro_6_digits('1949-08-22 22:22:22.1'))

	df = pd.read_csv('2.csv', encoding="ISO-8859-1", sep=',', low_memory=False)

	df = convert_df_column_datetime(df, 'time', 'bft6')

	df1 = df.query('time > "1900-01-01 20:20:20.000300"')
	print(df1)

	df = set_df_time_gap(df, 'time', 'bft6', 'S')

	print(df['time'].astype(str))

	df =  convert_df_column_str(df, 'time', format)
	print(df)

	




