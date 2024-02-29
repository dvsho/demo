import pandas as pd
import demotimelib 
import sys
import re
import os.path
from os import path
import argparse

def get_sep(sep):
    supported_sep = ['SPACE', 'COLON', 'COMMA', 'BSLASH', 'FSLASH', 'QMARK', 'SCOLON', 'VBAR', 'DOT', 'EMPTY']

    if sep not in supported_sep:
        print(supported_sep)
        return sep

    if sep == 'SPACE':
        return " "
    elif sep == "COLON":
        return ':'
    elif sep == "COMMA":
        return ','
    elif sep == "BSLASH":
        return '\\'
    elif sep == "FSLASH":
        return '/'
    elif sep == "QMARK":
        return '?'
    elif sep == 'SCOLON':
        return ';'
    elif sep == 'VBAR':
        return '|'
    elif sep == 'DOT':
        return '.'
    elif sep == 'EMPTY':
        return ''
    return sep

def convert_column(col):       
    special_chars = '!#$^&*{}[]():'
    for special_char in special_chars:
        col = col.replace(special_char, '')
    col = col.replace(' ', '_')
    col = col.replace("-", '_')
    return col.lower()

def get_columns_mapping(columns_list):
    columns_mapping = {}
    for col in columns_list:
        columns_mapping[col] = convert_column(col)
    return columns_mapping

def is_file(file):
    return path.isfile(file)

def get_df(file, sep=','):
    return pd.read_csv(file, encoding="ISO-8859-1", sep=sep, low_memory=False)

def get_df_col(file, sep=','):
    if is_file(file):
        col = pd.read_csv(file, encoding="ISO-8859-1", sep=sep, nrows=1).columns
        return list(col)
    return None

def get_start(df, column_name):
    new_df_column = df[column_name].loc[~df[column_name].isnull()]
    if new_df_column is not None and len(new_df_column) != 0:
        return new_df_column.iloc[0]
    else:
        return ''

def get_end(df, column_name):
    new_df_column = df[column_name].loc[~df[column_name].isnull()]
    if new_df_column is not None and len(new_df_column) != 0:
        return new_df_column.iloc[-1]
    else:
        return ''

class DemoDf:
    def __init__(self, file, condition, x, y, groupby, t, t2, separator):
        # Read file and convert dataframe from the file
        self.file = file.strip().strip('\r')

        if is_file(self.file) == False:
            print("Error: File " + self.file + " doesn't exist.  Quit")
            sys.exit()


        self.separator = ','
        if separator is not None:
            self.separator = separator
        self.df = get_df(file, self.separator)
        self.df.drop(self.df.columns[self.df.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
        self.df.dropna(axis=1, inplace=True, how='all')


        columns_mapping = get_columns_mapping(self.df.columns)
        self.df.rename(columns=columns_mapping, inplace=True)

        # Argument t is a string for the column name which column should be convert to time format
        # 'time' or 'Time' or 'date' or 'Date'
        self.t = None
        self.t2 = None
        self.time_format = {}


        if t is not None:
            self.t = t.strip().strip("\r").split(":")
            self.t = [self.df.columns[int(x)] if x.isdigit() and int(x) < len(self.df.columns) else x.lower() for x in self.t]

            if t2 is not None:
                self.t2 = t2.strip().strip("\r").split(":")
                if len(self.t) != len(self.t2):
                    print("Error: t and t2 should have the same length!!!")
                    sys.exit()

                for idx in range(0, len(self.t)):
                    self.df = timelib.convert_df_column_datetime_by_shortname(self.df, self.t[idx], self.t2[idx])
                    self.time_format[self.t[idx]] = timelib.get_time_format(self.t2[idx])
            else:
                for idx in range(0, len(self.t)):
                    t2_format = timelib.detect_time_format_of_df_column(self.df, self.t[idx])
                    self.time_format[self.t[idx]] = t2_format
                    self.df = timelib.convert_df_column_datetime(self.df, self.t[idx], t2_format)

        

        self.x = self.convert_item_list(x)

        self.y = self.convert_item_list(y)

        self.groupby = self.convert_item_list(groupby)

        self.df_item_list = []
        self.validate()

        # Check condition to remove all columns not fit for the conditions
        self.condition = condition
        if self.condition is not None:
            self.condition = self.condition.strip().strip('\r')


    def convert_item_list(self, input_list):
        item_list = None
        if input_list is not None:
            if '--' not in input_list:
                item_list = input_list.strip().strip('\r').split(':')
                item_list = sorted(set(item_list), key=lambda x: item_list.index(x))
                item_list = [self.df.columns[int(x)] if x.isdigit() and int(x) < len(self.df.columns) else x.lower() for x in item_list]
            else:
                y_list = input_list.strip().strip("\r").split(":")
                y_list = sorted(set(y_list), key=lambda x: y_list.index(x))
                y_list = [self.df.columns[int(x)] if x.isdigit() and int(x) < len(self.df.columns) else x.lower() for x in y_list]
                item_list = []
                for item in y_list:
                    if '--' not in item:
                        item_list.append(item)
                    else:
                        start_y = item.split('--')[0]
                        end_y = item.split('--')[1]

                        start_index = -1
                        end_index = -1
                        if start_y.isdigit() and int(start_y) < len(self.df.columns):
                            start_index = int(start_y)
                        elif start_y in self.df.columns:
                            start_index = self.df.columns.index(start_y)


                        if end_y.isdigit() and int(end_y) <len(self.df.columns):
                            end_index = int(end_y)
                        elif end_y in self.df.columns:
                            end_index = self.df.columns.index(end_y)

                        if start_index != -1 and end_index != -1:
                            item_list.extend(list(self.df.columns)[start_index:(end_index + 1)])
                item_list = sorted(set(item_list), key=lambda x: item_list.index(x))

        return item_list
                


    def check_condition(self):
        if self.condition is None:
            return

        condition = self.condition
        #-c '(dtx.str.contains("DTX"))&~(txNumber>1)'
        #-c '(dtx.str.contains("DTX"))|~(txNumber>1)'
        #-c 'dtx=="DTX"'
        #-c 'txNumber==1'
        condition = condition.replace("SPACE", " ")
        #print(condition)
        self.df = self.df.query('(' + condition + ')')
        self.df.dropna(axis=1, inplace=True, how='all')
        #self.df.drop_duplicates(inplace=True)
        self.df.reset_index(inplace=True, drop=True)
        columns_mapping = get_columns_mapping(self.df.columns)
        self.df.rename(columns=columns_mapping, inplace=True)

        if self.x is not None:
            self.x = [x for x in self.x if x in self.df.columns]
            if len(self.x) == 0:
                self.x = None

        if self.y is not None:
            self.y = [x for x in self.y if x in self.df.columns]
            if len(self.y) == 0:
                self.y = None
      

        if self.groupby is not None:
            self.groupby = [x for x in self.groupby if x in self.df.columns]
            if len(self.groupby) == 0:
                self.groupby = None

    def validate_arg(self, arg, argName):
        if arg is not None and (set(arg).issubset(set(self.df.columns)) == False or len(arg) == 0):
            invalid_list = [c for c in arg if c not in self.df.columns]
            print("Error: " + argName + " has invalid columns " + str(invalid_list))
            print("valid columns are: " + ','.join(self.df.columns))
            output='*** '
            count = 0
            for idx in range(0, len(self.df.columns)):
                output += str(count) + ": " + str(self.df.columns[idx]) + ', '
                count += 1
            output = output.strip(', ')
            #print(output)
            sys.exit()
        elif arg is not None:
            self.df_item_list.extend(arg)

    def validate(self):
        self.validate_arg(self.t, '-t')
        self.validate_arg(self.x, '-x')
        self.validate_arg(self.y, '-y')
        self.validate_arg(self.groupby, '-g')
