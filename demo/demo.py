import argparse
import sys
import pandas as pd
import numpy as np 
import os.path
from os import path
import demodf


def math_query(df, action, rows, groupby_items, items):
    # check if rows is defined.  If yes, add a new column and set it same as the index then add this new column to groupby_items                  
    if rows is not None and rows.isdigit():
        num_rows = int(rows)
        df['rows_' + rows]= np.arange(0, len(df), 1)
        #If we want to set index per groupby object, we can do as the following line.  But now we don't want to.
        #df['rows_' + rows] = df.groupby(groupby_items)[groupby_items].rank(method='first')
        df['rows_' + rows] = df['rows_' + rows] // num_rows

        if groupby_items is None:
            groupby_items = ['rows_' + rows]
        else:
            groupby_items.insert(0, 'rows_' + rows)
    else:
        if rows is not None:
            print("Warning: -v option is invalid(must be the digits).  It will be ignored.")

    # if items is None, create items list, add all columns with int or float types in it
    if items is None:
        items = []
        for col in df.select_dtypes(include=['float', 'int']).columns:
            if groupby_items is None or col not in groupby_items:
                items.append(col)

    if groupby_items is None:
        df = df[items]
        if action == 'mean' or action == 'avg':
            df = df.mean()
        elif action == 'sum':
            df = df.sum()
        elif action == 'max':
            df = df.max()
        elif action == 'min':
            df = df.min()
        elif action == 'count':
            df = df.count()
        elif action == 'describe':
            df = df.describe()
        else:
            return None 

        if action != 'describe':
            df = df.to_frame(name=action)
            #df = df.rename(columns={0: action})
            df.index.name = 'key'
            df = df.reset_index()
        
    else:
        if action == 'mean' or action == 'avg':
            df = df.groupby(groupby_items)[items].mean().reset_index()
        elif action == 'sum':
            df = df.groupby(groupby_items)[items].sum().reset_index()
        elif action == 'max':
            df = df.groupby(groupby_items)[items].max().reset_index()
        elif action == 'min':
            df = df.groupby(groupby_items)[items].min().reset_index()
        elif action == 'count':
            df = df.groupby(groupby_items)[items].count().reset_index()
        elif action == 'describe':
            df = df.groupby(groupby_items)[items].describe().reset_index()
            df.columns = ["_".join(pair) for pair in df.columns]
        else:
            return None

        if action != 'describe':
            mapping = {}
            for item in items:
                mapping[item] = item + '_' + action
            df = df.rename(columns=mapping)

    return df

def print_df(df, mode='', index=True):
    if mode == 'markdown':
        print(df.to_markdown(index=index))
    else:
        print(df.to_string(index=index))

def write(df, outputfile, groupby_items, mode='', sort_items=None, ascending=True):
    if groupby_items is None:
        write_df(df, outputfile, mode, sort_items, ascending)
    else:
        df_g = df.groupby(groupby_items)
        for key in df_g.groups.keys():
            sub_df = df_g.get_group(key)
            if len(groupby_items) == 1:
                filename_key = str(key)
            else:
                filename_key = '_'.join([str(x) for x in list(key)])
            outputfile = filename_key + '_' + outputfile
            write_df(sub_df, outputfile, mode, sort_items, ascending)

def write_df(df, outputfile, mode='', sort_items=None, ascending=True):
    if outputfile is None:
        print("Output file is not defined.  No file is written")
        return

    # Drop the columns if the whole columns are NA
    df = df.dropna(axis=1, how='all')

    if len(df) == 0:
        print("Warning: dataframe is empty.")
        sys.exit()

    if sort_items is not None and len(sort_items) != 0:
        df = df.sort_values(sort_items, ascending)

    # Reset the index
    df = df.reset_index(drop=True)

    if mode == 'a':
        if os.path.isfile(outputfile):
            columns = pd.read_csv(outputfile, nrows=1, encoding="ISO-8859-1").columns

            if sorted(list(df.columns)) == sorted(list(columns)):
                df = df[list(columns)]
                df.to_csv(outputfile, index=False, mode='a', header=False)
                print("Append new context to the file ", outputfile)
            else:
                o_df = pd.read_csv(outputfile, encoding="ISO-8859-1")
                f_df = pd.concat([o_df, df], sort=False)
                f_df = f_df.reset_index(drop=True)
                f_df.to_csv(outputfile, index=False)
                print("Merge and append new context to the file ", outputfile)
        else:
            df.to_csv(outputfile, index=False)
            print("Output file is ", outputfile)
    else:        
        df.to_csv(outputfile, index=False)
        print("Output file is ", outputfile)


class Demo(demodf.DemoDf):
    def __init__(self, file, condition, action, action_value, x, y, t, t2, groupby, outputFile, outputFileAppended, separator, prints, sorted_values):
        demodf.DemoDf.__init__(self, file, condition, x, y, groupby, t, t2, ',')
        # After checking the condition, the dataframe will be updated.  The new dataframe maybe the subset of the original dataframe
        if action != 'set':
            self.check_condition()
     
        if self.df is None or len(self.df) == 0:
            print("Warning: dataframe is empty")
            sys.exit()

        self.action = action

        self.action_value = action_value

        self.outputFile = None
        self.write_mode = ''
        if outputFile is not None:
            outputFile = outputFile.strip().strip('\r')
            if '.' in outputFile:
                self.outputFile = outputFile
            elif outputFile == '-':
                self.outputFile = file
            else:
                self.outputFile = outputFile + "_" + file
        else:
            if outputFileAppended is not None:
                self.write_mode = 'a'
                outputFile = outputFileAppended.strip().strip('\r')
                if '.' in outputFile:
                    self.outputFile = outputFile
                else:
                    self.outputFile = outputFile + "_" + file

        self.prints = prints
        if self.prints is None or self.prints == 'true':
            self.prints = True
        else:
            self.prints = False

        self.s = self.convert_item_list(sorted_values)
        self.validate_arg(self.s, '-s')

    def math_action(self):
        df = math_query(self.df, self.action, self.action_value, self.groupby, self.y)
        print_df(df)
        if self.outputFile is not None:
            write_df(df, self.outputFile, self.write_mode)

    def process(self):
        if self.action in ['avg', 'count', 'max', 'mean', 'min', 'sum', 'describe']:
            self.math_action()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='demo', add_help=True)
    parser.add_argument('f', help='file name')
    parser.add_argument('a', choices=('avg', 'count', 'max', 'mean', 'min', 'sum', 'describe', 'col'), help='algo for the specific item')
    parser.add_argument('-v', help='values needed for action')
    parser.add_argument('-c', help='condition')
    parser.add_argument('-x', help='index')
    parser.add_argument('-y', help='column index')
    parser.add_argument('-g', help='groupby')
    parser.add_argument('-t', help='column index for time')
    parser.add_argument('-t2', help='timeformat')
    parser.add_argument('-o', help='outputFile')
    parser.add_argument('-s', help='separetor')
    parser.add_argument('-w', help='outputFile which will be appended')
    parser.add_argument('-p', choices=('true', 'false'), help='no print')
    parser.add_argument('-sort', help='columns the dataframe wants to sort values')

    results = parser.parse_args()
    if results.a == 'col':
        col = demodf.get_df_col(results.f)
        if col is None:
            print("Empty file doesn't have header line")
        else:
            col_str = ''
            for i, item in enumerate(col, start=0):
                if results.v is None or results.v in item:
                    col_str += str(i) + ':' + str(item) + ' '
            print(col_str)
        sys.exit()

    my_demo = Demo(results.f, results.c, results.a, results.v, results.x, results.y, results.t, results.t2, results.g, results.o, results.w, results.s, results.p, results.sort)
    my_demo.process()




 