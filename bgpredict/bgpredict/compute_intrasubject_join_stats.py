"""Intermediary file. Calculates a series of statistics by column across all tables that are generated from the
intrasubject join. Writes the result to a pickle object in the misc folder."""

import os
from collections import namedtuple
import pandas as pd
import pickle

from helpers import S3Connection
from datetime import datetime


class StatCalculator(S3Connection):

    def __init__(self, head_directory, write_directory):
        super().__init__()
        self.head_directory = head_directory
        self.write_directory = write_directory
        self.table_pathes = []
        self.column_stats = None
        self.identify_files()

    def identify_files(self):
        subject_file_locs = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.head_directory)

        for x in subject_file_locs['Contents']:
            path = x['Key']
            uri = f"s3://{self.bucket_name}/{path}"
            self.table_pathes.append(uri)

    def compute_statistics(self):
        ColInfo = namedtuple("ColInfo", ['subjectid', 'table_shape','numeric', 'count', 'num_unique', 'num_null',
                                         'mean', 'std_dev'])
        column_stats = {}
        i = 0
        for path in self.table_pathes:
            split = path.split('/')
            file_name = split[len(split)-1]
            subjectid = file_name.split('_')[0]

            start = datetime.now()
            print("\n", f"Calculating Stats for subject: {subjectid}", "\n")

            tbl = pd.read_csv(path)

            desc = tbl.describe()
            value_rows = ['count', 'mean', 'std']

            table_shape = tbl.shape
            non_numeric_cols = list(set(tbl.columns).difference(set(desc.columns)))
            non_numeric_desc = tbl.loc[:, non_numeric_cols].describe()

            for col in tbl.columns:
                column = tbl.loc[:, col]
                num_unique = len(pd.unique(column))
                num_null = sum(column.isna())
                try:
                    vals = desc.loc[value_rows, col]
                    count, mean, std_dev = vals.array
                    numeric = True
                except KeyError:
                    count = non_numeric_desc.loc['count', col]
                    mean = None
                    std_dev = None
                    numeric = False
                col_info = ColInfo(subjectid=subjectid, table_shape=table_shape, numeric=numeric, count=count,
                                   num_unique=num_unique, num_null=num_null, mean=mean, std_dev=std_dev)
                if col in column_stats:
                    column_stats[col].append(col_info)
                else:
                    column_stats.update({col: [col_info]})
            # clean up variables
            del tbl
            del desc
            del non_numeric_desc
            del column

            print("\n", f"Finished subject: {subjectid} in {datetime.now() - start}", '\n')

        self.column_stats = column_stats
        with open(f"{self.write_directory}/column_stats.pickle", 'wb') as file:
            pickle.dump(self.column_stats, file, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    head_directory = "intrasubject_joinv0.2/"
    calc = StatCalculator(head_directory, write_directory="../../misc")
    calc.identify_files()
    calc.compute_statistics()
