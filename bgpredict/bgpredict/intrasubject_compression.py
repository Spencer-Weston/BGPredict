from helpers import S3Connection
import pandas as pd
from datetime import datetime


class CompressionManager(S3Connection):

    def __init__(self, source_directory, target_directory, version):
        super().__init__()
        self.source_directory = source_directory
        self.target_directory = f"{target_directory}{version}/"
        self.version = version
        self.source_files = []
        self._identify_files()

    def _identify_files(self):
        subject_file_locs = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.source_directory)

        for x in subject_file_locs['Contents']:
            path = x['Key']
            uri = f"s3://{self.bucket_name}/{path}"
            split = path.split('/')
            file_name = split[len(split)-1]
            subjectid = file_name.split('_')[0]
            self.source_files.append((subjectid, uri))

    def key_exists(self, key):
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
        if response and "Contents" in response.keys():
            for obj in response['Contents']:
                if key == obj['Key']:
                    return True
        return False

    def perform_compression(self):
        init_start = datetime.now()
        log_dict = {'subjectid': [], "insulin_sum": [], "insulin_first": [], "carb_sum": [], "carb_first":[]}
        for sub_id, source_file in self.source_files:
            start = datetime.now()
            print(f"starting {sub_id} at {start}")
            target_file = f"{self.target_directory}{sub_id}"
            if self.key_exists(target_file):
                print(f"File for {sub_id} already exists. Skipping")
                continue
            compressor = SubjectCompressor(file=source_file)
            df = compressor.compress()
            print(f"{sub_id} carb aggregation methods: {compressor.carb_count}")
            print(f"{sub_id} insulin aggregation methods: {compressor.ins_count}")

            # store stats
            log_dict['subjectid'].append(sub_id)
            log_dict['insulin_sum'].append(compressor.ins_count['sum'])
            log_dict['carb_sum'].append(compressor.carb_count['sum'])
            log_dict['insulin_first'].append(compressor.ins_count['first'])
            log_dict['carb_first'].append(compressor.carb_count['first'])

            df_text = df.to_csv(index=False)
            self.s3_client.put_object(Bucket=self.bucket_name, Key=target_file, Body=df_text)
            print(f"Finished {sub_id} in {start}")

        log_csv = pd.DataFrame(log_dict)
        log_csv.to_csv('../../misc/compression_stats.csv')
        print(f"Processed finished in {datetime.now() - init_start}")


class SubjectCompressor:

    def __init__(self, file):
        self.df = pd.read_csv(file)
        self.bg_col = None
        self.timestamp_col = None
        self.ins_count = {'sum': 0, 'first': 0}
        self.carb_count = {'sum': 0, 'first': 0}

    def compress(self):
        self._configure()
        self.df = self.column_selection()
        self.df = self.groupby_entryid()
        self.df = self.df.sort_values('timestamp')
        self.df = self.df.reset_index()
        return self.df

    def column_selection(self):
        columns = [self.timestamp_col, 'entryid', 'subjectid', self.bg_col, 'insulin', 'carbs']
        return self.df.loc[:, columns]

    def groupby_entryid(self):
        df = self.df.groupby('entryid').agg({
            'subjectid': "first",
            self.timestamp_col: self.select_timestamp,
            self.bg_col: 'first',
            'insulin': self.insulin_agg_wrapper,
            'carbs': self.carb_agg_wrapper
        })
        df = df.rename(columns={self.timestamp_col: 'timestamp', self.bg_col: 'bg'})
        return df

    # Aggregation functions and wrappers for groupby
    @staticmethod
    def select_timestamp(x):
        x = x.loc[~x.isna()]
        if len(x) == 0:
            return None
        else:
            return x.iloc[0]

    def insulin_agg_wrapper(self, x):
        x = self.insulin_carb_agg(x, self.ins_count)
        return x

    def carb_agg_wrapper(self, x):
        x = self.insulin_carb_agg(x, self.carb_count)
        return x

    @staticmethod
    def insulin_carb_agg(x, summary_dict):
        x = x.loc[~x.isna()]
        if len(x) > 1:
            # For subject 309157, there are often duplicate insulin/carb values. These are *obviously* duplicates
            # as validated by looking at the original treatment table. However, this may not always
            # be the case. A possible improved validation method would check the treatment 'created_at'
            # or timestamp value and verify the insulin was added at the same time
            if len(pd.unique(x)) == 1:
                summary_dict['first'] += 1
                return x.iloc[0]
            else:
                summary_dict['sum'] += 1
                return x.sum()
        elif x.empty:
            return 0
        else:
            return x

    # End Aggregation functions for groupby

    def _configure(self):
        columns = self.df.columns
        possible_bg_cols = ["bg", "bg_z"]
        possible_timestamp_cols = ["timestamp", "timestamp_y", "timestamp_z"]
        for c in possible_bg_cols:
            if c in columns:
                self.bg_col = c
        for c in possible_timestamp_cols:
            if c in columns:
                self.timestamp_col = c
        if self.bg_col is None or self.timestamp_col is None:
            raise Exception("")


if __name__ == "__main__":
    # path = "C:\\Users\\spenc\\Documents\\Berkeley\\Capstone\\intrasubject_joinv0.2\\309157_joined_csv"
    # subj = SubjectCompressor(path)
    # subj.compress()
    source_directory = "intrasubject_joinv0.2/"
    target_directory = "intrasubject_data"
    version = "V0.1"
    manager = CompressionManager(source_directory=source_directory, target_directory=target_directory, version=version)
    manager.perform_compression()
