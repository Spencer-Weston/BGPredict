import pandas as pd

from helpers import S3Connection

class ColumnCounter(S3Connection):

    def __init__(self, head_directory, write_directory):
        super().__init__()
        self.head_directory = head_directory
        self.write_directory = write_directory
        self.table_pathes = []
        self.column_stats = None
        self._identify_files()

    def _identify_files(self):
        subject_file_locs = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.head_directory)

        for x in subject_file_locs['Contents']:
            path = x['Key']
            uri = f"s3://{self.bucket_name}/{path}"
            self.table_pathes.append(uri)

    def make_column_table(self):
        subject_ids = []
        for path in self.table_pathes:
            split = path.split('/')
            file_name = split[len(split)-1]
            subjectid = file_name.split('_')[0]
            subject_ids.append(subjectid)
        all_cols = {}
        i = 0
        for path in self.table_pathes:
            split = path.split('/')
            file_name = split[len(split)-1]
            subjectid = file_name.split('_')[0]
            print(f'Subject: {subjectid}')
            if subjectid == "14092221":
                continue
            df = pd.read_csv(path)
            cols = list(df.columns)
            for c in cols:
                if c in all_cols:
                    idx = self.table_pathes.index(path)
                    all_cols[c][idx] = 1
                else:
                    idx = self.table_pathes.index(path)
                    vals = [0 for _ in range(len(self.table_pathes))]
                    vals[idx] = 1
                    all_cols.update({c: vals})

        df = pd.DataFrame(all_cols, index=subject_ids)
        df.to_csv(f"{self.write_directory}/column_by_subject.csv")

if __name__ == "__main__":
    head_directory = "intrasubject_joinv0.2/"
    counter = ColumnCounter(head_directory, write_directory="../../misc")
    counter.make_column_table()