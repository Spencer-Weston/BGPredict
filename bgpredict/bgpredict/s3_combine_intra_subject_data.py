"""Using the methods developed in Notesbooks/AggregateNightScoutJoinStatistics.ipynb, union all of the files in each
subject's treatments, device status, and entry folders in S3. Then, implement a temporal join to generate a single
output .csv per user and put it in the S3 bucket. """

import boto3
from collections import namedtuple
from datetime import datetime
import pytz
from sortedcontainers import SortedDict
import pandas as pd

class S3Connection:
    def __init__(self):
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = 'bgpredict'
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.s3_client = boto3.client("s3")

class S3FolderConnection(S3Connection):

    def __init__(self, head_directory=None):
        super().__init__()
        self.head_directory = head_directory
        self.subject_ids = []
        self.subject_dirs = []
        self.direct_sharing_folder = "direct-sharing-31/"  # Constant folder for each subject
        self.subject_info = []

    def set_head_directory(self, head_directory):
        self.head_directory = head_directory

    def identify_subject_ids(self):
        """Update self.subject_ids with unique identifiers by subject and self.subject_dirs with folders"""
        subject_file_locs = self.bucket.meta.client.list_objects_v2(Bucket=self.bucket_name,
                                                                    Delimiter="/",
                                                                    Prefix=self.head_directory).get('CommonPrefixes')
        for file_loc in subject_file_locs:
            file_str = file_loc['Prefix']
            file_str = file_str.replace(self.head_directory, "")
            file_str = file_str.replace("/", "")
            try:
                subject_id = int(file_str)
                self.subject_ids.append(subject_id)
                self.subject_dirs.append(file_loc["Prefix"])
            except ValueError:
                # Skip subject id if it cannot be coerce to a string (Mostly applies to AndroidAPS data)
                print(f"WARNING: Could not coerce {file_str} into valid subject_id. Skipping.")

        return self.subject_ids

    def generate_subject_info(self):
        """Generate named tuples which holds all relevant pathing info for each subject in self.subject_ids"""
        SubjectInfo = namedtuple("SubjectInfo", ['path', 'subject_id', 'devicestatus', 'entries', 'treatments'])
        for s_id, folder in zip(self.subject_ids, self.subject_dirs):
            relevant_dir_names = ["treatments", "devicestatus", "entries"]
            relevant_dirs = {k: [] for k in relevant_dir_names}
            path = f"{folder}{self.direct_sharing_folder}"
            # ToDo: List folders within each subject; identify device status, treatments, and entry
            subj_folders = self.bucket.meta.client.list_objects_v2(Bucket=self.bucket_name, Delimiter="/",
                                                                   Prefix=path).get('CommonPrefixes')
            # Some subjects don't
            if subj_folders is not None:
                for folder in subj_folders:
                    folder_path = folder['Prefix']
                    path_list = self.bucket.meta.client.list_objects_v2(Bucket=self.bucket_name, Delimiter="/",
                                                                        Prefix=f"{folder_path}")['Contents']
                    path_list = [i["Key"] for i in path_list]
                    for p in path_list:
                        for name in relevant_dir_names:
                            if name in p:
                                uri = f"s3://{self.bucket_name}/{p}"
                                relevant_dirs[name].append(uri)
                sub_info = SubjectInfo(path=path, subject_id=s_id, devicestatus=relevant_dirs['devicestatus'],
                                       treatments=relevant_dirs['treatments'], entries=relevant_dirs['entries'])
                self.subject_info.append(sub_info)


if __name__ == "__main__":
    print("Here and working!!!!")
    conn = S3FolderConnection()
    conn.set_head_directory(head_directory='openaps_csvs/')
    ids = conn.identify_subject_ids()
    conn.generate_subject_info()
    test = conn.subject_info
    print(test)
    t =2