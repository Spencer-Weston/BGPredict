"""Using the methods developed in Notesbooks/AggregateNightScoutJoinStatistics.ipynb, union all of the files in each
subject's treatments, device status, and entry folders in S3. Then, implement a temporal join to generate a single
output .csv per user and put it in the S3 bucket. """

import boto3
from collections import namedtuple
from datetime import datetime
import pytz
from sortedcontainers import SortedDict
import pandas as pd
from pandas.errors import ParserError

class S3Connection:
    def __init__(self):
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = 'bgpredict'
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.s3_client = boto3.client("s3")

class JoinManager(S3Connection):

    def __init__(self, head_directory=None):
        super().__init__()
        self.head_directory = head_directory
        self.subject_ids = []
        self.subject_dirs = []
        self.direct_sharing_folder = "direct-sharing-31/"  # Constant folder for each subject
        self.subject_info = []
        self.subject_classes = []

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
                # Skip subject id if it cannot be coerce to a string (Should only apply to AndroidAPS data)
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

    def generate_subject_classes(self):
        for s in self.subject_info:
            self.subject_classes.append(Subject(s.subject_id, s.path, s.treatments, s.devicestatus, s.entries))

    def intra_subject_join_and_put(self, folder, version):
        """For each subject, perform the temporal join and put in s3 bucket with specified folder and version."""
        versioned_folder = f"{folder}{version}/"
        full_run_start = datetime.now()
        # use while loop + pop(0) to allow deletion of all data associated with a subject each iteration to minimize
        # memory usage
        while len(self.subject_classes) > 0:
            iteration_start = datetime.now()
            subject = self.subject_classes.pop(0)
            subject_id = subject.subject_id
            s3_location = f"{versioned_folder}{subject_id}_joined_csv"
            if self.key_exists(s3_location):
                print(f"{s3_location} already exists. Skipping")
            else:
                print(f"Creating join data for {subject_id}")
                joined_data = subject.get_join_table()
                if joined_data is None:
                    print(f"{subject_id} did not return a joined dataframe. No data will be written.")
                else:
                    joined_data_text = joined_data.to_csv(index=False)

                    print(f"Putting join data for {subject_id}")
                    self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_location, Body=joined_data_text)
                    print(f"{subject_id} finished in {datetime.now() - iteration_start}")
            del subject
        print(f"Full run finished in {datetime.now() - full_run_start}")


    def key_exists(self, key):
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
        if response and "Contents" in response.keys():
            for obj in response['Contents']:
                if key == obj['Key']:
                    return True
        return False


class Subject:

    def __init__(self, subject_id, subject_path, treatment_files, device_status_files, entries_files):
        self.subject_id = subject_id
        self.subject_path = subject_path

        self.treatment_files = treatment_files
        self.treatment_shapes = None
        self.treatment_df = None

        self.device_status_files = device_status_files
        self.device_status_shapes = None
        self.device_status_df = None

        self.entries_files = entries_files
        self.entries_shapes = None
        self.entries_df = None

        self.join_table = None

    def get_device_status_shapes(self):
        if self.device_status_shapes is not None:
            return self.device_status_shapes
        else:
            self.device_status_shapes = [pd.read_csv(file, low_memory=False).shape for file in self.device_status_files]
            return self.device_status_shapes

    def get_device_status_df(self):
        if len(self.device_status_files) == 0:
            return None
        if self.device_status_df is not None:
            return self.device_status_df
        else:
            device_status_dfs = [pd.read_csv(file, low_memory=False).reset_index(drop=True) for file in
                                 self.device_status_files]
            self.device_status_df = pd.concat(device_status_dfs, axis=0)
            self.device_status_df['timestamp'] = pd.to_datetime(self.device_status_df['created_at'])
            self.device_status_df['devicestatusid'] = [i for i in range(len(self.device_status_df))]
            return self.device_status_df

    def get_treatment_shapes(self):
        if self.treatment_shapes is not None:
            return self.treatment_shapes
        else:
            self.treatment_shapes = [pd.read_csv(file, low_memory=False).shape for file in self.treatment_files]
            return self.treatment_shapes

    def get_treatment_df(self):
        if self.treatment_df is not None:
            return self.treatment_df
        else:
            treatment_dfs = [pd.read_csv(file, low_memory=False).reset_index(drop=True) for file in
                             self.treatment_files]
            self.treatment_df = pd.concat(treatment_dfs, axis=0)
            try:
                self.treatment_df['timestamp'] = pd.to_datetime(self.treatment_df["created_at"])
            except ValueError:
                # Somewhat randomly, for unclear reasons, we receive the following error:
                # ValueError: cannot reindex from a duplicate axis
                # Resetting index seems to resolve this.
                self.treatment_df = self.treatment_df.reset_index()
                self.treatment_df['timestamp'] = pd.to_datetime(self.treatment_df["created_at"])
            self.treatment_df['treatmentid'] = [i for i in range(len(self.treatment_df))]
            return self.treatment_df

    def get_entries_shapes(self):
        if self.entries_shapes is not None:
            return self.entries_shapes
        else:
            self.entries_shapes = [pd.read_csv(file, low_memory=False).shape for file in self.entries_files]
            return self.entries_shapes

    def get_entries_df(self):
        if self.entries_df is not None:
            return self.entries_df
        else:
            entries_dfs = [pd.read_csv(file, low_memory=False, header=None).reset_index(drop=True) for file in
                           self.entries_files]
            self.entries_df = pd.concat(entries_dfs, axis=0)
            self.entries_df.columns = ["time", "bg"]
            try:
                self.entries_df['timestamp'] = pd.to_datetime(self.entries_df['time'])
            except Exception:
                print("HERE IN THE ERROR")
                try:
                    self.entries_df['timestamp'] = pd.to_datetime(self.entries_df['time'].str.replace("PM", ""))
            #                 self.entries_df['timestamp'] = pd.to_datetime(self.entries_df['time'])
                except Exception:
                    self.entries_df['timestamp'] = pd.to_datetime(self.entries_df['time'].str.replace("vorm.", ""))
                    
            self.entries_df['entryid'] = [i for i in range(len(self.entries_df))]
            return self.entries_df

    def get_join_table(self):
        if self.join_table is not None:
            return self.join_table
        else:
            self.join_table = self._temporal_join()
            return self.join_table

    def _temporal_join(self):
        if len(self.entries_files) == 0:
            print(f"No entries for subject {self.subject_id}. Returning None")
            return None
        elif len(self.device_status_files) == 0 and len(self.treatment_files) == 0:
            print(f"{self.subject_id} does not have device status or treatment files. Returning None")
            return None

        # Load tables and convert relevant columns to date times
        entry_df = self.get_entries_df()
        treatments_df = self.get_treatment_df()
        device_status_df = self.get_device_status_df()

        if treatments_df.empty and device_status_df.empty:
            print(f"subject {self.subject_id} does not have treatment or device status tables. Passing.")
        # Creat
        index_dict = self._temporal_join_index_dict(entries=entry_df, device_status=device_status_df,
                                                    treatments=treatments_df)
        join_df = self._create_temporal_join_df(index_dict)

        if device_status_df is not None and treatments_df is not None:
            joined_data = (join_df
                           .merge(entry_df, how='left', left_on='entryid', right_on='entryid', suffixes=("_x", "_ent"))
                           .merge(device_status_df, how='left', left_on="devicestatusid", right_on="devicestatusid",
                                  suffixes=("_y", "_ds"))
                           .merge(treatments_df, how='left', left_on="treatmentid", right_on="treatmentid",
                                  suffixes=("_z", "_tre"))
                           )
        elif device_status_df is None and treatments_df is not None:
            joined_data = (join_df
                           .merge(entry_df, how='left', left_on='entryid', right_on='entryid', suffixes=("_x", "_ent"))
                           .merge(treatments_df, how='left', left_on="treatmentid", right_on="treatmentid",
                                  suffixes=("_z", "_tre"))
                           )
        elif device_status_df is not None and treatments_df is None:
            joined_data = (join_df
                           .merge(entry_df, how='left', left_on='entryid', right_on='entryid', suffixes=("_x", "_ent"))
                           .merge(device_status_df, how='left', left_on="devicestatusid", right_on="devicestatusid",
                                  suffixes=("_y", "_ds"))
                           )
        return joined_data

    def _temporal_join_index_dict(self, entries, device_status, treatments):
        """Assign device status and treatment rows to the nearest entry that occurs after the device status or treatment row."""
        # Store timestamp and entries in zipped list; get entry timezones
        timestamp_keys = entries['timestamp'].to_list()
        timestamp_keys = [x.replace(tzinfo=pytz.utc) for x in timestamp_keys if
                          isinstance(x, pd.Timestamp) or isinstance(x, datetime)]
        entry_id_list = entries['entryid'].to_list()
        zipped = list(zip(timestamp_keys, entry_id_list))

        # Check for offset aware

        # fill in standard python dictionary with entry data; convert to SortedDict sorted on entry timestamps
        index_dict = SortedDict(
            {timestamp: (entry_id, {"device_status": [], "treatment": []}) for timestamp, entry_id in zipped})

        # Generate list of tuples for (devicetimestamp, deviceid)
        if device_status is not None:
            device_tuples = list(zip(device_status['timestamp'], device_status['devicestatusid']))
        else:
            device_tuples = None

        # Generate list of tuples for (devicetimestamp, deviceid)
        if treatments is not None:
            treatments_tuples = list(zip(treatments['timestamp'], treatments['treatmentid']))
        else:
            treatments_tuples = None

        # Set constants from index_dict
        index_keys = index_dict.keys()
        max_idx = index_dict.index(index_keys[len(index_keys) - 1])

        if device_tuples is not None:
            for comparison_timestamp, comparison_id in device_tuples:
                # Left idx is the index of the entry timestamp the comparison timestamp is less than or equal to
                try:
                    left_idx = index_dict.bisect_left(comparison_timestamp)
                except TypeError:
                    left_idx = index_dict.bisect_left(comparison_timestamp.replace(tzinfo=pytz.utc))

                # Assign comparison timestamps greater than the last entry to the last entry
                # (Comparisons < min(entry timestamp) will naturally be joined to min(entry timestamp))
                if left_idx >= max_idx:
                    left_idx = max_idx

                # Get the index_dict key associated with the bisect_left operation
                assignment_key = index_keys[left_idx]

                # Assign the comparison_id to the assignment key of the index_dict
                index_dict[assignment_key][1]['device_status'].append(comparison_id)

        if treatments_tuples is not None:
            # Equivalent to the above for-loop but for treatments_tuples
            for comparison_timestamp, comparison_id in treatments_tuples:
                # Left idx is the index of the entry timestamp the comparison timestamp is less than or equal to
                try:
                    left_idx = index_dict.bisect_left(comparison_timestamp)
                except TypeError:
                    left_idx = index_dict.bisect_left(comparison_timestamp.replace(tzinfo=pytz.utc))

                # Assign comparison timestamps greater than the last entry to the last entry
                # (Comparisons < min(entry timestamp) will naturally be joined to min(entry timestamp))
                if left_idx >= max_idx:
                    left_idx = max_idx

                # Get the index_dict key associated with the bisect_left operation
                assignment_key = index_keys[left_idx]

                # Assign the comparison_id to the assignment key of the index_dict
                index_dict[assignment_key][1]['treatment'].append(comparison_id)

        return index_dict

    def _create_temporal_join_df(self, index_dict):
        join_ids = []
        keys = index_dict.keys()
        for k in keys:
            device_ids = index_dict[k][1]['device_status']
            treatment_ids = index_dict[k][1]['treatment']

            zip_lists = []
            # if device_ids and treatment_ids have different lengths...
            if len(device_ids) != len(treatment_ids):
                # ... fill the shorter list with None values
                ids = [device_ids, treatment_ids]
                lengths = [len(i) for i in ids]
                zip_length = max(lengths)

                # Fill in the shortest list
                min_len_idx = lengths.index(min(lengths))
                min_len_ids = ids[min_len_idx]
                min_len_ids.extend([None for _ in range(min_len_idx, zip_length)])

                # identify the longest list
                max_len_idx = lengths.index(max(lengths))
                max_len_ids = ids[max_len_idx]

                # Append all lists to zip_list ordered as (entry_ids, device_ids, treatments_ids)
                zip_lists.append([index_dict[k][0] for _ in range(zip_length)])  # entry Id's

                # Identify the device id list to facilitate appending lists in the correct order
                if max_len_ids == device_ids:
                    zip_lists.append(max_len_ids)  # device_ids
                    zip_lists.append(min_len_ids)  # treatment_ids
                else:
                    zip_lists.append(min_len_ids)  # device_ids
                    zip_lists.append(max_len_ids)  # treatment_ids
            else:
                # Set the zip_length to the length of device_ids
                zip_length = len(device_ids)
                zip_lists.append([index_dict[k][0] for _ in range(zip_length)])  # Entry_ids
                zip_lists.append(device_ids)
                zip_lists.append(treatment_ids)

            # Add the current iteration results
            join_ids.extend(list(zip(*zip_lists)))

        join_dict = {"entryid": [i[0] for i in join_ids],
                     "devicestatusid": [i[1] for i in join_ids],
                     "treatmentid": [i[2] for i in join_ids]}
        join_df = pd.DataFrame(join_dict)
        return join_df


if __name__ == "__main__":
    conn = JoinManager()
    conn.set_head_directory(head_directory='openaps_csvs/')
    ids = conn.identify_subject_ids()
    conn.generate_subject_info()
    conn.generate_subject_classes()
    conn.intra_subject_join_and_put(folder="intrasubject_join", version="0.1")
