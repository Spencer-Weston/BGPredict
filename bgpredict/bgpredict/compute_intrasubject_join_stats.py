"""Intermediary file. Calculates a series of statistics by column across all tables that are generated from the
intrasubject join. Writes the result to a pickle object in the misc folder."""

import os
from collections import namedtuple
import pandas as pd
import json
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
            # 14092221 is the largest dataset by far. The process gets killed on this subject due to its size
            # We'll make empty ColInfo's to capture the columns but won't calculate statistics
            if subjectid == "14092221":
                print("Handling 1409221 edge case")
                col_str = "entryid,devicestatusid,treatmentid,time,bg_z,timestamp_y,openaps/iob/lastBolusTime,openaps/iob/basaliob,openaps/iob/activity,openaps/iob/netbasalinsulin,openaps/iob/timestamp,openaps/iob/iob,openaps/iob/bolusinsulin,openaps/iob/lastTemp/duration,openaps/iob/lastTemp/timestamp,openaps/iob/lastTemp/date,openaps/iob/lastTemp/started_at,openaps/iob/lastTemp/rate,openaps/iob/iobWithZeroTemp/basaliob,openaps/iob/iobWithZeroTemp/activity,openaps/iob/iobWithZeroTemp/netbasalinsulin,openaps/iob/iobWithZeroTemp/iob,openaps/iob/iobWithZeroTemp/bolusinsulin,openaps/iob/iobWithZeroTemp/time,openaps/iob/iobWithZeroTemp/bolusiob,openaps/iob/bolusiob,openaps/enacted/reservoir,openaps/enacted/duration,openaps/enacted/deliverAt,openaps/enacted/timestamp,openaps/enacted/received,openaps/enacted/insulinReq,openaps/enacted/IOB,openaps/enacted/tick,openaps/enacted/COB,openaps/enacted/reason,openaps/enacted/eventualBG,openaps/enacted/sensitivityRatio,openaps/enacted/temp,openaps/enacted/bg,openaps/enacted/rate,openaps/suggested/reservoir,openaps/suggested/deliverAt,openaps/suggested/predBGs/IOB/0,openaps/suggested/predBGs/IOB/1,openaps/suggested/predBGs/IOB/2,openaps/suggested/predBGs/IOB/3,openaps/suggested/predBGs/IOB/4,openaps/suggested/predBGs/IOB/5,openaps/suggested/predBGs/IOB/6,openaps/suggested/predBGs/IOB/7,openaps/suggested/predBGs/IOB/8,openaps/suggested/predBGs/IOB/9,openaps/suggested/predBGs/IOB/10,openaps/suggested/predBGs/IOB/11,openaps/suggested/predBGs/IOB/12,openaps/suggested/predBGs/IOB/13,openaps/suggested/predBGs/IOB/14,openaps/suggested/predBGs/IOB/15,openaps/suggested/predBGs/IOB/16,openaps/suggested/predBGs/IOB/17,openaps/suggested/predBGs/IOB/18,openaps/suggested/predBGs/IOB/19,openaps/suggested/predBGs/UAM/0,openaps/suggested/predBGs/UAM/1,openaps/suggested/predBGs/UAM/2,openaps/suggested/predBGs/UAM/3,openaps/suggested/predBGs/UAM/4,openaps/suggested/predBGs/UAM/5,openaps/suggested/predBGs/UAM/6,openaps/suggested/predBGs/UAM/7,openaps/suggested/predBGs/UAM/8,openaps/suggested/predBGs/UAM/9,openaps/suggested/predBGs/UAM/10,openaps/suggested/predBGs/UAM/11,openaps/suggested/predBGs/UAM/12,openaps/suggested/predBGs/UAM/13,openaps/suggested/predBGs/UAM/14,openaps/suggested/predBGs/UAM/15,openaps/suggested/predBGs/UAM/16,openaps/suggested/predBGs/UAM/17,openaps/suggested/predBGs/UAM/18,openaps/suggested/predBGs/UAM/19,openaps/suggested/predBGs/ZT/0,openaps/suggested/predBGs/ZT/1,openaps/suggested/predBGs/ZT/2,openaps/suggested/predBGs/ZT/3,openaps/suggested/predBGs/ZT/4,openaps/suggested/predBGs/ZT/5,openaps/suggested/predBGs/ZT/6,openaps/suggested/predBGs/ZT/7,openaps/suggested/predBGs/ZT/8,openaps/suggested/predBGs/ZT/9,openaps/suggested/predBGs/ZT/10,openaps/suggested/predBGs/ZT/11,openaps/suggested/predBGs/ZT/12,openaps/suggested/predBGs/ZT/13,openaps/suggested/predBGs/ZT/14,openaps/suggested/predBGs/ZT/15,openaps/suggested/predBGs/ZT/16,openaps/suggested/predBGs/ZT/17,openaps/suggested/predBGs/ZT/18,openaps/suggested/predBGs/ZT/19,openaps/suggested/predBGs/ZT/20,openaps/suggested/predBGs/ZT/21,openaps/suggested/predBGs/ZT/22,openaps/suggested/predBGs/ZT/23,openaps/suggested/predBGs/ZT/24,openaps/suggested/predBGs/ZT/25,openaps/suggested/predBGs/ZT/26,openaps/suggested/predBGs/ZT/27,openaps/suggested/predBGs/ZT/28,openaps/suggested/predBGs/ZT/29,openaps/suggested/predBGs/ZT/30,openaps/suggested/predBGs/ZT/31,openaps/suggested/predBGs/ZT/32,openaps/suggested/predBGs/ZT/33,openaps/suggested/predBGs/ZT/34,openaps/suggested/predBGs/ZT/35,openaps/suggested/predBGs/ZT/36,openaps/suggested/predBGs/ZT/37,openaps/suggested/predBGs/ZT/38,openaps/suggested/predBGs/ZT/39,openaps/suggested/predBGs/ZT/40,openaps/suggested/predBGs/ZT/41,openaps/suggested/predBGs/ZT/42,openaps/suggested/predBGs/ZT/43,openaps/suggested/predBGs/ZT/44,openaps/suggested/predBGs/ZT/45,openaps/suggested/predBGs/ZT/46,openaps/suggested/predBGs/ZT/47,openaps/suggested/predBGs/COB/0,openaps/suggested/predBGs/COB/1,openaps/suggested/predBGs/COB/2,openaps/suggested/predBGs/COB/3,openaps/suggested/predBGs/COB/4,openaps/suggested/predBGs/COB/5,openaps/suggested/predBGs/COB/6,openaps/suggested/predBGs/COB/7,openaps/suggested/predBGs/COB/8,openaps/suggested/predBGs/COB/9,openaps/suggested/predBGs/COB/10,openaps/suggested/predBGs/COB/11,openaps/suggested/predBGs/COB/12,openaps/suggested/predBGs/COB/13,openaps/suggested/predBGs/COB/14,openaps/suggested/predBGs/COB/15,openaps/suggested/predBGs/COB/16,openaps/suggested/predBGs/COB/17,openaps/suggested/predBGs/COB/18,openaps/suggested/predBGs/COB/19,openaps/suggested/predBGs/COB/20,openaps/suggested/predBGs/COB/21,openaps/suggested/predBGs/COB/22,openaps/suggested/predBGs/COB/23,openaps/suggested/predBGs/COB/24,openaps/suggested/predBGs/COB/25,openaps/suggested/predBGs/COB/26,openaps/suggested/predBGs/COB/27,openaps/suggested/predBGs/COB/28,openaps/suggested/predBGs/COB/29,openaps/suggested/predBGs/COB/30,openaps/suggested/predBGs/COB/31,openaps/suggested/predBGs/COB/32,openaps/suggested/predBGs/COB/33,openaps/suggested/predBGs/COB/34,openaps/suggested/predBGs/COB/35,openaps/suggested/predBGs/COB/36,openaps/suggested/predBGs/COB/37,openaps/suggested/predBGs/COB/38,openaps/suggested/predBGs/COB/39,openaps/suggested/predBGs/COB/40,openaps/suggested/predBGs/COB/41,openaps/suggested/predBGs/COB/42,openaps/suggested/predBGs/COB/43,openaps/suggested/predBGs/COB/44,openaps/suggested/predBGs/COB/45,openaps/suggested/predBGs/COB/46,openaps/suggested/predBGs/COB/47,openaps/suggested/timestamp,openaps/suggested/insulinReq,openaps/suggested/IOB,openaps/suggested/bg,openaps/suggested/COB,openaps/suggested/reason,openaps/suggested/eventualBG,openaps/suggested/temp,openaps/suggested/tick,openaps/suggested/sensitivityRatio,uploader/batteryVoltage,uploader/battery,device,_id_z,created_at_z,pump/clock,pump/reservoir,pump/battery/voltage,pump/battery/status,pump/status/bolusing,pump/status/suspended,pump/status/timestamp,pump/status/status,openaps/enacted/predBGs/IOB/0,openaps/enacted/predBGs/IOB/1,openaps/enacted/predBGs/IOB/2,openaps/enacted/predBGs/IOB/3,openaps/enacted/predBGs/IOB/4,openaps/enacted/predBGs/IOB/5,openaps/enacted/predBGs/IOB/6,openaps/enacted/predBGs/IOB/7,openaps/enacted/predBGs/IOB/8,openaps/enacted/predBGs/IOB/9,openaps/enacted/predBGs/IOB/10,openaps/enacted/predBGs/IOB/11,openaps/enacted/predBGs/IOB/12,openaps/enacted/predBGs/IOB/13,openaps/enacted/predBGs/IOB/14,openaps/enacted/predBGs/IOB/15,openaps/enacted/predBGs/IOB/16,openaps/enacted/predBGs/IOB/17,openaps/enacted/predBGs/IOB/18,openaps/enacted/predBGs/IOB/19,openaps/enacted/predBGs/UAM/0,openaps/enacted/predBGs/UAM/1,openaps/enacted/predBGs/UAM/2,openaps/enacted/predBGs/UAM/3,openaps/enacted/predBGs/UAM/4,openaps/enacted/predBGs/UAM/5,openaps/enacted/predBGs/UAM/6,openaps/enacted/predBGs/UAM/7,openaps/enacted/predBGs/UAM/8,openaps/enacted/predBGs/UAM/9,openaps/enacted/predBGs/UAM/10,openaps/enacted/predBGs/UAM/11,openaps/enacted/predBGs/UAM/12,openaps/enacted/predBGs/UAM/13,openaps/enacted/predBGs/UAM/14,openaps/enacted/predBGs/UAM/15,openaps/enacted/predBGs/UAM/16,openaps/enacted/predBGs/UAM/17,openaps/enacted/predBGs/UAM/18,openaps/enacted/predBGs/UAM/19,openaps/enacted/predBGs/ZT/0,openaps/enacted/predBGs/ZT/1,openaps/enacted/predBGs/ZT/2,openaps/enacted/predBGs/ZT/3,openaps/enacted/predBGs/ZT/4,openaps/enacted/predBGs/ZT/5,openaps/enacted/predBGs/ZT/6,openaps/enacted/predBGs/ZT/7,openaps/enacted/predBGs/ZT/8,openaps/enacted/predBGs/ZT/9,openaps/enacted/predBGs/ZT/10,openaps/enacted/predBGs/ZT/11,openaps/enacted/predBGs/ZT/12,openaps/enacted/predBGs/ZT/13,openaps/enacted/predBGs/ZT/14,openaps/enacted/predBGs/ZT/15,openaps/enacted/predBGs/ZT/16,openaps/enacted/predBGs/ZT/17,openaps/enacted/predBGs/ZT/18,openaps/enacted/predBGs/ZT/19,openaps/enacted/predBGs/ZT/20,openaps/enacted/predBGs/ZT/21,openaps/enacted/predBGs/ZT/22,openaps/enacted/predBGs/ZT/23,openaps/enacted/predBGs/ZT/24,openaps/enacted/predBGs/ZT/25,openaps/enacted/predBGs/ZT/26,openaps/enacted/predBGs/ZT/27,openaps/enacted/predBGs/ZT/28,openaps/enacted/predBGs/ZT/29,openaps/enacted/predBGs/ZT/30,openaps/enacted/predBGs/ZT/31,openaps/enacted/predBGs/ZT/32,openaps/enacted/predBGs/ZT/33,openaps/enacted/predBGs/ZT/34,openaps/enacted/predBGs/ZT/35,openaps/enacted/predBGs/ZT/36,openaps/enacted/predBGs/ZT/37,openaps/enacted/predBGs/ZT/38,openaps/enacted/predBGs/ZT/39,openaps/enacted/predBGs/ZT/40,openaps/enacted/predBGs/ZT/41,openaps/enacted/predBGs/ZT/42,openaps/enacted/predBGs/ZT/43,openaps/enacted/predBGs/ZT/44,openaps/enacted/predBGs/ZT/45,openaps/enacted/predBGs/ZT/46,openaps/enacted/predBGs/ZT/47,openaps/enacted/predBGs/COB/0,openaps/enacted/predBGs/COB/1,openaps/enacted/predBGs/COB/2,openaps/enacted/predBGs/COB/3,openaps/enacted/predBGs/COB/4,openaps/enacted/predBGs/COB/5,openaps/enacted/predBGs/COB/6,openaps/enacted/predBGs/COB/7,openaps/enacted/predBGs/COB/8,openaps/enacted/predBGs/COB/9,openaps/enacted/predBGs/COB/10,openaps/enacted/predBGs/COB/11,openaps/enacted/predBGs/COB/12,openaps/enacted/predBGs/COB/13,openaps/enacted/predBGs/COB/14,openaps/enacted/predBGs/COB/15,openaps/enacted/predBGs/COB/16,openaps/enacted/predBGs/COB/17,openaps/enacted/predBGs/COB/18,openaps/enacted/predBGs/COB/19,openaps/enacted/predBGs/COB/20,openaps/enacted/predBGs/COB/21,openaps/enacted/predBGs/COB/22,openaps/enacted/predBGs/COB/23,openaps/enacted/predBGs/COB/24,openaps/enacted/predBGs/COB/25,openaps/enacted/predBGs/COB/26,openaps/enacted/predBGs/COB/27,openaps/enacted/predBGs/COB/28,openaps/enacted/predBGs/COB/29,openaps/enacted/predBGs/COB/30,openaps/enacted/predBGs/COB/31,openaps/enacted/predBGs/COB/32,openaps/enacted/predBGs/COB/33,openaps/enacted/predBGs/COB/34,openaps/enacted/predBGs/COB/35,openaps/enacted/predBGs/COB/36,openaps/enacted/predBGs/COB/37,openaps/enacted/predBGs/COB/38,openaps/enacted/predBGs/COB/39,openaps/enacted/predBGs/COB/40,openaps/enacted/predBGs/COB/41,openaps/enacted/predBGs/COB/42,openaps/enacted/predBGs/COB/43,openaps/enacted/predBGs/COB/44,openaps/enacted/predBGs/COB/45,openaps/enacted/predBGs/COB/46,openaps/enacted/predBGs/COB/47,openaps/suggested/duration,openaps/suggested/rate,openaps/suggested/predBGs/IOB/20,openaps/suggested/predBGs/UAM/20,openaps/enacted/predBGs/IOB/20,openaps/enacted/predBGs/IOB/21,openaps/enacted/predBGs/IOB/22,openaps/enacted/predBGs/UAM/20,openaps/enacted/predBGs/UAM/21,openaps/suggested/predBGs/IOB/21,openaps/suggested/predBGs/IOB/22,openaps/suggested/predBGs/IOB/23,openaps/suggested/predBGs/IOB/24,openaps/suggested/predBGs/IOB/25,openaps/suggested/predBGs/IOB/26,openaps/suggested/predBGs/IOB/27,openaps/suggested/predBGs/IOB/28,openaps/suggested/predBGs/IOB/29,openaps/suggested/predBGs/IOB/30,openaps/suggested/predBGs/IOB/31,openaps/suggested/predBGs/IOB/32,openaps/suggested/predBGs/IOB/33,openaps/suggested/predBGs/IOB/34,openaps/suggested/predBGs/IOB/35,openaps/iob,openaps/enacted/predBGs/IOB/23,openaps/enacted/predBGs/IOB/24,openaps/enacted/predBGs/IOB/25,openaps/enacted/predBGs/IOB/26,openaps/enacted/predBGs/IOB/27,openaps/enacted/predBGs/IOB/28,openaps/enacted/predBGs/IOB/29,openaps/enacted/predBGs/IOB/30,openaps/enacted/predBGs/IOB/31,openaps/enacted/predBGs/IOB/32,openaps/enacted/predBGs/IOB/33,openaps/enacted/predBGs/IOB/34,openaps/enacted/predBGs/IOB/35,openaps/suggested/predBGs/IOB/36,openaps/suggested/predBGs/IOB/37,openaps/suggested/predBGs/IOB/38,openaps/suggested/predBGs/IOB/39,openaps/suggested/predBGs/IOB/40,openaps/suggested/predBGs/IOB/41,openaps/enacted/predBGs/IOB/36,openaps/enacted/predBGs/IOB/37,openaps/enacted/predBGs/IOB/38,openaps/enacted/predBGs/IOB/39,openaps/enacted/predBGs/IOB/40,openaps/enacted/predBGs/IOB/41,openaps/enacted/predBGs/IOB/42,openaps/enacted/predBGs/IOB/43,openaps/enacted/predBGs/IOB/44,openaps/enacted/predBGs/IOB/45,openaps/suggested/predBGs/UAM/21,openaps/suggested/predBGs/UAM/22,openaps/suggested/predBGs/UAM/23,openaps/suggested/predBGs/UAM/24,openaps/suggested/predBGs/UAM/25,openaps/suggested/predBGs/UAM/26,openaps/suggested/predBGs/UAM/27,openaps/suggested/predBGs/UAM/28,openaps/suggested/predBGs/UAM/29,openaps/suggested/predBGs/UAM/30,openaps/suggested/predBGs/UAM/31,openaps/suggested/predBGs/UAM/32,openaps/suggested/predBGs/UAM/33,openaps/suggested/predBGs/UAM/34,openaps/enacted/predBGs/UAM/22,openaps/enacted/predBGs/UAM/23,openaps/enacted/predBGs/UAM/24,openaps/enacted/predBGs/UAM/25,openaps/enacted/predBGs/UAM/26,openaps/enacted/predBGs/UAM/27,openaps/enacted/predBGs/UAM/28,openaps/enacted/predBGs/UAM/29,openaps/enacted/predBGs/UAM/30,openaps/enacted/predBGs/UAM/31,openaps/enacted/predBGs/UAM/32,openaps/enacted/predBGs/UAM/33,openaps/enacted/predBGs/UAM/34,openaps/enacted/predBGs/UAM/35,openaps/enacted/predBGs/UAM/36,openaps/enacted/predBGs/UAM/37,openaps/enacted/predBGs/UAM/38,openaps/enacted/predBGs/UAM/39,openaps/enacted/predBGs/UAM/40,openaps/enacted/predBGs/UAM/41,openaps/enacted/predBGs/UAM/42,openaps/suggested/predBGs/IOB/42,openaps/suggested/predBGs/IOB/43,openaps/suggested/predBGs/IOB/44,openaps/suggested/predBGs/IOB/45,openaps/suggested/predBGs/IOB/46,openaps/suggested/predBGs/IOB/47,openaps/suggested/predBGs/UAM/35,openaps/suggested/predBGs/UAM/36,openaps/suggested/predBGs/UAM/37,openaps/suggested/predBGs/UAM/38,openaps/suggested/predBGs/UAM/39,openaps/suggested/predBGs/UAM/40,openaps/suggested/predBGs/UAM/41,openaps/suggested/predBGs/UAM/42,openaps/suggested/predBGs/UAM/43,openaps/suggested/predBGs/UAM/44,openaps/enacted/predBGs/IOB/46,openaps/suggested/predBGs/UAM/45,openaps/suggested/predBGs/UAM/46,openaps/suggested/predBGs/UAM/47,openaps/enacted/predBGs/UAM/43,openaps/enacted/predBGs/UAM/44,openaps/enacted/predBGs/UAM/45,openaps/enacted/predBGs/UAM/46,openaps/enacted/predBGs/UAM/47,openaps/enacted/units,openaps/suggested/units,openaps/enacted/predBGs/IOB/47,openaps/enacted/carbsReq,openaps/suggested/carbsReq,openaps/enacted/requested/temp,openaps/enacted/requested/duration,openaps/enacted/requested/rate,openaps/enacted/recieved,openaps/enacted/minPredBG,openaps/enacted/snoozeBG,uploaderBattery,openaps/iob/microBolusInsulin,openaps/iob/microBolusIOB,openaps/iob/hightempinsulin,openaps/iob/bolussnooze,openaps/suggested/snoozeBG,openaps/suggested/predBGs/aCOB/0,openaps/suggested/predBGs/aCOB/1,openaps/suggested/predBGs/aCOB/2,openaps/suggested/predBGs/aCOB/3,openaps/suggested/predBGs/aCOB/4,openaps/suggested/predBGs/aCOB/5,openaps/suggested/predBGs/aCOB/6,openaps/suggested/predBGs/aCOB/7,openaps/suggested/predBGs/aCOB/8,openaps/suggested/predBGs/aCOB/9,openaps/suggested/predBGs/aCOB/10,openaps/suggested/predBGs/aCOB/11,openaps/suggested/predBGs/aCOB/12,openaps/suggested/predBGs/aCOB/13,openaps/suggested/predBGs/aCOB/14,openaps/suggested/predBGs/aCOB/15,openaps/suggested/predBGs/aCOB/16,openaps/suggested/predBGs/aCOB/17,openaps/suggested/predBGs/aCOB/18,openaps/suggested/predBGs/aCOB/19,openaps/suggested/predBGs/aCOB/20,openaps/suggested/predBGs/aCOB/21,openaps/suggested/predBGs/aCOB/22,openaps/suggested/predBGs/aCOB/23,openaps/suggested/predBGs/aCOB/24,openaps/suggested/predBGs/aCOB/25,openaps/suggested/predBGs/aCOB/26,openaps/suggested/predBGs/aCOB/27,openaps/suggested/predBGs/aCOB/28,openaps/suggested/predBGs/aCOB/29,openaps/suggested/predBGs/aCOB/30,openaps/suggested/predBGs/aCOB/31,openaps/suggested/predBGs/aCOB/32,openaps/suggested/predBGs/aCOB/33,openaps/suggested/predBGs/aCOB/34,openaps/suggested/predBGs/aCOB/35,openaps/suggested/predBGs/aCOB/36,openaps/suggested/predBGs/aCOB/37,openaps/suggested/predBGs/aCOB/38,openaps/suggested/predBGs/aCOB/39,openaps/suggested/predBGs/aCOB/40,openaps/suggested/predBGs/aCOB/41,openaps/suggested/predBGs/aCOB/42,openaps/enacted/predBGs/aCOB/0,openaps/enacted/predBGs/aCOB/1,openaps/enacted/predBGs/aCOB/2,openaps/enacted/predBGs/aCOB/3,openaps/enacted/predBGs/aCOB/4,openaps/enacted/predBGs/aCOB/5,openaps/enacted/predBGs/aCOB/6,openaps/enacted/predBGs/aCOB/7,openaps/enacted/predBGs/aCOB/8,openaps/enacted/predBGs/aCOB/9,openaps/enacted/predBGs/aCOB/10,openaps/enacted/predBGs/aCOB/11,openaps/enacted/predBGs/aCOB/12,openaps/enacted/predBGs/aCOB/13,openaps/enacted/predBGs/aCOB/14,openaps/enacted/predBGs/aCOB/15,openaps/enacted/predBGs/aCOB/16,openaps/enacted/predBGs/aCOB/17,openaps/enacted/predBGs/aCOB/18,openaps/enacted/predBGs/aCOB/19,openaps/enacted/predBGs/aCOB/20,openaps/enacted/predBGs/aCOB/21,openaps/enacted/predBGs/aCOB/22,openaps/enacted/predBGs/aCOB/23,openaps/enacted/predBGs/aCOB/24,openaps/enacted/predBGs/aCOB/25,openaps/enacted/predBGs/aCOB/26,openaps/enacted/predBGs/aCOB/27,openaps/enacted/predBGs/aCOB/28,openaps/enacted/predBGs/aCOB/29,openaps/enacted/predBGs/aCOB/30,openaps/enacted/predBGs/aCOB/31,openaps/enacted/predBGs/aCOB/32,openaps/enacted/predBGs/aCOB/33,openaps/enacted/predBGs/aCOB/34,openaps/enacted/predBGs/aCOB/35,openaps/enacted/predBGs/aCOB/36,openaps/enacted/predBGs/aCOB/37,openaps/enacted/predBGs/aCOB/38,openaps/enacted/predBGs/aCOB/39,openaps/enacted/predBGs/aCOB/40,openaps/enacted/predBGs/aCOB/41,openaps/enacted/predBGs/aCOB/42,openaps/enacted/predBGs/aCOB/43,openaps/enacted/predBGs/aCOB/44,openaps/suggested/predBGs/aCOB/43,openaps/suggested/predBGs/aCOB/44,openaps/suggested/predBGs/aCOB/45,openaps/suggested/predBGs/aCOB/46,openaps/suggested/predBGs/aCOB/47,openaps/enacted/predBGs/aCOB/45,openaps/enacted/predBGs/aCOB/46,openaps/enacted/predBGs/aCOB/47,openaps/suggested/minPredBG,override/timestamp,override/active,loop/predicted/values/0,loop/predicted/values/1,loop/predicted/values/2,loop/predicted/values/3,loop/predicted/values/4,loop/predicted/values/5,loop/predicted/values/6,loop/predicted/values/7,loop/predicted/values/8,loop/predicted/values/9,loop/predicted/values/10,loop/predicted/values/11,loop/predicted/values/12,loop/predicted/values/13,loop/predicted/values/14,loop/predicted/values/15,loop/predicted/values/16,loop/predicted/values/17,loop/predicted/values/18,loop/predicted/values/19,loop/predicted/values/20,loop/predicted/values/21,loop/predicted/values/22,loop/predicted/values/23,loop/predicted/values/24,loop/predicted/values/25,loop/predicted/values/26,loop/predicted/values/27,loop/predicted/values/28,loop/predicted/values/29,loop/predicted/values/30,loop/predicted/values/31,loop/predicted/values/32,loop/predicted/values/33,loop/predicted/values/34,loop/predicted/values/35,loop/predicted/values/36,loop/predicted/values/37,loop/predicted/values/38,loop/predicted/values/39,loop/predicted/values/40,loop/predicted/values/41,loop/predicted/values/42,loop/predicted/values/43,loop/predicted/values/44,loop/predicted/values/45,loop/predicted/values/46,loop/predicted/values/47,loop/predicted/values/48,loop/predicted/values/49,loop/predicted/values/50,loop/predicted/values/51,loop/predicted/values/52,loop/predicted/values/53,loop/predicted/values/54,loop/predicted/values/55,loop/predicted/values/56,loop/predicted/values/57,loop/predicted/values/58,loop/predicted/values/59,loop/predicted/values/60,loop/predicted/values/61,loop/predicted/values/62,loop/predicted/values/63,loop/predicted/values/64,loop/predicted/values/65,loop/predicted/values/66,loop/predicted/values/67,loop/predicted/values/68,loop/predicted/values/69,loop/predicted/values/70,loop/predicted/values/71,loop/predicted/values/72,loop/predicted/values/73,loop/predicted/values/74,loop/predicted/values/75,loop/predicted/values/76,loop/predicted/values/77,loop/predicted/values/78,loop/predicted/values/79,loop/predicted/values/80,loop/predicted/values/81,loop/predicted/startDate,loop/recommendedBolus,loop/timestamp,loop/iob/timestamp,loop/iob/iob,loop/cob/timestamp,loop/cob/cob,loop/name,loop/version,uploader/name,uploader/timestamp,pump/bolusing,pump/model,pump/pumpID,pump/secondsFromGMT,pump/battery/percent,pump/suspended,pump/manufacturer,loop/predicted/values/82,loop/enacted/timestamp,loop/enacted/rate,loop/enacted/received,loop/enacted/duration,loop/failureReason,loop/predicted/values/83,loop/recommendedTempBasal/timestamp,loop/recommendedTempBasal/rate,loop/recommendedTempBasal/duration,override/name,override/currentCorrectionRange/maxValue,override/currentCorrectionRange/minValue,override/duration,override/multiplier,loop/predicted/values/84,loop/predicted/values/85,loop/predicted/values/86,loop/predicted/values/87,loop/predicted/values/88,loop/predicted/values/89,loop/predicted/values/90,loop/predicted/values/91,loop/predicted/values/92,loop/predicted/values/93,loop/predicted/values/94,loop/predicted/values/95,loop/predicted/values/96,loop/predicted/values/97,loop/predicted/values/98,loop/predicted/values/99,loop/predicted/values/100,loop/predicted/values/101,loop/predicted/values/102,loop/predicted/values/103,loop/predicted/values/104,loop/predicted/values/105,loop/predicted/values/106,loop/predicted/values/107,loop/predicted/values/108,loop/predicted/values/109,loop/predicted/values/110,loop/predicted/values/111,uploader/tBatteryValue,uploader/tBatteryColor,uploader/tName,utcOffset_z,openaps/suggested/predBGs/IOB/48,openaps/suggested/predBGs/IOB/49,openaps/suggested/predBGs/IOB/50,openaps/suggested/predBGs/IOB/51,openaps/suggested/predBGs/COB/48,openaps/suggested/predBGs/COB/49,openaps/enacted/predBGs/aCOB/48,openaps/enacted/predBGs/aCOB/49,openaps/enacted/predBGs/aCOB/50,openaps/suggested/predBGs/aCOB/48,openaps/enacted/predBGs/COB/48,openaps/enacted/predBGs/COB/49,openaps/enacted/predBGs/COB/50,openaps/suggested/predBGs/aCOB/49,openaps/suggested/predBGs/aCOB/50,openaps/suggested/predBGs/aCOB/51,openaps/suggested/predBGs/aCOB/52,openaps/suggested/predBGs/COB/50,openaps/suggested/predBGs/COB/51,openaps/suggested/predBGs/COB/52,openaps/suggested/predBGs/COB/53,openaps/suggested/predBGs/COB/54,openaps/suggested/predBGs/aCOB/53,openaps/enacted/predBGs/aCOB/51,openaps/enacted/predBGs/aCOB/52,openaps/enacted/predBGs/aCOB/53,openaps/enacted/predBGs/aCOB/54,openaps/suggested/predBGs/aCOB/54,openaps/enacted/predBGs/COB/51,openaps/enacted/predBGs/COB/52,openaps/enacted/predBGs/COB/53,openaps/enacted/predBGs/COB/54,openaps/enacted/predBGs/IOB/48,openaps/suggested/predBGs/IOB/52,openaps/enacted/predBGs/IOB/49,openaps/enacted/predBGs/IOB/50,openaps/enacted/predBGs/IOB/51,openaps/enacted/predBGs/COB/55,openaps/enacted/predBGs/COB/56,openaps/enacted/predBGs/COB/57,openaps/enacted/predBGs/IOB/52,openaps/enacted/predBGs/IOB/53,openaps/enacted/predBGs/IOB/54,openaps/enacted/predBGs/IOB/55,openaps/enacted/predBGs/IOB/56,openaps/enacted/predBGs/IOB/57,openaps/enacted/predBGs/aCOB/55,openaps/enacted/predBGs/aCOB/56,openaps/enacted/predBGs/aCOB/57,openaps/enacted/predBGs/aCOB/58,openaps/enacted/predBGs/aCOB/59,openaps/enacted/predBGs/aCOB/60,openaps/suggested/predBGs/aCOB/55,openaps/suggested/predBGs/aCOB/56,openaps/suggested/predBGs/aCOB/57,openaps/suggested/predBGs/aCOB/58,openaps/suggested/predBGs/aCOB/59,openaps/suggested/predBGs/aCOB/60,openaps/suggested/predBGs/aCOB/61,openaps/suggested/predBGs/COB/55,openaps/suggested/predBGs/COB/56,openaps/suggested/predBGs/COB/57,openaps/suggested/predBGs/COB/58,openaps/suggested/predBGs/COB/59,openaps/suggested/predBGs/COB/60,openaps/suggested/predBGs/COB/61,openaps/suggested/predBGs/COB/62,openaps/suggested/predBGs/COB/63,openaps/suggested/predBGs/COB/64,openaps/suggested/predBGs/COB/65,openaps/suggested/predBGs/COB/66,openaps/suggested/predBGs/aCOB/62,openaps/suggested/predBGs/aCOB/63,openaps/suggested/predBGs/aCOB/64,openaps/enacted/predBGs/aCOB/61,openaps/enacted/predBGs/aCOB/62,openaps/enacted/predBGs/aCOB/63,openaps/enacted/predBGs/aCOB/64,openaps/enacted/predBGs/aCOB/65,openaps/enacted/predBGs/aCOB/66,openaps/suggested/predBGs/IOB/53,openaps/suggested/predBGs/IOB/54,openaps/suggested/predBGs/IOB/55,openaps/suggested/predBGs/IOB/56,openaps/suggested/predBGs/IOB/57,openaps/suggested/predBGs/IOB/58,openaps/suggested/predBGs/IOB/59,openaps/suggested/predBGs/IOB/60,openaps/suggested/predBGs/IOB/61,openaps/suggested/predBGs/IOB/62,openaps/suggested/predBGs/IOB/63,openaps/enacted/predBGs/COB/58,openaps/enacted/predBGs/COB/59,openaps/enacted/predBGs/COB/60,openaps/enacted/predBGs/COB/61,openaps/enacted/predBGs/COB/62,openaps/enacted/predBGs/COB/63,openaps/enacted/predBGs/COB/64,openaps/enacted/predBGs/COB/65,openaps/enacted/predBGs/COB/66,openaps/enacted/predBGs/IOB/58,openaps/enacted/predBGs/IOB/59,openaps/enacted/predBGs/IOB/60,openaps/enacted/predBGs/IOB/61,openaps/enacted/predBGs/IOB/62,openaps/suggested/predBGs/aCOB/65,openaps/suggested/predBGs/aCOB/66,openaps/suggested/predBGs/IOB/64,timestamp_ds,duration,_id_tre,carbs,eventType,absolute,medtronic,timestamp,created_at_tre,raw_rate/_description,raw_rate/timestamp,raw_rate/_type,raw_rate/_date,raw_rate/_body,raw_rate/_head,raw_rate/temp,raw_rate/rate,insulin,enteredBy,raw_duration/_description,raw_duration/timestamp,raw_duration/_type,raw_duration/_date,raw_duration/duration (min),raw_duration/_body,raw_duration/_head,rate,targetBottom,targetTop,reason,bolus/programmed,bolus/_description,bolus/duration,bolus/amount,bolus/timestamp,bolus/unabsorbed,bolus/_type,bolus/_date,bolus/type,bolus/_body,bolus/_head,notes,ratio,bolus/appended/0/_description,bolus/appended/0/data/0/age,bolus/appended/0/data/0/amount,bolus/appended/0/data/1/age,bolus/appended/0/data/1/amount,bolus/appended/0/data/2/age,bolus/appended/0/data/2/amount,bolus/appended/0/data/3/age,bolus/appended/0/data/3/amount,bolus/appended/0/data/4/age,bolus/appended/0/data/4/amount,bolus/appended/0/data/5/age,bolus/appended/0/data/5/amount,bolus/appended/0/data/6/age,bolus/appended/0/data/6/amount,bolus/appended/0/data/7/age,bolus/appended/0/data/7/amount,bolus/appended/0/data/8/age,bolus/appended/0/data/8/amount,bolus/appended/0/data/9/age,bolus/appended/0/data/9/amount,bolus/appended/0/data/10/age,bolus/appended/0/data/10/amount,bolus/appended/0/data/11/age,bolus/appended/0/data/11/amount,bolus/appended/0/_type,bolus/appended/0/_date,bolus/appended/0/_body,bolus/appended/0/_head,wizard/unabsorbed_insulin_total,wizard/_description,wizard/correction_estimate,wizard/bg_target_high,wizard/bolus_estimate,wizard/timestamp,wizard/_type,wizard/_date,wizard/_body,wizard/food_estimate,wizard/_head,wizard/carb_input,wizard/sensitivity,wizard/bg_target_low,wizard/bg,wizard/carb_ratio,_description,_type,_date,_body,_head,amount,glucose,glucoseType,bg_tre,fixed,type,appended/0/_description,appended/0/data/0/age,appended/0/data/0/amount,appended/0/data/1/age,appended/0/data/1/amount,appended/0/data/2/age,appended/0/data/2/amount,appended/0/data/3/age,appended/0/data/3/amount,appended/0/data/4/age,appended/0/data/4/amount,appended/0/data/5/age,appended/0/data/5/amount,appended/0/data/6/age,appended/0/data/6/amount,appended/0/data/7/age,appended/0/data/7/amount,appended/0/data/8/age,appended/0/data/8/amount,appended/0/data/9/age,appended/0/data/9/amount,appended/0/data/10/age,appended/0/data/10/amount,appended/0/data/11/age,appended/0/data/11/amount,appended/0/_type,appended/0/_date,appended/0/_body,appended/0/_head,bolus/appended/0/data/12/age,bolus/appended/0/data/12/amount,bolus/appended/0/data/13/age,bolus/appended/0/data/13/amount,bolus/appended/0/data/14/age,bolus/appended/0/data/14/amount,bolus/appended/0/data/15/age,bolus/appended/0/data/15/amount,bolus/appended/0/data/16/age,bolus/appended/0/data/16/amount,bolus/appended/0/data/17/age,bolus/appended/0/data/17/amount,bolus/appended/0/data/18/age,bolus/appended/0/data/18/amount,bolus/appended/0/data/19/age,bolus/appended/0/data/19/amount,profile,wizard/appended/0/_description,wizard/appended/0/data/0/age,wizard/appended/0/data/0/amount,wizard/appended/0/data/1/age,wizard/appended/0/data/1/amount,wizard/appended/0/data/2/age,wizard/appended/0/data/2/amount,wizard/appended/0/data/3/age,wizard/appended/0/data/3/amount,wizard/appended/0/data/4/age,wizard/appended/0/data/4/amount,wizard/appended/0/data/5/age,wizard/appended/0/data/5/amount,wizard/appended/0/data/6/age,wizard/appended/0/data/6/amount,wizard/appended/0/data/7/age,wizard/appended/0/data/7/amount,wizard/appended/0/data/8/age,wizard/appended/0/data/8/amount,wizard/appended/0/_type,wizard/appended/0/_date,wizard/appended/0/_body,wizard/appended/0/_head,wizard/appended/0/data/9/age,wizard/appended/0/data/9/amount,bolus/appended/0/data/20/age,bolus/appended/0/data/20/amount,bolus/appended/0/data/21/age,bolus/appended/0/data/21/amount,appended/0/data/12/age,appended/0/data/12/amount,appended/0/data/13/age,appended/0/data/13/amount,stale/carb_ratios/0/x,stale/carb_ratios/0/ratio,stale/carb_ratios/0/offset,stale/carb_ratios/0/start,stale/carb_ratios/0/i,stale/carb_ratios/1/x,stale/carb_ratios/1/ratio,stale/carb_ratios/1/offset,stale/carb_ratios/1/start,stale/carb_ratios/1/i,stale/carb_ratios/2/x,stale/carb_ratios/2/ratio,stale/carb_ratios/2/offset,stale/carb_ratios/2/start,stale/carb_ratios/2/i,stale/carb_ratios/3/x,stale/carb_ratios/3/ratio,stale/carb_ratios/3/offset,stale/carb_ratios/3/start,stale/carb_ratios/3/i,stale/carb_ratios/4/x,stale/carb_ratios/4/ratio,stale/carb_ratios/4/offset,stale/carb_ratios/4/start,stale/carb_ratios/4/i,stale/insulin_sensitivies/0/offset,stale/insulin_sensitivies/0/_offset,stale/insulin_sensitivies/0/i,stale/insulin_sensitivies/0/sensitivity,stale/insulin_sensitivies/1/offset,stale/insulin_sensitivies/1/_offset,stale/insulin_sensitivies/1/i,stale/insulin_sensitivies/1/sensitivity,stale/insulin_sensitivies/2/offset,stale/insulin_sensitivies/2/_offset,stale/insulin_sensitivies/2/i,stale/insulin_sensitivies/2/sensitivity,stale/insulin_sensitivies/3/offset,stale/insulin_sensitivies/3/_offset,stale/insulin_sensitivies/3/i,stale/insulin_sensitivies/3/sensitivity,stale/insulin_sensitivies/4/offset,stale/insulin_sensitivies/4/_offset,stale/insulin_sensitivies/4/i,stale/insulin_sensitivies/4/sensitivity,stale/insulin_sensitivies/5/offset,stale/insulin_sensitivies/5/_offset,stale/insulin_sensitivies/5/i,stale/insulin_sensitivies/5/sensitivity,stale/insulin_sensitivies/6/offset,stale/insulin_sensitivies/6/_offset,stale/insulin_sensitivies/6/i,stale/insulin_sensitivies/6/sensitivity,stale/insulin_sensitivies/7/offset,stale/insulin_sensitivies/7/_offset,stale/insulin_sensitivies/7/i,stale/insulin_sensitivies/7/sensitivity,stale/bg_targets/0/low,stale/bg_targets/0/offset,stale/bg_targets/0/_offset,stale/bg_targets/0/high,stale/bg_targets/1/low,stale/bg_targets/1/offset,stale/bg_targets/1/_offset,stale/bg_targets/1/high,stale/bg_targets/2/low,stale/bg_targets/2/offset,stale/bg_targets/2/_offset,stale/bg_targets/2/high,stale/bg_targets/3/low,stale/bg_targets/3/offset,stale/bg_targets/3/_offset,stale/bg_targets/3/high,stale/bg_targets/4/low,stale/bg_targets/4/offset,stale/bg_targets/4/_offset,stale/bg_targets/4/high,stale/bg_targets/5/low,stale/bg_targets/5/offset,stale/bg_targets/5/_offset,stale/bg_targets/5/high,stale/bg_targets/6/low,stale/bg_targets/6/offset,stale/bg_targets/6/_offset,stale/bg_targets/6/high,stale/bg_targets/7/low,stale/bg_targets/7/offset,stale/bg_targets/7/_offset,stale/bg_targets/7/high,stale/_head,stale/InsulinActionHours,stale/head,tail,changed/carb_ratios/0/x,changed/carb_ratios/0/ratio,changed/carb_ratios/0/offset,changed/carb_ratios/0/start,changed/carb_ratios/0/i,changed/carb_ratios/1/x,changed/carb_ratios/1/ratio,changed/carb_ratios/1/offset,changed/carb_ratios/1/start,changed/carb_ratios/1/i,changed/carb_ratios/2/x,changed/carb_ratios/2/ratio,changed/carb_ratios/2/offset,changed/carb_ratios/2/start,changed/carb_ratios/2/i,changed/carb_ratios/3/x,changed/carb_ratios/3/ratio,changed/carb_ratios/3/offset,changed/carb_ratios/3/start,changed/carb_ratios/3/i,changed/carb_ratios/4/x,changed/carb_ratios/4/ratio,changed/carb_ratios/4/offset,changed/carb_ratios/4/start,changed/carb_ratios/4/i,changed/insulin_sensitivies/0/offset,changed/insulin_sensitivies/0/_offset,changed/insulin_sensitivies/0/i,changed/insulin_sensitivies/0/sensitivity,changed/insulin_sensitivies/1/offset,changed/insulin_sensitivies/1/_offset,changed/insulin_sensitivies/1/i,changed/insulin_sensitivies/1/sensitivity,changed/insulin_sensitivies/2/offset,changed/insulin_sensitivies/2/_offset,changed/insulin_sensitivies/2/i,changed/insulin_sensitivies/2/sensitivity,changed/insulin_sensitivies/3/offset,changed/insulin_sensitivies/3/_offset,changed/insulin_sensitivies/3/i,changed/insulin_sensitivies/3/sensitivity,changed/insulin_sensitivies/4/offset,changed/insulin_sensitivies/4/_offset,changed/insulin_sensitivies/4/i,changed/insulin_sensitivies/4/sensitivity,changed/insulin_sensitivies/5/offset,changed/insulin_sensitivies/5/_offset,changed/insulin_sensitivies/5/i,changed/insulin_sensitivies/5/sensitivity,changed/insulin_sensitivies/6/offset,changed/insulin_sensitivies/6/_offset,changed/insulin_sensitivies/6/i,changed/insulin_sensitivies/6/sensitivity,changed/insulin_sensitivies/7/offset,changed/insulin_sensitivies/7/_offset,changed/insulin_sensitivies/7/i,changed/insulin_sensitivies/7/sensitivity,changed/bg_targets/0/low,changed/bg_targ"
                columns = col_str.split(sep=",")
                for col in columns:
                    col_info = {"subjectid": subjectid, "table_shape": None, "numeric": None, "count": None,
                                "num_unique": None, "num_null": None, "mean": None, "std_dev": None}

                    if col in column_stats:
                        column_stats[col].append(col_info)
                    else:
                        column_stats.update({col: [col_info]})
            else:
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
                    col_info = {"subjectid": subjectid, "table_shape": table_shape, "numeric": numeric, "count": count,
                                "num_unique": num_unique, "num_null": num_null, "mean": mean, "std_dev": std_dev}
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

            # Overwrite each iteration to hopefully save some data
            self.column_stats = column_stats
            with open(f"{self.write_directory}/column_stats.pickle", 'wb') as file:
                pickle.dump(self.column_stats, file, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    head_directory = "intrasubject_joinv0.2/"
    calc = StatCalculator(head_directory, write_directory="../../misc")
    calc.identify_files()
    calc.compute_statistics()
