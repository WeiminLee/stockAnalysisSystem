# -*- encoding: utf-8 -*-
# user:LWM
import collections
import datetime
import os

import pandas as pd

import DateUtil
import matplotlib.pyplot as plt

guba_dir = r'D:\LearningProgram\data\guba_url'

for file_name in os.listdir(guba_dir):
    df = pd.read_csv(os.path.join(guba_dir, file_name))
    df.sort_values(by=['index'], inplace=True)
    reply_time = df['replttime'].values.tolist()
    pages = max(df['index'].values.tolist())

    result_date_time = []
    for pag in range(pages):
        reply_time = df[df['index']==pag]['replttime'].values.tolist()
        cnt = collections.defaultdict(int)
        for idx, time in enumerate(reply_time):
            this_month = reply_time[idx].split("-")[0]
            cnt[this_month] += 1
        res = list(sorted(cnt.items(),key=lambda x:x[1]))

        result_date_time.append(res[-1][0])

    plt.yticks([x for x in range(1, 13, 1)])
    plt.plot(list(range(len(result_date_time))),result_date_time)

    plt.show()




# 获取2023年的数据，stop_idx用来定位2022年的6-7月
# stop_idx = 0
# for idx, time in enumerate(reply_time):
#     this_month = reply_time[idx].split("-")[0]
#     if this_month == '07':
#         stop_idx = idx
#         break
# for i in range(stop_idx):
#     this_month = reply_time[i].split("-")[0]
#     if this_month == '01' or this_month == '02' or this_month == '03':
#         result_date_time.append('2023-' + reply_time[i])
#     else:
#         result_date_time.append(reply_time[i])
#
# # 开始定位2022年，end_idx 用来定位2021年
# end_idx = 0
# for i in range(stop_idx, len(reply_time)):
#     this_month = reply_time[i].split("-")[0]
#     if this_month == '09':
#         end_idx = i
#         break
# for j in range(0, stop_idx):
#     if len(result_date_time[j].split('-')) == 3:
#         continue
#     else:
#         result_date_time[j] = '2022-' + result_date_time[j]
# for k in range(stop_idx, end_idx):
#     this_month = reply_time[k].split("-")[0]
#     if this_month >= '01' or this_month <= '07':
#         result_date_time.append('2022-' + reply_time[k])
#     else:
#         result_date_time.append(reply_time[k])
#
# # 定位2021年的
# do_idx = 0
# for i in range(end_idx, len(reply_time)):
#     this_month = reply_time[i].split("-")[0]
#     if this_month == '11':
#         do_idx = i
#         break
# for j in range(stop_idx, end_idx):
#     if len(result_date_time[j].split('-')) == 3:
#         continue
#     else:
#         result_date_time[j] = '2021-' + result_date_time[j]
# for k in range(end_idx, do_idx):
#     this_month = reply_time[k].split("-")[0]
#     if this_month >= '01' or this_month <= '09':
#         result_date_time.append('2021-' + reply_time[k])
#     else:
#         result_date_time.append(reply_time[k])
#
# break
