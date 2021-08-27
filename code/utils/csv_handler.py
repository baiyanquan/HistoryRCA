import pandas as pd
import numpy as np
import os
import ast


def csv_data_read(csv_file_path):
    """
    读取csv数据
    :param csv_file_path: 读取的csv文件路径
    :return: dict，以列名为key，值列表为value
    """
    csv_file = pd.read_csv(csv_file_path)
    csv_data = {}
    if len(csv_file) != 0:
        list_label = csv_file.columns.values
        for label in list_label:
            if label == "datetime":
                continue
            else:
                csv_data[label] = csv_file[label].values
    return csv_data


def preprocess(data_list):
    """
    数据清洗与补全
    :param data_list: 需要补全的数据list
    :return: dict，其中包含两个key，is_valid表示此数据是否有效，对应bool类型的value；data对应补全后的数据list
    """
    result_dict = {
        'is_valid': True,
        'data': []
    }

    # 无效值（非数字值）的比例超出0.5，认为此数据失效
    nan_count = 0
    for i in data_list:
        if np.isnan(i):
            nan_count += 1
    if nan_count > 0.5 * len(data_list):
        result_dict['is_valid'] = False
        return result_dict

    # 连续缺失长度达到阈值，用0填补；未达到阈值，在两端用最近的有效值填补，中间用插值法填补
    lost_threshold = 6

    i = 0
    j = 0
    while j != len(data_list):
        if not np.isnan(data_list[i]):
            i = i + 1
            j = i
        else:
            while j < len(data_list) and np.isnan(data_list[j]):
                j = j + 1
            if j - i <= lost_threshold:
                if i == 0:
                    for k in range(i, j):
                        data_list[k] = data_list[j]
                elif j == len(data_list):
                    for k in range(i, j):
                        data_list[k] = data_list[i - 1]
                else:
                    divide = (data_list[j] - data_list[i - 1]) / (j - i + 1)
                    for k in range(i, j):
                        data_list[k] = data_list[i - 1] + (k - i + 1) * divide
            else:
                for k in range(i, j):
                    data_list[k] = 0
    result_dict['data'] = data_list
    return result_dict
