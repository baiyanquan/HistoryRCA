import numpy as np


def average_anomaly_detection(data_list, probable_start_index, probable_end_index):
    """
    检测Metrics是否存在异常
    :param data_list: 需要检测的时间序列
    :param probable_start_index: 怀疑有问题出现的起始位置
    :param probable_end_index: 怀疑有问题出现的终止位置
    :return bool值，即是否存在异常
    """
    normal_average = np.mean(data_list[0:probable_start_index])
    test_average = np.mean(data_list[probable_start_index:probable_end_index])
    if normal_average != 0 and abs(normal_average - test_average)/normal_average > 0.2:
        return True
    else:
        return False
