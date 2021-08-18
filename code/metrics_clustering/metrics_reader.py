import os
from utils.injection_reader import InjectionReader
from utils.time_handler import TimeHandler


class MetricsReader:
    def __init__(self):
        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        temp_path = os.path.join(os.path.join(temp_path, 'dataset'), 'sock-shop')
        self.metrics_base_path = os.path.join(temp_path, 'metrics')
        self.inject_reader = InjectionReader()

    def data_loader(self, time_str, start_timestamp, end_timestamp):
        """
        根据特定混沌实验时间，读取对应的metrics数据
        :param time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :param start_timestamp: 读取的起始时间戳
        :param end_timestamp: 读取的终止时间戳
        :return: dict，以metrics名称为key值，value为在指定时间段内的metrics值序列
        """
        result_dict = dict()



