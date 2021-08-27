import os
from utils.injection_reader import InjectionReader
from utils.csv_handler import csv_data_read, preprocess
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
        csv_file_path = os.path.join(self.metrics_base_path, str(time_str).replace(' ', '_').replace('-', '')
                                     + "3000_SockShopPerformance.csv")
        csv_data = csv_data_read(csv_file_path)
        l_index = 0
        r_index = 0
        for index in range(len(csv_data['timestamp'])):
            if csv_data['timestamp'][index] <= start_timestamp:
                l_index = index
            if csv_data['timestamp'][index] <= end_timestamp:
                r_index = index
            if csv_data['timestamp'][index] > end_timestamp:
                break
        for key, value in csv_data.items():
            preprocess_result_dict = preprocess(value[l_index:r_index])
            if preprocess_result_dict['is_valid']:
                csv_data[key] = preprocess_result_dict['data']
            else:
                csv_data[key] = []
        return csv_data

    def chaos_metrics_reader(self, start_time_str=':34:00', end_time_str=':37:00'):
        """
        根据inject_action.log中有效的混沌实验时间，设定截取时间字符串获取指定时间段内的metrics数据
        :param start_time_str: 截取的起始时间字符串，样例为':34:00'
        :param end_time_str: 截取的终止时间字符串，样例为':37:00'
        :return: dict，以混沌实验时间为key值，value为在指定时间段内的metrics dict
        """
        result_dict = {
            'metrics_data': dict(),
            'metrics_name_list': []
        }
        inject_time_list = self.inject_reader.get_injection_time()
        for inject_time in inject_time_list:
            result_dict['metrics_data'][inject_time] = self.data_loader(inject_time,
                                                                        TimeHandler.utc_str_to_timestamp(inject_time + start_time_str),
                                                                        TimeHandler.utc_str_to_timestamp(inject_time + end_time_str))
            if len(result_dict['metrics_name_list']) == 0:
                result_dict['metrics_name_list'] = list(result_dict['metrics_data'][inject_time].keys())
        return result_dict


if __name__ == '__main__':
    m = MetricsReader()
    m.chaos_metrics_reader()
