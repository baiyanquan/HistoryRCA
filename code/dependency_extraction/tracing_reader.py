import os
import json
from utils.injection_reader import InjectionReader
from utils.time_handler import TimeHandler


class TracingReader:
    def __init__(self):
        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        temp_path = os.path.join(os.path.join(temp_path, 'dataset'), 'sock-shop')
        self.raw_tracing_base_path = os.path.join(temp_path, 'raw_tracing')
        self.tracing_base_path = os.path.join(temp_path, 'tracing')
        self.inject_reader = InjectionReader()

    def split_specific_time_data(self, time_str):
        """
        根据特定时间，从原始tracing数据（raw_tracing）中拆分出特定时间段的tracing数据，加快之后分析的速度
        :param time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :return: 拆分得到的span列表，为便于存储，套了一层data的dict
        """
        result_dict = {
            'data': []
        }

        # 设定需要拆分的时间点
        split_start_timestamp = TimeHandler.utc_str_to_timestamp(time_str + ":32:00")
        split_end_timestamp = TimeHandler.utc_str_to_timestamp(time_str + ":36:10")

        tracing_file_path = os.path.join(self.raw_tracing_base_path, 'jaeger-span-' + time_str.split(' ')[0] + '.json')
        if not os.path.exists(tracing_file_path):
            print('Invalid raw tracing file path. Please check the dataset.')

        with open(tracing_file_path, 'r', encoding='utf-8') as f:
            line = f.readline()
            while line:
                span = json.loads(line)
                span_time = int(int(span['_source']['startTime']) / 1000000)
                if split_start_timestamp < span_time < split_end_timestamp:
                    result_dict['data'].append(span['_source'])
                # if span_time - split_end_timestamp > 3600:
                #     break
                line = f.readline()
        return result_dict

    def extract_tracing_data(self):
        """
        根据inject_action.log中的每一次成功混沌实验，从原始tracing数据（raw_tracing）中拆分出特定时间段的tracing数据存储为json文件
        """
        inject_time_list = self.inject_reader.get_injection_time()
        for inject_time in inject_time_list:
            with open(os.path.join(self.tracing_base_path, inject_time + '.json'), 'w') as f:
                json.dump(self.split_specific_time_data(inject_time), f)


t = TracingReader()
t.extract_tracing_data()
