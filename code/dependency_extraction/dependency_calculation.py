import os
import json
from utils.injection_reader import InjectionReader
from utils.time_handler import TimeHandler
from tracing_reader import TracingReader


class DependencyCalculation:
    def __init__(self):
        self.inject_reader = InjectionReader()
        self.tracing_reader = TracingReader()

        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        temp_path = os.path.join(os.path.join(temp_path, 'dataset'), 'sock-shop')
        self.tracing_base_path = os.path.join(temp_path, 'tracing')

        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        temp_path = os.path.join(temp_path, 'result')
        if not os.path.exists(temp_path):
            os.mkdir(temp_path)
        self.result_base_path = os.path.join(temp_path, 'service_dependency')
        if not os.path.exists(self.result_base_path):
            os.mkdir(self.result_base_path)

    def tracing_preparation(self):
        """
        预处理得到依赖度提取所需的tracing数据
        """
        self.tracing_reader.extract_tracing_data()

    def dependency_calculation(self, time_str, start_timestamp, end_timestamp):
        """
        根据特定时间，从原始tracing数据（raw_tracing）中拆分出特定时间段的tracing数据，加快之后分析的速度
        :param time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :param start_timestamp: 分析的起始时间戳
        :param end_timestamp: 分析的终止时间戳
        :return: dict，包含以dependency（依赖度）和duration（平均时长）的两个子字典，每个子字典以“调用方服务名-被调用方服务名”为key
        """
        result_dict = {
            'dependency': {},
            'duration': {}
        }
        tracing_file_path = os.path.join(self.tracing_base_path, time_str + '.json')
        if not os.path.exists(tracing_file_path):
            print('Invalid tracing file path. Please check the dataset and try to extract tracing data.')
        span_list = []
        with open(tracing_file_path, 'r', encoding='utf-8') as f:
            for span in json.load(f)['data']:
                span_time = int(int(span['startTime']) / 1000000)
                if start_timestamp < span_time < end_timestamp:
                    span_list.append(span)
        span_info_dict = dict()

        # extract span info
        for span in span_list:
            span_info_dict[span['traceID'] + '-' + span['spanID']] = {
                'operationName': span['operationName'],
                'serviceName': span['process']['serviceName'],
                'duration': int(span['duration'])
            }

        # merge from tracing
        frequency_dict = dict()
        duration_dict = dict()
        for span in span_list:
            if len(span['references']) == 1 and span['references'][0]['refType'] == 'CHILD_OF':
                parent_span_info_id = span['references'][0]['traceID'] + '-' + span['references'][0]['spanID']
                if parent_span_info_id not in span_info_dict:
                    continue
                correlation_key = span_info_dict[parent_span_info_id]['serviceName'] + '->' + span['process'][
                    'serviceName']
                duration = span_info_dict[parent_span_info_id]['duration'] - int(span['duration'])
                if correlation_key in frequency_dict.keys():
                    if span_info_dict[parent_span_info_id]['serviceName'] != span['process']['serviceName']:
                        frequency_dict[correlation_key] += 1
                        duration_dict[correlation_key] += duration
                else:
                    if span_info_dict[parent_span_info_id]['serviceName'] != span['process']['serviceName']:
                        frequency_dict[correlation_key] = 1
                        duration_dict[correlation_key] = duration

        sum_dict = dict()
        for correlation_key in frequency_dict.keys():
            if correlation_key.split('-')[0] in sum_dict.keys():
                sum_dict[correlation_key.split('-')[0]] += frequency_dict[correlation_key]
            else:
                sum_dict[correlation_key.split('-')[0]] = frequency_dict[correlation_key]

        correlation_dict = dict()
        for service_key in sum_dict.keys():
            for correlation_key in frequency_dict.keys():
                if service_key + '-' in correlation_key:
                    if service_key in correlation_dict:
                        correlation_dict[service_key].append({
                            'correlation': correlation_key,
                            'value': frequency_dict[correlation_key]
                        })
                    else:
                        correlation_dict[service_key] = [{
                            'correlation': correlation_key,
                            'value': frequency_dict[correlation_key]
                        }]
        for service_key in correlation_dict.keys():
            for index in range(len(correlation_dict[service_key]) - 1):
                correlation_dict[service_key][index]['value'] = correlation_dict[service_key][index]['value'] / sum_dict[service_key]
            correlation_dict[service_key][len(correlation_dict[service_key]) - 1]['value'] = 1
            for index in range(len(correlation_dict[service_key]) - 1):
                correlation_dict[service_key][len(correlation_dict[service_key]) - 1]['value'] -= \
                    correlation_dict[service_key][index]['value']
        for service_key, value in correlation_dict.items():
            for item in value:
                result_dict['dependency'][item['correlation']] = item['value']
        for key, value in duration_dict.items():
            result_dict['duration'][key] = int(value/(end_timestamp - start_timestamp))
        return result_dict

    def extract_dependency(self, preparation=False):
        """
        根据inject_action.log中的每一次成功混沌实验，从提取后的tracing数据中计算服务间的依赖度并存储为json文件
        :param preparation: 是否需要预处理得到基础的tracing数据，默认为不处理
        """
        if preparation:
            self.tracing_preparation()

        inject_time_list = self.inject_reader.get_injection_time()
        for inject_time in inject_time_list:
            correlation_dict = {
                "before": self.dependency_calculation(inject_time,
                                                      TimeHandler.utc_str_to_timestamp(inject_time + ":34:00"),
                                                      TimeHandler.utc_str_to_timestamp(inject_time + ":35:00")),
                "after": self.dependency_calculation(inject_time,
                                                     TimeHandler.utc_str_to_timestamp(inject_time + ":35:10"),
                                                     TimeHandler.utc_str_to_timestamp(inject_time + ":36:10"))
            }
            with open(os.path.join(self.result_base_path, inject_time + '.json'), 'w') as f:
                json.dump(correlation_dict, f, indent=4)
