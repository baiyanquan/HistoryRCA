import yaml
import os
import json
from utils.time_handler import TimeHandler


class K8sDataHandler:
    def __init__(self):
        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        self.k8s_template_base_path = os.path.join(temp_path, 'model', 'knowledge_graph_template')
        self.k8s_logging_base_path = os.path.join(temp_path, 'dataset', 'sock-shop', 'k8s-logging')
        self.k8s_result_base_path = os.path.join(temp_path, 'result', 'knowledge_graph')
        if not os.path.exists(self.k8s_result_base_path):
            os.mkdir(self.k8s_result_base_path)

        self.resource_type_list = ['node', 'service', 'deployment', 'pod', 'container']

    def load_k8s_logging_data(self, inject_time_str, minute_str):
        """
        加载特定实验中某一分钟（数据按分钟为单位收集）的k8s-logging数据
        :param inject_time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :param minute_str: 记录分钟信息的字符串，样例为："34"
        :return dict，key为运维实体种类，value为运维实体详细信息的list
        """
        result_dict = dict()

        inject_k8s_logging_base_path = os.path.join(self.k8s_logging_base_path, inject_time_str)
        for k8s_logging_file in os.listdir(inject_k8s_logging_base_path):
            if inject_time_str + '-' + minute_str in k8s_logging_file:
                inject_k8s_logging_file_path = os.path.join(inject_k8s_logging_base_path, k8s_logging_file)
                with open(inject_k8s_logging_file_path) as f:
                    result_dict['data'] = json.load(f)
                    raw_time_str = k8s_logging_file.split('.')[0]
                    time_str = raw_time_str.split(' ')[0] + ' ' + raw_time_str.split('.')[0].split(' ')[1].replace('-', ':')
                    result_dict['etime'] = TimeHandler.utc_str_to_timestamp(time_str)
                    return result_dict

        return result_dict

    def load_global_env(self):
        """
        加载全局环境变量
        """
        result_dict = dict()
        with open(os.path.join(self.k8s_template_base_path, 'global_env.yaml'), 'r', encoding='utf-8') as f:
            result_dict = yaml.load(f.read(), Loader=yaml.FullLoader)
        return result_dict

    def load_parse_template(self):
        """
        加载解析k8s-logging的yaml模板
        """
        result_dict = dict()
        for resource_type in self.resource_type_list:
            with open(os.path.join(self.k8s_template_base_path, resource_type + '.yaml'), 'r', encoding='utf-8') as f:
                result_dict[resource_type] = yaml.load(f.read(), Loader=yaml.FullLoader)
        return result_dict

    def save_kg_json(self, inject_time_str, minute_str, kg_dict):
        """
        存储转换后的图谱json数据
        :param inject_time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :param minute_str: 记录分钟信息的字符串，样例为："34"
        :param kg_dict: 图谱信息
        """
        kg_result_base_path = os.path.join(self.k8s_result_base_path, inject_time_str)
        if not os.path.exists(kg_result_base_path):
            os.makedirs(kg_result_base_path)

        with open(os.path.join(kg_result_base_path, inject_time_str + '-' + minute_str + '.json'), 'w') as f:
            json.dump(kg_dict, f, indent=4)
