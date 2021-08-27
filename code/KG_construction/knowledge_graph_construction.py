from collections import OrderedDict
from datetime import datetime
from k8s_data_handler import K8sDataHandler
from utils.time_handler import TimeHandler
from k8s_data_template_matching import K8sDataTemplateMatching


class KnowledgeGraphConstruction:
    def __init__(self):
        self.k8s_data_handler = K8sDataHandler()

        self.parse_template = self.k8s_data_handler.load_parse_template()
        self.env_dict = self.k8s_data_handler.load_global_env()
        self.node_dict = OrderedDict()
        self.link_list = []
        self.arg_dict = dict()

    def transform_k8s_data(self, inject_time_str, minute_str):
        """
        将k8s-logging中的信息提取并生成图谱
        :param inject_time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :param minute_str: 记录分钟信息的字符串，样例为："34"
        """
        k8s_data = self.k8s_data_handler.load_k8s_logging_data(inject_time_str, minute_str)['data']
        k8s_data['container'] = k8s_data['pod'].copy()
        etime = self.k8s_data_handler.load_k8s_logging_data(inject_time_str, minute_str)['etime']

        result_dict = K8sDataTemplateMatching.template_matching(k8s_data, self.parse_template, self.env_dict, etime)
        self.k8s_data_handler.save_kg_json(inject_time_str, minute_str, result_dict)
