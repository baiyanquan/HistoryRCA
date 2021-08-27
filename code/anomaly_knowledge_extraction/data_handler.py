import os
import json


class DataHandler:
    def __init__(self):
        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        self.topology_knowledge_path = os.path.join(temp_path, 'model', 'dependency', 'topology-knowledge.json')
        self.knowledge_graph_base_path = os.path.join(temp_path, 'result', 'knowledge_graph')
        self.service_dependency_base_path = os.path.join(temp_path, 'result', 'service_dependency')
        self.metrics_feature_path = os.path.join(temp_path, 'result', 'metrics_cluster', 'metrics_feature', 'TimeSeriesKMeans---dtw.json')
        self.anomaly_knowledge_base_path = os.path.join(temp_path, 'result', 'anomaly_knowledge')
        if not os.path.exists(self.anomaly_knowledge_base_path):
            os.makedirs(self.anomaly_knowledge_base_path)

    def load_topology_knowledge(self):
        """
        加载service间的拓扑关系知识（根据架构图构建）
        :return dict
        """
        result_dict = dict()
        with open(self.topology_knowledge_path) as f:
            result_dict = json.load(f)
        return result_dict

    def load_knowledge_graph(self, inject_time_str, before_minute_str='34', after_minute_str='36'):
        """
        加载特定实验的运维知识图谱
        :param inject_time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :param before_minute_str: 记录异常发生前图谱分钟信息的字符串，样例为："34"
        :param after_minute_str: 记录异常发生后图谱分钟信息的字符串，样例为："36"
        :return dict，包含before和after两个key，分别对应实验前（默认为34分）和实验后（默认为36分）的图谱
        """
        result_dict = {
            'before': dict(),
            'after': dict()
        }

        with open(os.path.join(self.knowledge_graph_base_path, inject_time_str, inject_time_str + '-' + before_minute_str + '.json')) as f:
            result_dict['before'] = json.load(f)
        with open(os.path.join(self.knowledge_graph_base_path, inject_time_str, inject_time_str + '-' + after_minute_str + '.json')) as f:
            result_dict['after'] = json.load(f)

        return result_dict

    def load_service_dependency(self, inject_time_str):
        """
        加载特定实验中根据调用链提取的依赖关系
        :param inject_time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :return dict，包含before和after两个key，分别对应实验前和实验后的依赖关系
        """
        result_dict = dict()
        with open(os.path.join(self.service_dependency_base_path, inject_time_str + '.json')) as f:
            result_dict = json.load(f)
        return result_dict

    def load_metrics_feature(self, inject_time_str):
        """
        加载特定实验中提取得到的metrics语义表达
        :param inject_time_str: 从injection_action.log中提取的时间字符串，样例为: "2021-07-18 12"
        :return list，即语义表达构成的列表
        """
        result_list = []
        with open(self.metrics_feature_path) as f:
            result_list = json.load(f)[inject_time_str]
        return result_list

    @staticmethod
    def info_mix(kg_dict, topology_knowledge_dict, service_dependency_dict):
        """
        将相关信息融合在知识图谱中
        :param kg_dict: 从k8s-logging中分析得到的运维知识图谱
        :param topology_knowledge_dict: 从架构图中获取的拓扑知识
        :param service_dependency_dict: 从tracing中分析得到的服务依赖关系
        :return dict，混合信息后的运维知识图谱
        """
        for key in kg_dict['links'].keys():
            for index in range(len(kg_dict['links'][key])):
                kg_dict['links'][key][index]['weight'] = 1

        service_name_dict = dict()
        for service_info in kg_dict['nodes']['service']:
            service_name_dict[service_info['name']] = service_info['id']

        kg_dict['links']['calls'] = []
        for key in service_dependency_dict.keys():
            s_service_name = 'http://k8s/service/' + key.split('->')[0]
            t_service_name = 'http://k8s/service/' + key.split('->')[1]
            kg_dict['links']['calls'].append({
                'hid': service_name_dict[s_service_name],
                'tid': service_name_dict[t_service_name],
                'name': 'calls',
                'label': 'calls',
                'weight': service_dependency_dict[key]
            })

        kg_dict['links']['relies_on'] = []
        for key in topology_knowledge_dict.keys():
            s_service_name = 'http://k8s/service/' + key.split('->')[0]
            t_service_name = 'http://k8s/service/' + key.split('->')[1]
            kg_dict['links']['relies_on'].append({
                'hid': service_name_dict[s_service_name],
                'tid': service_name_dict[t_service_name],
                'name': 'relies_on',
                'label': 'relies_on',
                'weight': topology_knowledge_dict[key]
            })

        return kg_dict

    def save_anomaly_knowledge(self, anomaly_knowledge_list):
        with open(os.path.join(self.anomaly_knowledge_base_path, 'anomaly_knowledge.json'), 'w') as f:
            json.dump(anomaly_knowledge_list, f, indent=4)



