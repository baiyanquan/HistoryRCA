from data_handler import DataHandler
from path_extraction import PathExtraction
from utils.injection_reader import InjectionReader


class AnomalyKnowledgeExtractionApp:
    def __init__(self):
        pass

    @staticmethod
    def main():
        anomaly_knowledge_dict = dict()

        injection_reader = InjectionReader()
        data_handler = DataHandler()
        path_extraction = PathExtraction()

        inject_dict = injection_reader.get_injection_data()
        topology_knowledge_dict = data_handler.load_topology_knowledge()
        for inject_time_str, inject_action_dict in inject_dict.items():
            anomaly_knowledge_dict[inject_time_str] = []
            base_kg_dict = data_handler.load_knowledge_graph(inject_time_str)
            service_dependency_dict = data_handler.load_service_dependency(inject_time_str)
            kg_dict = {
                'before': dict(),
                'after': dict()
            }
            for key in kg_dict.keys():
                kg_dict[key] = DataHandler.info_mix(base_kg_dict[key], topology_knowledge_dict,
                                                    service_dependency_dict[key]['dependency'])

            metrics_feature_list = data_handler.load_metrics_feature(inject_time_str)

            node_list = {
                'before': [],
                'after': []
            }
            link_list = {
                'before': [],
                'after': []
            }
            for key in kg_dict.keys():
                for resource_type in kg_dict[key]['links']:
                    link_list[key].extend(kg_dict[key]['links'][resource_type])
                for resource_type in kg_dict[key]['nodes']:
                    node_list[key].extend(kg_dict[key]['nodes'][resource_type])

            for metrics_feature in metrics_feature_list:
                relation_list = []
                if '::anomaly' in metrics_feature:
                    head_entity_list = PathExtraction.extract_head_entity(metrics_feature, kg_dict['before']['nodes'])
                    tail_entity_list = PathExtraction.extract_tail_entity(inject_action_dict, kg_dict['after']['nodes'])
                    path_is_find = False
                    for head_entity in head_entity_list:
                        for tail_entity in tail_entity_list:
                            before_path_dict = path_extraction.path_searching(head_entity['id'], tail_entity['id'],
                                                                              link_list['before'])
                            after_path_dict = path_extraction.path_searching(head_entity['id'], tail_entity['id'],
                                                                             link_list['after'])
                            if len(before_path_dict['node_list']) == 0 or len(after_path_dict['node_list']) == 0:
                                continue
                            else:
                                path_is_find = True
                                influence_path = PathExtraction.path_identification(before_path_dict, after_path_dict)
                                relation_list = PathExtraction.path_id_to_name(node_list['before'], influence_path['node_list'])
                                break

                    if not path_is_find:
                        relation_list = ['unknown_effected_by']
                else:
                    relation_list = ['not_effected_by']

                knowledge_dict = {
                    'head': metrics_feature,
                    'relation': relation_list,
                    'tail': inject_action_dict['type']
                }
                anomaly_knowledge_dict[inject_time_str].append(knowledge_dict)
        data_handler.save_anomaly_knowledge(anomaly_knowledge_dict)


if __name__ == '__main__':
    AnomalyKnowledgeExtractionApp.main()
