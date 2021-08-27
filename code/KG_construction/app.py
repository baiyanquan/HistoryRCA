from knowledge_graph_construction import KnowledgeGraphConstruction
from utils.injection_reader import InjectionReader


if __name__ == '__main__':
    before_minute_str = '34'
    after_minute_str = '36'

    inject_reader = InjectionReader()
    inject_time_list = inject_reader.get_injection_time()

    kg_constructor = KnowledgeGraphConstruction()

    for inject_time in inject_time_list:
        kg_constructor.transform_k8s_data(inject_time, before_minute_str)
        kg_constructor.transform_k8s_data(inject_time, after_minute_str)
