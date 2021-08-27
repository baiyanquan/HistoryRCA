from collections import deque


class PathExtraction:
    def __init__(self):
        self.rule_dict = {
            'provides': 'reverse',
            'supports': 'reverse',
            'contains': 'both',
            'is_deployed_in': 'forward',
            'relies_on': 'forward',
            'calls': 'both'
        }

    def path_searching(self, head_entity_id, tail_entity_id, link_list):
        """
        给定头实体id和尾实体id，根据定义的关系方向从关系列表中找到全部影响路径
        :param head_entity_id: 头实体id
        :param tail_entity_id: 尾实体id
        :param link_list: 图谱中的关系列表
        :return dict，key:link_list为路径的详细信息，key:node_list为路径上的实体信息（简要信息）
        """
        result_dict = {
            'link_list': [],
            'node_list': []
        }

        link_stack = deque()
        node_stack = deque()
        node_stack.append(head_entity_id)
        visited = set()
        visited.add(head_entity_id)

        seen_path = dict()

        while len(node_stack) > 0:
            head_entity_id = node_stack[-1]

            if head_entity_id not in seen_path:
                seen_path[head_entity_id] = []

            is_pop = True
            for link in link_list:
                if link['hid'] == head_entity_id and link['tid'] not in visited and link['tid'] not in seen_path[
                    head_entity_id]:
                    if self.rule_dict[link['name']] != 'reverse':
                        is_pop = False
                        link_stack.append(link)
                        node_stack.append(link['tid'])
                        visited.add(link['tid'])

                        seen_path[head_entity_id].append(link['tid'])

                        if link['tid'] == tail_entity_id:
                            result_dict['link_list'].append(list(link_stack))
                            result_dict['node_list'].append(list(node_stack))
                            if len(link_stack) > 0:
                                link_stack.pop()
                            visited.remove(node_stack.pop())
                        break

                elif link['tid'] == head_entity_id and link['hid'] not in visited and link['hid'] not in seen_path[
                    head_entity_id]:
                    if self.rule_dict[link['name']] != 'forward':
                        is_pop = False
                        link_stack.append(link)
                        node_stack.append(link['hid'])
                        visited.add(link['hid'])

                        seen_path[head_entity_id].append(link['hid'])

                        if link['hid'] == tail_entity_id:
                            result_dict['link_list'].append(list(link_stack))
                            result_dict['node_list'].append(list(node_stack))
                            if len(link_stack) > 0:
                                link_stack.pop()
                            visited.remove(node_stack.pop())
                        break

            if is_pop:
                pop_id = node_stack.pop()
                visited.remove(pop_id)
                seen_path.pop(pop_id)
                if len(link_stack) > 0:
                    link_stack.pop()

        return result_dict

    @staticmethod
    def path_identification(before_path_dict, after_path_dict):
        """
        从提取到的路径中，找到最有可能的影响路径
        :param before_path_dict: 根据path_searching找到的实验前的路径信息
        :param after_path_dict: 根据path_searching找到的实验后的路径信息
        :return dict，包含node_list和link_list，组合起来为影响路径
        """
        result_dict = dict()

        before_after_corresponding_dict = dict()
        for before_index in range(len(before_path_dict['node_list'])):
            before_after_corresponding_dict[before_index] = ''
            for after_index in range(len(after_path_dict['node_list'])):
                if before_path_dict['node_list'][before_index] == after_path_dict['node_list'][after_index]:
                    before_after_corresponding_dict[before_index] = after_index
                    break

        potential_link_list = []
        potential_node_list = []
        for before_index, after_index in before_after_corresponding_dict.items():
            if after_index == '':
                potential_link_list.append(before_path_dict['link_list'][before_index])
                potential_node_list.append(before_path_dict['node_list'][before_index])
        # 原有路径消失
        if len(potential_link_list) > 0:
            # 优先选跳数短的
            path_length = 0
            index_list = []
            for index in range(len(potential_link_list)):
                if len(potential_link_list[index]) > path_length:
                    index_list = [index]
                    path_length = len(potential_link_list[index])
                elif len(potential_link_list[index]) == path_length:
                    index_list.append(index)
            # 跳数相同的优先选权重高的
            total_weight = 0
            final_index = 0
            for index in index_list:
                if PathExtraction.calculate_path_weight(potential_link_list[index]) > total_weight:
                    final_index = index
                    total_weight = PathExtraction.calculate_path_weight(potential_link_list[index])
            result_dict = {
                'link_list': potential_link_list[final_index],
                'node_list': potential_node_list[final_index]
            }
        # 不存在路径消失的情况
        else:
            potential_link_list = before_path_dict['link_list']
            potential_node_list = before_path_dict['node_list']
            # 取影响因子（平均变化度*平均权重）最高的路径
            base_factor = 0
            final_index = 0
            for index in range(len(potential_link_list)):
                temp_factor = PathExtraction.calculate_path_factor(potential_link_list[index], after_path_dict[
                    'link_list'][before_after_corresponding_dict[index]])
                if temp_factor > base_factor:
                    final_index = index
                    base_factor = temp_factor
            result_dict = {
                'link_list': potential_link_list[final_index],
                'node_list': potential_node_list[final_index]
            }

        return result_dict

    @staticmethod
    def extract_head_entity(metric_feature_expression, nodes_dict):
        """
        从给定的实体列表中解析指标所在的受影响的（头）实体
        :param metric_feature_expression: 记录metrics特征的语义表达，样例为：service/carts/qps(2xx)::anomaly-3
        :param nodes_dict: 运维实体dict，key为实体类别，value为实体信息列表
        :return list（可能有多个受影响的实体），每个元素为实体信息的dict
        """
        result_list = []
        metric_name = metric_feature_expression.split('::')[0]
        resource_type = metric_name.split('/')[0]
        resource_info = metric_name.split('/')[1]
        for resource_item in nodes_dict[resource_type]:
            if resource_type == 'node':
                if resource_item['property']['query']['resource']['nodeName'] in resource_info:
                    result_list.append(resource_item.copy())
            else:
                if 'http://k8s/' + resource_type + '/' + resource_info == resource_item['name']:
                    result_list.append(resource_item.copy())
        return result_list

    @staticmethod
    def extract_tail_entity(inject_action_dict, nodes_dict):
        """
        从给定的实体列表中解析混沌实验的目标（尾/根因）实体
        :param inject_action_dict: 记录混沌实验信息的dict
        :param nodes_dict: 运维实体dict，key为实体类别，value为实体信息列表
        :return list（可能有多个根因实体，典型为kill多个pod），每个元素为实体信息的dict
        """
        result_list = []
        resource_type = ''
        if 'http://k8s/pod/' in inject_action_dict['position']:
            resource_type = 'pod'
        elif 'http://k8s/node/' in inject_action_dict['position']:
            resource_type = 'node'

        for resource_item in nodes_dict[resource_type]:
            if resource_item['name'] in inject_action_dict['position']:
                result_list.append(resource_item.copy())
        return result_list

    @staticmethod
    def calculate_path_weight(path_list):
        total_weight = 0
        for link in path_list:
            total_weight += link['weight']
        return total_weight

    @staticmethod
    def calculate_path_factor(before_path_list, after_path_list, gamma=0.1):
        """
        从提取到的路径中，找到最有可能的影响路径
        :param before_path_list: 实验前某条的路径信息
        :param after_path_list: 实验后与before_path_list对应的路径信息
        :param gamma: 超参数，防止轻微变化带来的剧烈抖动，默认为1
        :return dict，包含node_list和link_list，组合起来为影响路径
        """
        total_difference = 0
        for index in range(len(before_path_list)):
            total_difference += (before_path_list[index]['weight'] - after_path_list[index]['weight']) ** 2
        path_weight = PathExtraction.calculate_path_weight(before_path_list)
        return (gamma + total_difference / len(before_path_list)) * path_weight / len(before_path_list) ** 2

    @staticmethod
    def path_id_to_name(node_list, path_id_list):
        """
        从给定的实体列表中解析指标所在的受影响的（头）实体
        :param node_list: 实体信息列表
        :param path_id_list: 实体id组成的路径列表
        :return list，实体名列表
        """
        result_list = []
        for path_id in path_id_list:
            for node in node_list:
                if path_id == node['id']:
                    result_list.append(node['name'])
                    break
        return result_list
