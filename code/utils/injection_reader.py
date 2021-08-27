import os


class InjectionReader:
    def __init__(self):
        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        temp_path = os.path.join(os.path.join(temp_path, 'dataset'), 'sock-shop')
        self.inject_log_file_path = os.path.join(temp_path, 'inject_action.log')

    def get_injection_data(self):
        """
        从混沌实验的记录日志中提取时间和实验细节信息
        :return: dict，key值为时间，value为实验的详细信息
        """
        result_dict = dict()
        with open(self.inject_log_file_path, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                timestamp = line.split('\t')[0][0:13]
                action = line.split('\t')[1].strip()
                result_dict[timestamp] = self.action_parser(action)
        return result_dict

    def get_injection_time(self):
        """
        从混沌实验的记录日志中提取时间
        :return: list，元素为有效的实验时间
        """
        return list(self.get_injection_data().keys())

    @staticmethod
    def action_parser(action_str):
        """
        从混沌实验的记录字符串（日志中的每一行）中提取实验信息
        :param action_str: 参见inject_action.log中的每一行格式
        :return: dict，包含position（注入对象）、type（实验类型）、timeout（实验时长）的信息
        """
        inject_action_dict = {
            'position': '',
            'type': '',
            'timeout': ''
        }
        command_details = action_str.split(' ')
        if 'pod' and 'labels' in action_str:
            inject_action_dict['position'] = 'http://k8s/pod/' + command_details[command_details.index('--labels') + 1].strip('name\\=')
        else:
            inject_action_dict['position'] = 'http://k8s/node/' + command_details[command_details.index('--names') + 1]
        inject_action_dict['type'] = ' '.join(command_details[0:5])
        inject_action_dict['timeout'] = command_details[command_details.index('--timeout') + 1]
        return inject_action_dict
