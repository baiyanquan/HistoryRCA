class StrHandler:
    def __init__(self):
        self.rule_dict = {
            '/': '--1--',
            ':': '--2--'
        }

    def change_to_file_name(self, old_str):
        """
        将含有特殊符号的字符串转换成能够存储的文件名
        :param old_str: 聚类的指标名称
        :return str，即转换后的字符串
        """
        new_str = old_str
        for key, value in self.rule_dict.items():
            new_str = new_str.replace(key, value)
        return new_str

    def file_name_change_back(self, old_str):
        """
        将为了存储转换过的文件名转换为原字符串
        :param old_str: 转换过的文件名
        :return str，原字符串
        """
        new_str = old_str
        for key, value in self.rule_dict.items():
            new_str = new_str.replace(value, key)
        return new_str
