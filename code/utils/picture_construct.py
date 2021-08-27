import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os


class PictureConstruct:
    def __init__(self):
        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        temp_path = os.path.join(os.path.join(temp_path, 'result'), 'figure')
        if not os.path.exists(temp_path):
            os.mkdir(temp_path)
        self.figure_base_path = temp_path
        self.color_list = ["red", "green", "blue", "yellow", "pink", "purple", "aliceblue", "brown"]

    def generate_metrics_picture(self, name, multiple_data_list, label_list, folder_name, filename):
        """
        绘制metrics曲线
        :param name: 绘制图像的名称
        :param multiple_data_list: 指标数据
        :param label_list: 指标标签，最终会转化为曲线的颜色，元素为int型
        :param folder_name: 保存到的特定文件夹
        :param filename: 保存到的文件名
        """
        plt.title(name)
        for index in range(0, multiple_data_list):
            plt.plot(list(range(len(multiple_data_list[index]))), multiple_data_list[index],
                     self.color_list[label_list[index]])
        plt.legend()
        plt.xlabel('time')
        plt.ylabel('value')
        folder_path = os.path.join(self.figure_base_path, folder_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        plt.savefig(os.path.join(folder_path, filename + '.png'))
