from tslearn.utils import to_time_series_dataset
from tslearn.clustering import TimeSeriesKMeans
from tslearn.clustering import silhouette_score
from tslearn.clustering import GlobalAlignmentKernelKMeans
from tslearn.clustering import KShape

import os
import json
from metrics_reader import MetricsReader
from metrics_anomaly_detection import average_anomaly_detection
from utils.str_handler import StrHandler


class MetricsCluster:
    def __init__(self):
        self.metrics_reader = MetricsReader()
        self.str_handler = StrHandler()

        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        temp_path = os.path.join(os.path.join(temp_path, 'result'), 'metrics_cluster')
        if not os.path.exists(temp_path):
            os.mkdir(temp_path)
        self.cluster_result_base_path = temp_path

        temp_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        temp_path = os.path.join(os.path.join(temp_path, 'model'), 'metrics_cluster')
        if not os.path.exists(temp_path):
            os.mkdir(temp_path)
        self.cluster_model_base_path = temp_path

        self.default_min_cluster_num = 3
        self.default_max_cluster_num = 8

    def cluster_data_loader(self, least_valid_num=20):
        """
        加载并预处理聚类数据，以便构建下一步的聚类模型
        :param least_valid_num: 聚类最少拥有的异常数据条数，筛选后低于此条数的数据不认为具有参考价值，不纳入分析
        :return dict，key为有效metrics名，value为聚类所用的数据
        """
        result_dict = dict()

        chaos_metrics_dict = self.metrics_reader.chaos_metrics_reader()
        for inject_time, inject_time_metrics_dict in chaos_metrics_dict['metrics_data'].items():
            for metric_name, data_list in inject_time_metrics_dict.items():
                if metric_name == 'timestamp':
                    continue
                elif average_anomaly_detection(data_list, 60, 120):
                    if metric_name not in result_dict.keys():
                        result_dict[metric_name] = [data_list]
                    else:
                        result_dict[metric_name].append(data_list)

        invalid_key_list = []
        for metric_name, metrics_data_list in result_dict.items():
            if len(metrics_data_list) < least_valid_num:
                invalid_key_list.append(metric_name)
        for invalid_key in invalid_key_list:
            result_dict.pop(invalid_key)
        return result_dict

    def cluster(self, metric_name, metrics_data_list, model, distance_type, cluster_num):
        """
        加载并预处理聚类数据，以便构建下一步的聚类模型
        :param metric_name: 聚类的指标名称
        :param metrics_data_list: 聚类数据
        :param model: 模型名称，目前可选项为：TimeSeriesKMeans、GlobalAlignmentKernelKMeans、KShape
        :param distance_type: 使用何种距离度量
        :param cluster_num: 聚类类别数
        :return dict，key为有效metrics名，value为聚类所用的数据
        """
        result_dict = dict()

        cluster_num = int(cluster_num)

        x_train = to_time_series_dataset(metrics_data_list)
        km = TimeSeriesKMeans(n_clusters=cluster_num, metric=distance_type, verbose=True)
        if model == 'TimeSeriesKMeans':
            pass
        elif model == 'GlobalAlignmentKernelKMeans':
            km = GlobalAlignmentKernelKMeans(n_clusters=cluster_num, verbose=True)
        elif model == 'KShape':
            km = KShape(n_clusters=cluster_num, verbose=True)
        else:
            result_dict["message"] = "Incorrect model type"
            return result_dict

        label = km.fit_predict(x_train)
        model_path = os.path.join(self.cluster_model_base_path, self.str_handler.change_to_file_name(metric_name))
        if not os.path.exists(model_path):
            os.mkdir(model_path)
        model_file_name = model + '---' + distance_type + '---' + str(cluster_num) + '.h5py'
        model_file_path = os.path.join(model_path, model_file_name)
        if os.path.exists(model_file_path):
            os.remove(model_file_path)
        km.to_hdf5(model_file_path)

        sc = silhouette_score(x_train, label, metric=distance_type)
        sse = km.inertia_
        result_dict['label'] = str(label)
        result_dict['sc'] = sc
        result_dict['sse'] = sse

        judge_path = os.path.join(self.cluster_result_base_path, 'judge')
        if not os.path.exists(judge_path):
            os.mkdir(judge_path)
        with open(os.path.join(judge_path, self.str_handler.change_to_file_name(metric_name) + model + '---' +
                               distance_type + '---' + str(cluster_num) + '---label.json'), 'w') as f:
            json.dump(result_dict, f, indent=4)

    def anomaly_metrics_cluster(self):
        """
        对加载的异常metrics数据分别进行聚类
        """
        metrics_data_dict = self.cluster_data_loader()
        for metrics_name, metrics_data_list in metrics_data_dict.items():
            for cluster_num in range(self.default_min_cluster_num, self.default_max_cluster_num):
                self.cluster(metrics_name, metrics_data_list, 'TimeSeriesKMeans', 'dtw', cluster_num)

    def best_cluster_num_select(self, model, distance_type):
        """
        根据轮廓系数，找到最为合适的聚类模型
        """
        best_cluster_num_dict = dict()

        metrics_data_dict = self.cluster_data_loader()
        judge_path = os.path.join(self.cluster_result_base_path, 'judge')
        for metric_name, metrics_data_list in metrics_data_dict.items():
            best_sc = 0
            best_cluster_num = 0
            for cluster_num in range(self.default_min_cluster_num, self.default_max_cluster_num):
                with open(os.path.join(judge_path, self.str_handler.change_to_file_name(metric_name) + model + '---' +
                                       distance_type + '---' + str(cluster_num) + '---label.json'), 'r') as f:
                    sc = json.load(f)['sc']
                    if sc > best_sc:
                        best_sc = sc
                        best_cluster_num = cluster_num
            best_cluster_num_dict[metric_name] = best_cluster_num

        best_cluster_num_result_path = os.path.join(self.cluster_result_base_path, 'best_num')
        if not os.path.exists(best_cluster_num_result_path):
            os.mkdir(best_cluster_num_result_path)
        with open(os.path.join(best_cluster_num_result_path, model + '---' + distance_type + '.json'), 'w') as f:
            json.dump(best_cluster_num_dict, f, indent=4)

    def predict(self, metric_name, metrics_data_list, model, distance_type, cluster_num):
        """
        加载训练好的聚类模型，并给出输入的metrics数据类别
        :param metric_name: 聚类的指标名称
        :param metrics_data_list: 聚类数据
        :param model: 模型名称，目前可选项为：TimeSeriesKMeans、GlobalAlignmentKernelKMeans、KShape
        :param distance_type: 使用何种距离度量
        :param cluster_num: 聚类类别数
        :return dict，key为有效metrics名，value为聚类所用的数据
        """
        result_dict = dict()

        model_path = os.path.join(self.cluster_model_base_path, self.str_handler.change_to_file_name(metric_name))
        if os.path.exists(model_path):
            model_file_name = model + '---' + distance_type + '---' + str(cluster_num) + '.h5py'
            model_file_path = os.path.join(model_path, model_file_name)
            km = TimeSeriesKMeans.from_hdf5(model_file_path)
            if model == 'TimeSeriesKMeans':
                pass
            elif model == 'GlobalAlignmentKernelKMeans':
                km = GlobalAlignmentKernelKMeans.from_hdf5(model_file_path)
            elif model == 'KShape':
                km = KShape.from_hdf5(model_file_path)
            else:
                result_dict["message"] = "Incorrect model type"
                return result_dict
        else:
            result_dict["message"] = "Please cluster first"
            return result_dict
        result_dict["result"] = km.predict(to_time_series_dataset(metrics_data_list))[0]
        return result_dict

    def extract_metrics_feature(self, model='TimeSeriesKMeans', distance_type='dtw'):
        """
        根据聚类模型，提取每次实验的metrics特征并转换为语义表达
        :param model: 使用的模型名称
        :param distance_type: 使用的度量距离
        """
        result_dict = dict()

        best_cluster_num_dict = dict()
        best_cluster_num_result_path = os.path.join(self.cluster_result_base_path, 'best_num')
        if not os.path.exists(best_cluster_num_result_path):
            print("Please generate model and analyze best of them")
        with open(os.path.join(best_cluster_num_result_path, model + '---' + distance_type + '.json'), 'r') as f:
            best_cluster_num_dict = json.load(f)

        chaos_metrics_dict = self.metrics_reader.chaos_metrics_reader()
        for inject_time, inject_time_metrics_dict in chaos_metrics_dict['metrics_data'].items():
            if inject_time not in result_dict:
                result_dict[inject_time] = []
            for metric_name, cluster_num in best_cluster_num_dict.items():
                data_list = inject_time_metrics_dict[metric_name]
                feature_str = ''
                if average_anomaly_detection(data_list, 60, 120):
                    feature_str = 'anomaly-' + str(self.predict(metric_name, data_list, model, distance_type, cluster_num)['result'])
                else:
                    feature_str = 'normal'
                result_dict[inject_time].append(metric_name + '::' + feature_str)

        result_path = os.path.join(self.cluster_result_base_path, 'metrics_feature')
        if not os.path.exists(result_path):
            os.mkdir(result_path)
        with open(os.path.join(result_path, model + '---' + distance_type + '.json'), 'w') as f:
            json.dump(result_dict, f, indent=4)


if __name__ == '__main__':
    m = MetricsCluster()
    m.extract_metrics_feature()
