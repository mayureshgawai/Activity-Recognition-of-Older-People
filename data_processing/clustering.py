import os

from application_logging import logger
from sklearn.cluster import KMeans, DBSCAN
import yaml

class Clustering:
    def __init__(self):
        self.file_object = './logs/clustering/ClusteringProcess.txt'
        self.logger = logger.Logger()
        self.parsed_yaml = yaml.load(open('./config.yml'), Loader=yaml.FullLoader)

    def clusterDataset(self):
        trainPath = self.parsed_yaml['path']['validData']
        files = [f for f in os.listdir(trainPath)]


