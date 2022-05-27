import os

from application_logging.logger import Logger
import yaml
class PredData:
    def __init__(self):
        self.file_object = 'logs/PredictionModel/Prediction_Model.txt'
        self.logger = Logger()
        self.parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)

    def get_data(self):
        pred_data =  self.parsed_yaml['path']['predData']
        files = [f for f in os.listdir(pred_data)]

        for file in files:
            pass