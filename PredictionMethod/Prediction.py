import os
import pickle

from application_logging.logger import Logger
import yaml

class Prediction:

    '''
        class_name: Prediction
        Description: Consist of method to make final predicions using the input data . Customized ML approach is followed.
        Constructor param:

        '''

    def __init__(self):
        self.file_object = 'logs/PredictionModel/Prediction_Model.txt'
        self.logger = Logger()
        self.parsed_yaml = yaml.load(open('./config.yml'), Loader=yaml.FullLoader)
        self.model_directory = "./models"

    def prediction(self, data, cluster):

        '''
            Method name: prediction
            Description: To make final predictionsa
            :param data:
            :param cluster:
            :return: Dataframe
        '''

        try:
            files = [f for f in os.listdir(self.model_directory)]

            for file in files:
                if(file.find(str(cluster)) != -1):
                    with open(self.model_directory+"/"+file+"/"+file+".sav", 'rb') as f:
                        model = pickle.load(f)
                    break
            y_pred = model.predict(data)
            data['Label'] = y_pred
            return data

        except Exception as e:
            self.logger.log(self.file_object, "prediction::"+str(e))

