import os

from application_logging.logger import Logger
from dbOperation.dbOperation import DBOperation
import yaml
from prediction_data_validation import Prediction_Data_Validation
from data_processing.predprocessor import Processor
from data_processing.clustering import Clustering
from PredictionMethod.Prediction import Prediction

class PredictModel:
    def __init__(self):
        self.file_object = 'logs/PredictionModel/Prediction_Model.txt'
        self.logger = Logger()
        self.dbop = DBOperation()
        self.parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)
        self.pred_data_val = Prediction_Data_Validation()
        self.processor = Processor()
        self.clustering = Clustering()
        self.prediction = Prediction()

    def prediction_model(self):
        try:
            self.logger.log(self.file_object, "prediction_model:: Start of Prediction")
            self.logger.log(self.file_object, "prediction_model:: Get data from db")

            # Getting schema details
            LengthOfDateStampInFile, LengthOfTimeStampInFile, NumberOfColumns, prefix, colNames = self.pred_data_val.validation_attributes()
            # Get the regex
            regex = self.pred_data_val.fileVal_regex()
            self.pred_data_val.check_file_name(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile, prefix)
            self.pred_data_val.column_length_validation()
            self.pred_data_val.validate_missing_values_in_whole_columnn()

            present = self.processor.isNullPresent()
            if(present):
                self.processor.impute_missing_values()

            self.logger.log(self.file_object, "prediction_model:: Let's first try to create DB in case it doesn't exists")
            # I am trying to make seperate database for prediction and training
            dbname = self.parsed_yaml['dbconnect_train']['pred_database']
            tblname = self.parsed_yaml['dbconnect_train']['pred_table_name']

            self.dbop.create_table(dbname, tblname, 'prediction')
            self.dbop.insertIntoDB(dbname, tblname, 'prediction')
            self.dbop.select_and_create_input(tblname, 'prediction')

            inputData = self.parsed_yaml['path']['pred_input']
            data = self.clustering.cluster_prediction(inputData)

            list_of_cluster = data['clusters'].unique()
            for cluster in list_of_cluster:
                df = data[data['clusters'] == cluster]
                maindf = df.drop(['clusters'], axis=1)

                outcome = self.prediction.prediction(maindf, cluster)
                outcome.to_csv(self.parsed_yaml['path']['finalPrediction']+"/"+"Output.csv")

        except Exception as e:
            self.logger.log(self.file_object, f"prediction_model::Error Occured, {str(e)}")









