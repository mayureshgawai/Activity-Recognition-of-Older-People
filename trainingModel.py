from data_validation import Data_Validation
from application_logging import logger
import yaml
import json

class TrainValModel:
    def __init__(self, path):
        self.valid_data = Data_Validation(path)
        self.logger = logger.Logger()
        self.file_object = "logs/training_val_logs/TrainingAndValidationLogs.txt"
        self.parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)





    def train_val_model(self):
        '''This method is responsible to run the pipeline from Validation of
            training datatset to the training phase'''

        try:
            self.logger.log(self.file_object, "Entered into Training Validation method::train_val_model()")
            print("Entered into training method")
            # Getting schema details
            LengthOfDateStampInFile, LengthOfTimeStampInFile, NumberOfColumns, prefix, colNames = self.valid_data.validation_attributes()
            # Get the regex
            regex = self.valid_data.fileVal_regex()
            # Check the file name for datestamp, timestamp and prefix
            self.valid_data.check_file_name(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile, prefix)
            # Check column numbers, name and datatype
            self.valid_data.column_length_validation()


        except Exception as e:
            print("Unknown Error occured ", e)