from data_validation import Data_Validation
from application_logging import logger
import yaml
import json
from data_processing.processor import Processor

class TrainValModel:
    def __init__(self, path):
        self.valid_data = Data_Validation(path)
        self.logger = logger.Logger()
        self.file_object = "logs/training_val_logs/TrainingAndValidationLogs.txt"
        self.parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)
        self.processor = Processor()




    def train_val_data(self):
        '''This method is responsible to run the pipeline from Validation of
            training datatset to the training phase'''

        try:
            self.logger.log(self.file_object, "train_val_model()::Entered into Validation method")
            # print("Entered into training method")
            # Getting schema details
            LengthOfDateStampInFile, LengthOfTimeStampInFile, NumberOfColumns, prefix, colNames = self.valid_data.validation_attributes()
            # Get the regex
            regex = self.valid_data.fileVal_regex()
            # Check the file name for datestamp, timestamp and prefix
            self.valid_data.check_file_name(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile, prefix)
            # Check column numbers, name and datatype
            self.valid_data.column_length_validation()
            self.logger.log(self.file_object, "train_val_model():column_length_validation()::Validation of Column length, name and dtype is done!")
            # If there are columns paresent in dataset with no values, remove it
            self.valid_data.validate_missing_values_in_columnn()
            self.logger.log(self.file_object,"train_val_model():validate_missing_values_in_columnn()::Missing whole column values validation performed!")
            self.logger.log(self.file_object, "Data Validation is done")

            # Checking if any null values are there in columns. In case, then files will be moved to the NullRemovalFiles directory
            self.processor.isNullPresent()
            self.processor.impute_missing_values()






        except Exception as e:
            print("Unknown Error occured ", e)