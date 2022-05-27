import os

import pandas as pd

from data_validation import Data_Validation
from application_logging import logger
import yaml
import json
from data_processing.processor import Processor
from dbOperation.dbOperation import DBOperation
from data_processing.clustering import Clustering
from sklearn.model_selection import train_test_split
from Tuner.tuner import Model_Finder
from file_operations.file_opr import File_Operations

class TrainValModel:
    def __init__(self):
        self.valid_data = Data_Validation()
        self.logger = logger.Logger()
        self.file_object = "logs/training_val_logs/TrainingAndValidationLogs.txt"
        self.parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)
        self.processor = Processor()
        self.dbOperation = DBOperation()
        self.clustering = Clustering()
        self.fileOpr = File_Operations()


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
            self.valid_data.validate_missing_values_in_whole_columnn()
            self.logger.log(self.file_object,"train_val_model():validate_missing_values_in_columnn()::Missing whole column values validation performed!")
            self.logger.log(self.file_object, "Data Validation is done")

            # Checking if any null values are there in columns. In case, then files will be moved to the NullRemovalFiles directory
            isnull = self.processor.isNullPresent()
            if(isnull):
                self.processor.impute_missing_values()

            self.logger.log(self.file_object, "train_val_data()::Data Processing Ends here.")

            self.logger.log(self.file_object, "train_val_data()::DB operation starts.")

            dbname = self.parsed_yaml['dbconnect_train']['database_name']
            tblname = self.parsed_yaml['dbconnect_train']['table_name']
            trainPath = self.parsed_yaml['path']['validData']

            # Create database
            self.dbOperation.create_database(dbname)
            # Create table
            self.dbOperation.create_table(dbname, tblname, 'training/output')
            # It will take files from valid data directory and insert into db.
            self.dbOperation.insertIntoDB(dbname, tblname, 'training/output')
            # It will select values from table and create final input file
            self.dbOperation.select_and_create_input(tblname, 'training/output')
            self.logger.log(self.file_object, "train_val_data()::DB operations Completed.")

            input = self.parsed_yaml['path']['inputs']

            # self.clustering.elbow_plot()
            cluster_data = self.clustering.clusterDataset()
            file = os.listdir(input)
            data1 = pd.read_csv(input+"/"+file[0])
            data = data1.drop(columns=['Unnamed: 0'])
            data['cluster'] = cluster_data

            list_of_cluster = data['cluster'].unique()

            for num  in list_of_cluster:
                seperate_data = data[data['cluster'] == num]
                seperate_features = seperate_data.drop(columns=['Label', 'cluster'])
                label = seperate_data['Label']

                # split the dataset into training and testing
                X_train, X_test, y_train, y_test = train_test_split(seperate_features, label, test_size=0.25, random_state=30)

                model_finder = Model_Finder(self.file_object, self.logger)

                best_model_name, best_model = model_finder.get_best_model(X_train, X_test, y_train, y_test)
                self.fileOpr.save_model(best_model_name+str(num), best_model)


        except Exception as e:
            print("Unknown Error occured ",+ str(e))