from application_logging.logger import Logger
import yaml
import json
import shutil
from Exceptions.Errors import EmptyDirectoryError
import re
import os
import pandas as pd

class Prediction_Data_Validation:
    def __init__(self):
        self.file_object = 'logs/PredictionModel/Prediction_Model.txt'
        self.logger = Logger()
        self.parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)

    def validation_attributes(self):
        self.logger.log(self.file_object, "Attributes validation Started")

        '''
            Method Name: validate_attributes()
           Description:  method will check schema and validate if the data recieved is appropriate for prediction or not
           return: LengthOfTimeStampInFile, LengthOfDateStampInFile, NumberOfColumns, ColName
            '''

        try:
            self.logger.log(self.file_object, "::validation_attributes()")
            attr = json.load(open(self.parsed_yaml['path']['schema_prediction']))
            # attr = self.parsed_yaml['path']['schema_training']
            LengthOfDateStampInFile = int(attr['LengthOfDateStampInFile'])
            LengthOfTimeStampInFile = int(attr['LengthOfTimeStampInFile'])
            NumberOfColumns = int(attr['NumberOfColumns'])
            pattern = attr['SampleFileName']
            colNames = attr['ColName']
            prefix = attr['prefix']

            self.logger.log(self.file_object, "LengthOfDateStampInFile:" + str(
                LengthOfDateStampInFile) + ", LengthOfTimeStampInFile:" + str(
                LengthOfTimeStampInFile) + ", NumberOfColumns:" + str(
                NumberOfColumns) + ", pattern:" + pattern + " Columns:")
            for names in colNames:
                self.logger.log(self.file_object, names)

            return LengthOfDateStampInFile, LengthOfTimeStampInFile, NumberOfColumns, prefix, colNames
        except Exception as e:
            raise e

    def fileVal_regex(self):
        '''
            Method Name: fileVal_regex
            Descripion: return regular expression to validate the file names and contents
        :return:
        '''
        regex = "['activity']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def deleteTrainingData(self, filelist, path):
        '''
            Method Name: deleteTrainingData()
            Description: Deleting all the previously available file to work on new data
            params: filelist, path:
            '''
        try:
            if len(filelist)==0:
                self.logger.log(self.file_object, "deleteTrainingData()::No files to be deleted")
                return
            for files in filelist:
                os.remove(path+"/"+files)
            self.logger.log(self.file_object, "Deleted previous training files")
        except Exception as e:
            self.logger.log(self.file_object, str(e))

    def check_file_name(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile, prefix):

        self.logger.log(self.file_object, "File name validation according to DSA conventions is Started")

        '''
            Method: check_file_name()
            Description: This method is responsible for validation of filename according to schema standards and moving training file
                         into appropriate folder i.e valid and invalid training data
            params: regex, LengthOfDateStampInFile, LengthOfTimeStampInFile, prefix
            return:
            '''

        try:
            predPath = self.parsed_yaml['path']['prediction_files']
            validData = self.parsed_yaml['path']['pred_validData']
            invalidData = self.parsed_yaml['path']['pred_invalidData']
            files = [f for f in os.listdir(predPath)]

            self.deleteTrainingData([vf for vf in os.listdir(validData)], validData)
            self.deleteTrainingData([ivf for ivf in os.listdir(validData)], invalidData)

            if len(files) == 0:
                raise EmptyDirectoryError()

            for filename in files:
                if(re.match(regex, filename)):
                    dotsplit = re.split('.csv', filename)
                    usrsplit = re.split('_', dotsplit[0])
                    if(usrsplit[0] == prefix):
                        len1 = len(usrsplit[1])
                        len2 = len(usrsplit[2])
                        if((len(usrsplit[1]) == LengthOfDateStampInFile) and (len(usrsplit[2]) == LengthOfTimeStampInFile)):
                            shutil.copy(str(predPath) +"/"+ filename, 'predictionDataset/validPredData')
                            self.logger.log(self.file_object, filename+" is valid and moved into validPredData")
                        else:
                            shutil.copy(str(predPath) + filename, 'predictionDataset/invalidPredData')
                            self.logger.log(self.file_object, filename + " is invalid and moved into invalidPredData")
                    else:
                        shutil.copy(str(predPath) + filename, 'predictionDataset/invalidPredData')
                        self.logger.log(self.file_object, filename + " is invalid and moved into invalidPredData")

        except EmptyDirectoryError as e:
            self.logger.log(self.file_object, "Exception: check_file_name():"+str(e))
        except Exception as e:
            print(e)


    def column_length_validation(self):

        self.logger.log(self.file_object,"Columns length validation Started")

        '''
            Method Name: column_length_validation()
            Description: Check the column length in files and also name and datatype
            param:
            return:
        '''


        try:
            predPath = self.parsed_yaml['path']['prediction_files']
            validData = self.parsed_yaml['path']['pred_validData']
            invalidData = self.parsed_yaml['path']['pred_invalidData']
            schemaPath = self.parsed_yaml['path']['schema_prediction']
            files = [f for f in os.listdir(predPath)]

            data = json.load(open(str(schemaPath)))

            if len(files) == 0:
                raise EmptyDirectoryError()

            for filename in files:
                colLength = data['NumberOfColumns']
                df = pd.read_csv(validData+"/"+filename)
                # df.drop(columns=['Unnamed: 8'], inplace=True)

                # To drop all unnecessary columns
                for cols in df.columns:
                    if(cols.find('Unnamed') != -1):
                        df.drop(columns=[cols], inplace=True)

                s = df.shape
                if(df.shape[1] == colLength):
                    columns = df.dtypes
                    sc_columns = data['ColName']

                    # checking for column name sequence and datatype
                    counter = 1
                    for col in columns.index:
                        sc_columns[col]
                        if(str(df[col].dtypes) == sc_columns[col]):
                            # If counter reaches to shape[1], that means we ahve gone through all the columns and now it should be moved to valid folder
                            if(counter == df.shape[1]):
                                return

                            else:
                                counter += 1
                                continue
                        else:
                            shutil.copy(str(validData) + filename, 'predictionDataset/invalidPredData')
                            self.logger.log(self.file_object,filename + " is invalid for column and moved into invalidPredData")
                            break
                else:
                    shutil.copy(str(validData) + filename, 'predictionDataset/invalidPredData')
                    self.logger.log(self.file_object, filename + " is invalid for column length and moved into invalidPredData")

        except KeyError as e:
            print(e)
        except EmptyDirectoryError as e:
            self.logger.log(self.file_object, str(e))
        except Exception as e:
            self.logger.log(self.file_object, str(e.args))

    def validate_missing_values_in_whole_columnn(self):

        '''
            Method Name: validate_missing_column_length()
            Description: Checks for columns if they are not filled with all Null values in it. If there are columnns
                         then move it to the invalid one.
            param:
            return:
        '''

        try:
            predPath = self.parsed_yaml['path']['prediction_files']
            validData = self.parsed_yaml['path']['pred_validData']
            invalidData = self.parsed_yaml['path']['pred_invalidData']
            schemaPath = self.parsed_yaml['path']['schema_prediction']
            files = [f for f in os.listdir(validData)]

            data = json.load(open(str(schemaPath)))

            if len(files) == 0:
                raise EmptyDirectoryError()

            for filename in files:

                df = pd.read_csv(validData + "/" + filename)

                # To drop all unnecessary columns
                for cols in df.columns:
                    if (cols.find('Unnamed') != -1):
                        df.drop(columns=[cols], inplace=True)

                cols = df.columns
                for col in cols:
                    if ((len(df[col]) - df[col].count()) == len(df[col])):
                        shutil.copy(str(validData) + filename, 'predictionDataset/invalidPredData')
                        self.logger.log(self.file_object,
                                        filename + " is invalid for column and moved into invalidPredData")
                        break

        except EmptyDirectoryError as e:
            self.logger.log(self.file_object, str(e))
        except Exception as e:
            self.logger.log(self.file_object, str(e))
        except KeyError as e:
            self.logger.log(self.file_object, str(e))