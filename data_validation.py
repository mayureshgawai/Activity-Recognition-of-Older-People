import os
import re
import shutil

from Exceptions.Errors import EmptyDirectoryError
from application_logging import logger
import json
import yaml
import pandas as pd

class Data_Validation:
    def __init__(self, path):
        self.path = path
        self.file_object = "logs/training_val_logs/TrainingAndValidationLogs.txt"
        self.logger = logger.Logger()
        self.parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)

    def validation_attributes(self):
        '''
            Method Name: validate_attributes()
           Description:  method will check schema and validate if the data recieved is appropriate for training or not
           return: LengthOfTimeStampInFile, LengthOfDateStampInFile, NumberOfColumns, ColName
            '''

        try:
            self.logger.log(self.file_object, "::validation_attributes()")
            attr = json.load(open(self.parsed_yaml['path']['schema_training']))
            # attr = self.parsed_yaml['path']['schema_training']
            LengthOfDateStampInFile = int(attr['LengthOfDateStampInFile'])
            LengthOfTimeStampInFile = int(attr['LengthOfTimeStampInFile'])
            NumberOfColumns = int(attr['NumberOfColumns'])
            pattern = attr['SampleFileName']
            colNames = attr['ColName']
            prefix = attr['prefix']


            self.logger.log(self.file_object, "LengthOfDateStampInFile:"+str(LengthOfDateStampInFile)+", LengthOfTimeStampInFile:"+str(LengthOfTimeStampInFile)+", NumberOfColumns:"+str(NumberOfColumns)+", pattern:"+pattern+" Columns:")
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


    def check_file_name(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile, prefix):

        '''
            Method: check_file_name()
            Description: This method is responsible for validation of filename according to schema standards and moving training file
                         into appropriate folder i.e valid and invalid training data
            params: regex, LengthOfDateStampInFile, LengthOfTimeStampInFile, prefix
            return:
            '''

        try:
            trainPath = self.parsed_yaml['path']['trainingFiles']
            validData = self.parsed_yaml['path']['validData']
            invalidData = self.parsed_yaml['path']['invalidData']


            self.deleteTrainingData([f for f in os.listdir(validData)], validData)
            self.deleteTrainingData([f for f in os.listdir(invalidData)], invalidData)

            files = [f for f in os.listdir(trainPath)]
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
                            shutil.copy(str(trainPath) +"/"+ filename, 'TrainingDataSet/validTrainingData')
                            self.logger.log(self.file_object, filename+" is valid and moved into validTrainingData")
                        else:
                            shutil.copy(str(trainPath) + filename, 'TrainingDataSet/invalidTrainingData')
                            self.logger.log(self.file_object, filename + " is invalid and moved into invalidTrainingData")
                    else:
                        shutil.copy(str(trainPath) + filename, 'TrainingDataSet/invalidTrainingData')
                        self.logger.log(self.file_object, filename + " is invalid and moved into invalidTrainingData")

        except EmptyDirectoryError as e:
            self.logger.log(self.file_object, "Exception: check_file_name():"+str(e))
        except Exception as e:
            print(e)


    def deleteTrainingData(self, filelist, path):
        '''
            Method Name: deleteTrainingData()
            Description: Deleting all the previously available file to work on new data
            params: filelist, path:
            '''
        for files in filelist:
            os.remove(path+files)

    def column_length_validation(self):

        '''
            Method Name: column_length_validation()
            Description: Check the column length in files and also name and datatype
            param:
            return:
        '''


        try:
            trainPath = self.parsed_yaml['path']['validData']
            files = [f for f in os.listdir(trainPath)]
            schemaPath = self.parsed_yaml['path']['schema_training']

            data = json.load(open(str(schemaPath)))

            if len(files) == 0:
                raise EmptyDirectoryError()

            for filename in files:
                colLength = data['NumberOfColumns']
                df = pd.read_csv(trainPath+"/"+filename)
                if(df.shape[1] == colLength):
                    columns = df.dtypes
                    sc_columns = data['ColName']

                    # checking for column name sequence and datatype
                    counter = 0
                    for col in columns.index:
                        sc_columns[col]
                        if(str(df[col].dtypes) == sc_columns[col]):
                            if(counter == df.shape[1]):
                                shutil.copy(str(trainPath) + "/" + filename, 'TrainingDataSet/validTrainingData')
                                self.logger.log(self.file_object,filename + " is valid for column length and moved into validTrainingData")
                            else:
                                continue
                                counter += 1
                        else:
                            shutil.copy(str(trainPath) + filename, 'TrainingDataSet/invalidTrainingData')
                            self.logger.log(self.file_object,filename + " is invalid for column and moved into invalidTrainingData")
                            break

                else:
                    shutil.copy(str(trainPath) + filename, 'TrainingDataSet/invalidTrainingData')
                    self.logger.log(self.file_object, filename + " is invalid for column length and moved into invalidTrainingData")

        except KeyError as e:
            print(e)
        except EmptyDirectoryError as e:
            self.logger.log(self.file_object, str(e))
        except Exception as e:
            self.logger.log(self.file_object, str(e.args))