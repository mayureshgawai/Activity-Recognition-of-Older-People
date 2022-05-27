from application_logging.logger import Logger
import yaml
from Exceptions.Errors import EmptyDirectoryError
import os
import shutil
import pandas as pd
from sklearn.impute import KNNImputer
import numpy as np

class Processor:
    def __init__(self):
        self.file_object = 'logs/PredictionModel/Prediction_Model.txt'
        self.logger = Logger()
        self.parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)

    def isNullPresent(self):

        '''
            Method Name: isNullPresent()
            Description: Checks if any null values are there in dataset. Move to the NullRemovalFiles Directory if there is any null values in it.
            param:
            return:
        '''

        try:

            self.logger.log(self.file_object, "isNullPresent()::Checking if null values are present")
            nullData = self.parsed_yaml['path']['pred_nullValuesData']

            predPath = self.parsed_yaml['path']['prediction_files']
            validData = self.parsed_yaml['path']['pred_validData']
            invalidData = self.parsed_yaml['path']['pred_invalidData']
            files = [f for f in os.listdir(validData)]

            self.deleteNullDataset([nf for nf in os.listdir(nullData)], nullData)

            for file in files:
                df = pd.read_csv(predPath+"/"+file)

                # To drop all unnecessary columns
                for cols in df.columns:
                    if (cols.find('Unnamed') != -1):
                        df.drop(columns=[cols], inplace=True)

                null_count = df.isnull().sum()

                for index in null_count.index:
                    if(null_count[index] > 0):
                        shutil.move("./"+str(validData)+"/"+file, './predictionDataset/invalidPredData')
                        self.logger.log(self.file_object, "isNullPresent()::File "+"'"+file+"'"+" having null values in it. Moved to NullValuesData Directory")
                        return True
            return False
        except Exception as e:
            self.logger.log(self.file_object, str(e))
        except EmptyDirectoryError as e:
            self.logger.log(self.file_object, str(e))


    def deleteNullDataset(self, filelist, path):

        '''
                    Method Name: deleteNullDataset()
                    Description: Deleting all the previously available file to work on new data
                    params: filelist, path:
                    return:
                    '''
        try:
            if len(filelist) == 0:
                self.logger.log(self.file_object, "deleteNullDataset()::No files to be deleted")
                return

            for files in filelist:
                os.remove(path + "/" + files)
            self.logger.log(self.file_object, "Deleted previous null datasets")
        except Exception as e:
            self.logger.log(self.file_object, str(e))

    def impute_missing_values(self):
        '''
            Method Name: impute_missing_values()
            Description: Applying knn imputer for missing values.
            param:
            return:
        '''

        self.logger.log(self.file_object, "impute_missing_values():: Entered into imputing method")

        try:
            nanDatasets = self.parsed_yaml['path']['pred_nullValuesData']
            predPath = self.parsed_yaml['path']['prediction_files']
            files = [f for f in os.listdir(nanDatasets)]

            if len(files) == 0:
                return

            # Using KNN imputer
            imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)

            for file in files:
                df = pd.read_csv(nanDatasets+"/"+file)
                imputed_array = imputer.fit_transform(df)
                final_df = pd.DataFrame(df)
                final_df.to_csv('./predictionDataset/validPredData'+str(file))

        except Exception as e:
            self.logger.log(self.file_object, str(e))


