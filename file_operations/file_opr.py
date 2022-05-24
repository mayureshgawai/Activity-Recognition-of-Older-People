import shutil
import pickle
from application_logging.logger import Logger
import os

class File_Operations:
    def __init__(self):
        self.file_object = './logs/File_Operations/file_operations.txt'
        self.logger = Logger()
        self.model_directory = './models'


    def save_model(self, model_name, model):

        # self.logger.log()
        try:
            path = os.path.join(self.model_directory, model_name)
            # if(len(os.listdir(path)) != 0):
            if(os.path.isdir(path)):
                shutil.rmtree(path)
                os.makedirs(path)
            else:
                os.makedirs(path)

            with open(path+"/"+model_name+".sav", "wb") as f:
                pickle.dump(model, f)
            self.logger.log(self.file_object, "Model saved successfully...")
            return True

        except Exception as e:
            return False

