from wsgiref import simple_server

from flask import Flask, request,render_template
from flask import Response
import json
import os
from flask_cors import CORS, cross_origin
import yaml
from application_logging import logger
from trainingModel import TrainValModel
from predictionModel import PredictModel
import pandas as pd

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
@cross_origin()
def index():
    return render_template('index.html')


@app.route('/predict', methods=['GET', 'POST'])
@cross_origin()
def predict_model():
    try:
        print("Entered into prediction model")
        predmodel = PredictModel()
        df = predmodel.prediction_model()

        return render_template("index.html", column_names=df.columns.values, row_data=list(df.values))
    except Exception as e:
        print(e)

@app.route('/train', methods=['GET', 'POST'])
@cross_origin()
def train_model():
    try:
        yaml_file = open('config.yml')
        parsed_yaml = yaml.load(yaml_file, Loader=yaml.FullLoader)
        path = parsed_yaml["path"]['trainingFiles']

        trainValModel = TrainValModel()
        trainValModel.train_val_data()
        # file_obj.close()
        return True

    except Exception as e:
        return "Unknown Error occcured. "+ e



port = int(os.getenv("PORT", 5000))
if __name__ == '__main__':

    parsed_yaml = yaml.load(open('config.yml'), Loader=yaml.FullLoader)
    app.run(port=parsed_yaml['port'], debug=True)