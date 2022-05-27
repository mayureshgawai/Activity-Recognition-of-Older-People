import os
import pickle

import pandas as pd

from Exceptions.Errors import EmptyDirectoryError
from application_logging import logger
from sklearn.cluster import KMeans, DBSCAN
from kneed import KneeLocator
import matplotlib.pyplot as plt
from file_operations.file_opr import File_Operations
import csv
import yaml

class Clustering:
    def __init__(self):
        self.file_object = './logs/clustering/ClusteringProcess.txt'
        self.logger = logger.Logger()
        self.parsed_yaml = yaml.load(open('./config.yml'), Loader=yaml.FullLoader)
        self.fileOpr = File_Operations()

    def elbow_plot(self):

        inputPath = self.parsed_yaml['path']['inputs']
        # Because only one file will be here, So we are not going to loop here
        file = os.listdir(inputPath)[0]

        df = pd.read_csv(inputPath+"/"+file)
        self.logger.log(self.file_object, "elbow_plot()::Elbow plot creation started")
        df1 = df.drop(columns=['Unnamed: 0', 'Label'])
        y = df['Label']
        # df1.to_csv('Spectate.csv')

        wcss = []
        try:
            for i in range(1, 20):
                kmeans = KMeans(n_clusters=i, init='k-means++', random_state=30)
                kmeans.fit(df1)
                wcss.append(kmeans.inertia_)
            # plt.plot(range(1, 20), wcss)
            # plt.title('The Elbow Method')
            # plt.xlabel('Number of clusters')
            # plt.ylabel('WCSS')
            # plt.savefig('K-Means_Elbow.PNG')

            # finding the value of optimum cluster programmatically
            kn = KneeLocator(range(1, 20), wcss, curve='convex', direction='decreasing')
            self.logger.log(self.file_object, "elbow_plot()::Best cluster value for data is: "+str(kn.knee))

            return kn.knee, df1

        except Exception as e:
            pass


    def clusterDataset(self):
        inputPath = self.parsed_yaml['path']['inputs']
        files = [f for f in os.listdir(inputPath)]
        if len(files) == 0:
            raise EmptyDirectoryError()

        knee, df = self.elbow_plot()

        try:
            kmeans = KMeans(n_clusters=int(knee), init='k-means++', random_state=30)

            y_kmeans = kmeans.fit_predict(df)
            self.logger.log(self.file_object, "clusterDataset()::Save the Kmeans model created.")
            self.fileOpr.save_model('kmeans', kmeans)
            self.logger.log(self.file_object, "clusterDataset()::KMeans model saved successfully.")
            return y_kmeans

        except Exception as e:
            self.logger.log(self.file_object, "Error occurred in clustring, "+str(e))

    def cluster_prediction(self, validDataPath):

        file = os.listdir(validDataPath)[0]
        model = self.parsed_yaml['modelpath']['kmean']
        # model = pickle.load('./'+model+'kmeans.sav')
        with open('./'+model+"/"+'kmeans.sav', 'rb') as f:
            model = pickle.load(f)

        with open(validDataPath+'/'+file, 'r') as f:
            df = pd.read_csv(f)

            # To drop all unnecessary columns
        for cols in df.columns:
            if (cols.find('Unnamed') != -1):
                df.drop(columns=[cols], inplace=True)
        clusters = model.predict(df)

        df['clusters'] = clusters
        return df