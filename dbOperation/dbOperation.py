import csv
import os

from Exceptions.Errors import EmptyDirectoryError
from application_logging.logger import Logger
import yaml
import sqlite3
import mysql.connector as connection
import pandas as pd
import json

class DBOperation:
    def __init__(self):
        self.parsed_yaml = yaml.load(open('./config.yml'), Loader=yaml.FullLoader)
        self.trainPath = self.parsed_yaml['path']['validData']
        self.logger = Logger()
        self.file_object = './logs/dbOperation/DBOperation.txt'
        self.host = self.parsed_yaml['dbconnect']['host']
        self.user = self.parsed_yaml['dbconnect']['user']
        self.passwd = self.parsed_yaml['dbconnect']['password']




    def establish_connection(self, host_name, user, password):
        try:
            mydb = connection.connect(host=host_name, user=user, passwd=password, use_pure=True)
            return mydb
        except Exception as e:
            self.logger.log(self.file_object,"create_connection()::Error occured during establishing connection, " + str(e))
            return 0


    def select_and_create_input(self, table_name):

        try:
            inputPath = self.parsed_yaml['path']['inputs']
            dbname = self.parsed_yaml['dbconnect']['database_name']
            mydb = self.establish_connection(self.host, self.user, self.passwd)
            query = 'select * from '+dbname+"."+str(table_name)
            # cursor = mydb.cursor()
            # cursor.execute(query)
            objects = pd.read_sql(query, mydb)
            objects.to_csv(str(inputPath)+"/"+"InputFile.csv")

        except Exception as e:
            mydb.close()
            self.logger.log(self.file_object, "select_and_create_input()::Error occured in execution, "+str(e))


    def create_database(self, name):

        try:
            dbname = self.parsed_yaml['dbconnect']['database_name']

            query = 'create database IF NOT EXISTS '+str(dbname)

            mydb = self.establish_connection(self.host, self.user, self.passwd)
            cursor = mydb.cursor()
            cursor.execute(query)
        except Exception as e:
            self.logger.log(self.file_object, "create_database()::Error occurred in creating database. "+str(e))

    def create_table(self, dbname, tableName):
        try:
            schema = self.parsed_yaml['path']['schema_training']
            # columns = json.load(open('./'+str(schema)))
            mydb = self.establish_connection(self.host, self.user, self.passwd)
            query = 'create table IF NOT EXISTS '+dbname+"."+tableName+'(`Time in seconds` float4, `Acceleration reading in G for frontal axis` float4, ' \
                                                            '`Acceleration reading in G for vertical axis` float4,' \
                                                            '`Acceleration reading in G for lateral axis` float4,' \
                                                            '`Id of antenna reading sensor` float4,' \
                                                            '`Received signal strength indicator (RSSI)` float4, ' \
                                                            '`Phase` float, `Frequency` float4, `Label` int)'
            cursor = mydb.cursor()
            cursor.execute(query)

        except Exception as e:
            self.logger.log(self.file_object, "create_database()::Error occurred in creating table. "+str(e))


    def insertIntoDB(self):

        try:
            trainPath = self.parsed_yaml['path']['validData']
            files = [f for f in os.listdir(trainPath)]
            tblname = self.parsed_yaml['dbconnect']['table_name']
            dbname = self.parsed_yaml['dbconnect']['database_name']

            if len(files) == 0:
                raise EmptyDirectoryError()
            mydb = self.establish_connection(self.host, self.user, self.passwd)
            for file in files:
                with open("./"+str(trainPath)+"/"+file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        try:
                            query = 'insert into '+dbname+"."+tblname+' values({values})'.format(values=line[1][0])
                            cursor = mydb.cursor()
                            cursor.execute(query)
                            mydb.commit()
                        except Exception as e:
                            self.logger.log(self.file_object,
                                            "insertIntoDB()::Error occurred in inserting into table traversing. " + str(e))
        except Exception as e:
            self.logger.log(self.file_object, "insertIntoDB()::Error occurred in inserting into table. " + str(e))

