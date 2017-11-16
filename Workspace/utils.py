import boto3
import io
import botocore
from pyspark.sql import SparkSession
import os
import re
import json
import pandas as pd
import math

PREFIX = 'capstone/2017-11/september/'

class DataLoader(object):
    
    prefix = PREFIX
    
    def __new__(cls, aws_access_key_id, aws_secret_access_key, Bucket):
        # initialize SpartSession
        print('initializing Spark')
        try:
            cls.spark = spark
        except NameError:
            cls.spark = SparkSession \
                .builder \
                .appName("PythonSpark") \
                .getOrCreate()
        # connect to s3 buckert
        print('connecting to AWS S3')
        cls.s3 = boto3.resource(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        cls.bucket = cls.s3.Bucket(Bucket)
        instance = super(DataLoader, cls).__new__(cls)
        return instance
        
    def listFiles(self):
        total_size = 0
        keys = list()
        for o in self.bucket.objects.filter(Prefix=self.prefix):
            print(o.key)
            keys.append(o.key)
            print('size: {}'.format(DataLoader.human_readable(o.size)))
            total_size += o.size
        print('total data size: {}'.format(DataLoader.human_readable(total_size)))
        return keys
    
    def downloadFile(self, Key):
        fileName = Key.replace(self.prefix, './')
        if os.path.exists(fileName):
            print('{} already exists.'.format(fileName))
        else:
            os.makedirs(os.path.dirname(fileName), exist_ok=True)
            try:
                print('Downloading data as {}'.format(fileName))
                self.bucket.download_file(Key, fileName)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise e
    
    def sparkRead(self, Key):
        fileName = Key.replace(self.prefix, './')
        if not os.path.exists(fileName):
            self.downloadFile(Key)
        return self.spark.read.parquet(fileName)
    
    def toJSON(self, Key, fraction='sqrt'):
        size = int(self.bucket.Object(Key).content_length)
        frac = 1
        if isinstance(fraction, float) amd fraction < 1 and fraction > 0:
            frac = fraction
        elif fraction == 'sqrt':
            size_in_m = size >> 20
            frac = round(math.sqrt(size_in_m)/size_in_m, 4)
        elif fraction == 'none':
            frac = 1
        else:
            print('fraction can only be postive numbers, "sqrt" or "none", "none" will be used instead.')
            frac = 1
        print('data size: {}'.format(DataLoader.human_readable(size)))
        print('when converting to JSON file, {}% will be used.'.format(frac*100))
        jsonPath = os.path.splitext(os.path.splitext(Key.replace(self.prefix, './'))[0])[0]
        if os.path.exists(jsonPath):
            print('{} already exists.'.format(jsonPath))
        else:
            spark_df = self.sparkRead(Key)
            if frac < 1:
                spark_df = spark_df.sample(withReplacement=False, fraction=frac)
            spark_df.write.json(jsonPath)
        
    def pandasRead(self, Key, n_row=1000000, fraction='sqrt'):
        tmp = list()
        pd_df = None
        jsonPath = os.path.splitext(os.path.splitext(Key.replace(self.prefix, './'))[0])[0]
        if not os.path.exists(jsonPath): 
            self.toJSON(Key, fraction=fraction)
        for file in os.listdir(jsonPath):
            if re.search('.json$', file):
                dataFile = os.path.join(jsonPath, file)
                print('importing file {}'.format(dataFile))
                with open(dataFile) as f:
                    for line in f:
                        row = DataLoader.flatten_row(json.loads(line))
                        tmp.append(row)
                if pd_df is None:
                    pd_df = pd.DataFrame.from_dict(tmp)
                else:
                    pd_df = pd.concat([pd_df, pd.DataFrame.from_dict(tmp)])
                print(pd_df.shape)
                if n_row > 0 and pd_df.shape[0] > n_row:
                    break 
        return pd_df
    
    @staticmethod
    def human_readable(size, decimal_places=2):
        for unit in ['','KB','MB','GB','TB']:
            if size < 1024.0:
                break
            size /= 1024.0
        return f"{size:.{decimal_places}f}{unit}"
    
    @staticmethod
    def flatten_row(row, key_sep='_', prefix=None):
        if not isinstance(row, dict):
            raise ValueError('unsupported data type: {}'.format(type(row)))
        flattened = dict()
        for key in row:
            if prefix is None:
                prefixed_key = key
            else:
                prefixed_key = prefix + key_sep + key
            if isinstance(row[key], dict):
                flattened.update(DataLoader.flatten_row(row=row[key], 
                                                        prefix=prefixed_key))
            else:
                flattened[prefixed_key] = row[key]
        return flattened
    
