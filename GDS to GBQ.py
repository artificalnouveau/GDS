import pandas as pd
import numpy as np
import re
import string
import uuid
import keyword
import os
import itertools

os.chdir("/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/")

#import data
GDS2013_raw=pd.read_csv("/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/GDS2013_labels_u.csv", engine='python')
GDS2014_raw=pd.read_csv("/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/GDS2014_labels_u.csv", engine='python')
GDS2015_raw=pd.read_csv("/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/GDS2015_labels_u.csv", engine='python')
GDS2016_raw=pd.read_csv("/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/GDS2016_labels_u.csv", engine='python')
GDS2017_raw=pd.read_csv("/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/GDS2017_labels_u.csv", engine='python')
GDS2018_raw=pd.read_csv("/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/GDS2018_labels_u.csv", engine='python')

#import dictionary
GDS_dictionary=pd.read_csv("/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/GDS_dictionary_final.csv", engine='python')
GDS_dictionary.columns=['all_names','category','three_key','three_value','var_2018','name_2018']

#convert dictionary to dictionary format
global_dict_names=dict(zip(GDS_dictionary.three_key, GDS_dictionary.three_value))
dict_names_2018=dict(zip(GDS_dictionary.var_2018, GDS_dictionary.name_2018))

#2013
#==================================================================
#make uuid and survey year an index
GDS2013_labels = GDS2013_raw.set_index(['syear', 'uuid'])
GDS2013_labels.replace(regex=True,inplace=True,to_replace=r'"',value=r'')

#change column names
GDS2013_labels.columns = [global_dict_names.get(x[:3], x) + x[3:] for x in GDS2013_labels.columns]

#create dictionary of the columns
dict_2013 = {k: g for k, g in GDS2013_labels.groupby(by=lambda x: x[:3].lower(), axis=1)}

#2014 
#======================================================
GDS2014_labels = GDS2014_raw.set_index(['syear', 'uuid'])
GDS2014_labels.replace(regex=True,inplace=True,to_replace=r'"',value=r'')

GDS2014_labels.columns = [global_dict_names.get(x[:3], x)+ x[3:] for x in GDS2014_labels.columns]

dict_2014 = {k: g for k, g in GDS2014_labels.groupby(by=lambda x: x[:3].lower(), axis=1)}

#2015 
#======================================================
GDS2015_labels = GDS2015_raw.set_index(['syear', 'uuid'])
GDS2015_labels.replace(regex=True,inplace=True,to_replace=r'"',value=r'')

GDS2015_labels.columns = [global_dict_names.get(x[:3], x)+ x[3:] for x in GDS2015_labels.columns]

dict_2015 = {k: g for k, g in GDS2015_labels.groupby(by=lambda x: x[:3].lower(), axis=1)}


#2016 
#======================================================
GDS2016_labels = GDS2016_raw.set_index(['syear', 'uuid'])
GDS2016_labels.replace(regex=True,inplace=True,to_replace=r'"',value=r'')

GDS2016_labels.columns = [global_dict_names.get(x[:3], x)+ x[3:] for x in GDS2016_labels.columns]

dict_2016 = {k: g for k, g in GDS2016_labels.groupby(by=lambda x: x[:3].lower(), axis=1)}


#2017 
#======================================================
GDS2017_labels = GDS2017_raw.set_index(['syear', 'uuid'])
GDS2017_labels.replace(regex=True,inplace=True,to_replace=r'"',value=r'')

GDS2017_labels.columns = [global_dict_names.get(x[:3], x)+ x[3:] for x in GDS2017_labels.columns]

dict_2017 = {k: g for k, g in GDS2017_labels.groupby(by=lambda x: x[:3].lower(), axis=1)}


#2018 
#======================================================
GDS2018_labels = GDS2018_raw.set_index(['syear', 'uuid'])
GDS2018_labels.replace(regex=True,inplace=True,to_replace=r'"',value=r'')

#change column names
GDS2018_labels.columns = [global_dict_names.get(x[:3], x) + x[3:] for x in GDS2018_labels.columns]

dict_2018 = {k: g for k, g in GDS2018_labels.groupby(by=lambda x: x[:3].lower(), axis=1)}


#MERGING ALL DICTIONARIES
#===============================================================
from collections import defaultdict

global_dict = defaultdict(list, {k: [v] for k, v in dict_2013.items()})

for key,val in dict_2014.items():
    global_dict[key].append(val)

for key,val in dict_2015.items():
    global_dict[key].append(val)
    
for key,val in dict_2016.items():
    global_dict[key].append(val)
    
for key,val in dict_2017.items():
    global_dict[key].append(val)
    
for key,val in dict_2018.items():
    global_dict[key].append(val)

#check no
len(set(global_dict.keys()))

#BIG Query   
#=================================================================
#https://stackoverflow.com/questions/36314797/write-a-pandas-dataframe-to-google-cloud-storage-or-bigquery
#http://oswco.com/2016/mar/31/few-hints-google-cloud-datalab/

from datalab.context import Context
import google.datalab.storage as storage
import google.datalab.bigquery as bq

#credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/ahnzhu/Documents/coding_projects/GDS/database_v2/GDS database-ef4e58a12ef6.json"

def clean_dataset(category):
    category.columns = [x.lower() for x in category.columns]
    category=category.groupby(category.columns, 1, sort=False).first()
    category=category.reset_index(level=['syear', 'uuid'])
    category=category.fillna('NaN')
    category=category.astype(str)
    category.columns =[col[4:] if '_' in col else col for col in category.columns]
    category.drop(['syear.1'], axis=1)
    return category

#insert dataframe into bigquery
def df_to_bq(insert_dataframe):
    table_schema = bq.Schema.from_data(insert_dataframe)
    table.create(schema = table_schema, overwrite = True)
    table.insert(insert_dataframe)
   
#BIG QUERY Upload
#=================================================
#stand alone:
#=====================
sample_bucket_name='gds-database'
sample_bucket_path = 'gs://' + sample_bucket_name
sample_bucket_object = sample_bucket_path + '/GDS_annual.txt'
bigquery_dataset_name = 'GDS'
bigquery_table_name = 'GDS2013'
sample_bucket = storage.Bucket(sample_bucket_name)
if not sample_bucket.exists():
    sample_bucket.create()
dataset = bq.Dataset(bigquery_dataset_name)
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
dataset.location = 'EU'
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
    
#bq_df=pd.concat(global_dict['amp'], axis=1, ignore_index=False)
#bq_df_cleaned=clean_dataset(GDS2013)
df_to_bq(GDS2013_raw)

sample_bucket_name='gds-database'
sample_bucket_path = 'gs://' + sample_bucket_name
sample_bucket_object = sample_bucket_path + '/GDS_annual.txt'
bigquery_dataset_name = 'GDS'
bigquery_table_name = 'GDS2014'
sample_bucket = storage.Bucket(sample_bucket_name)
if not sample_bucket.exists():
    sample_bucket.create()
dataset = bq.Dataset(bigquery_dataset_name)
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
dataset.location = 'EU'
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
    
#bq_df=pd.concat(global_dict['amp'], axis=1, ignore_index=False)
#bq_df_cleaned=clean_dataset(GDS2013)
df_to_bq(GDS2014_raw)

sample_bucket_name='gds-database'
sample_bucket_path = 'gs://' + sample_bucket_name
sample_bucket_object = sample_bucket_path + '/GDS_annual.txt'
bigquery_dataset_name = 'GDS'
bigquery_table_name = 'GDS2015'
sample_bucket = storage.Bucket(sample_bucket_name)
if not sample_bucket.exists():
    sample_bucket.create()
dataset = bq.Dataset(bigquery_dataset_name)
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
dataset.location = 'EU'
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
    
#bq_df=pd.concat(global_dict['amp'], axis=1, ignore_index=False)
#bq_df_cleaned=clean_dataset(GDS2013)
df_to_bq(GDS2015_raw)

sample_bucket_name='gds-database'
sample_bucket_path = 'gs://' + sample_bucket_name
sample_bucket_object = sample_bucket_path + '/GDS_annual.txt'
bigquery_dataset_name = 'GDS'
bigquery_table_name = 'GDS2016'
sample_bucket = storage.Bucket(sample_bucket_name)
if not sample_bucket.exists():
    sample_bucket.create()
dataset = bq.Dataset(bigquery_dataset_name)
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
dataset.location = 'EU'
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
    
#bq_df=pd.concat(global_dict['amp'], axis=1, ignore_index=False)
#bq_df_cleaned=clean_dataset(GDS2013)
df_to_bq(GDS2016_raw)

sample_bucket_name='gds-database'
sample_bucket_path = 'gs://' + sample_bucket_name
sample_bucket_object = sample_bucket_path + '/GDS_annual.txt'
bigquery_dataset_name = 'GDS'
bigquery_table_name = 'GDS2017'
sample_bucket = storage.Bucket(sample_bucket_name)
if not sample_bucket.exists():
    sample_bucket.create()
dataset = bq.Dataset(bigquery_dataset_name)
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
dataset.location = 'EU'
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
    
#bq_df=pd.concat(global_dict['amp'], axis=1, ignore_index=False)
#bq_df_cleaned=clean_dataset(GDS2013)
df_to_bq(GDS2017_raw)


sample_bucket_name='gds-database'
sample_bucket_path = 'gs://' + sample_bucket_name
sample_bucket_object = sample_bucket_path + '/GDS_annual.txt'
bigquery_dataset_name = 'GDS'
bigquery_table_name = 'GDS2018'
sample_bucket = storage.Bucket(sample_bucket_name)
if not sample_bucket.exists():
    sample_bucket.create()
dataset = bq.Dataset(bigquery_dataset_name)
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
dataset.location = 'EU'
table = bq.Table(bigquery_dataset_name + '.' + bigquery_table_name)
if not dataset.exists():
    dataset.create()
    
#bq_df=pd.concat(global_dict['amp'], axis=1, ignore_index=False)
#bq_df_cleaned=clean_dataset(GDS2013)
df_to_bq(GDS2018_raw)

