# Databricks notebook source
# DBTITLE 1,Sample mount script ADLS Gen2 OAuth version
storage_account_name = '<YOUR ADLS GEN2 STORAGE ACCOUNT NAME>'
container_name = '<YOUR CONTAINER NAME>'
source = "wasbs://{container_name}@{storage_account_name}.dfs.core.windows.net".format(container_name = container_name, storage_account_name = storage_account_name)
mount_point = "/mnt/{location}".format(location = container_name)
cliend_id = '<YOUR APP CLIENT ID>'
secret = '<YOUR APP SECRET>'
directory_id = '<YOUR TENANT/DIRECTORY ID'

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": cliend_id,
           "fs.azure.account.oauth2.client.secret": secret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{directory_id}/oauth2/token".format(directory_id=directory_id)}
dbutils.fs.mount(source = source,mount_point = mount_point, extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,CDM classes and methods (class code can be hidden or refactored to a library)
from datetime import datetime
import json
import os

class commondatamodel():
    def __init__(self,name, cdm_folder, datalake_location, application = 'databricks',description = '',version = '1.0',culture = 'en-US',):
        self.model_file_path = '/dbfs/{cdm_folder}model.json'.format(cdm_folder = cdm_folder)
        if os.path.exists(self.model_file_path):
          f = open(self.model_file_path, 'r')
          self.model_data = json.loads(f.read())
          f.close()
        else:
          self.model_data = {'application': application, 
                           'name': name,
                           'description': description,
                           'version': version,
                           'culture': culture,
                           'modifiedTime' : datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
                           'isHidden' : False,
                           'entities' : [],
                           'referenceModels': [],
                           'relationships': []
                          }
        self.cdm_folder = cdm_folder
        self.datalake_location = datalake_location
    def add_entity(self, entity):      
        if entity.info['name'] in [x['name'] for x in self.model_data['entities']]:
          entity_index = next((index for (index, d) in enumerate(self.model_data['entities']) if d["name"] == entity.info['name']), None)
          self.model_data['entities'][entity_index] = entity.info
        else:
          self.model_data['entities'].append(entity.info)
    def to_json(self):
        return json.dumps(self.model_data)
    def save_model_json(self):
        self.modifiedTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
        model_path = '/dbfs/{cdm_folder}model.json'.format(cdm_folder = self.cdm_folder)
        f = open(model_path, 'w+')
        f.write(self.to_json())
        f.close()
    def write_to_cdm(self, dataframe_in, entity_name):
        write_file_name = '{cdm_folder}/{entity_name}/{entity_name}.csv.snapshots'.format(entity_name=entity_name, cdm_folder= self.cdm_folder)
        dataframe_in.write.mode('overwrite') \
                          .option("header", "False") \
                          .option("delimiter", ',') \
                          .option('quote','"') \
                          .option('escape','"') \
                          .option('quoteall','True') \
                          .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'") \
                          .csv(write_file_name)  
        entity_model = entity(self.cdm_folder, self.datalake_location, entity_name)
        entity_model.add_attributes(dataframe_in.dtypes)  
        entity_model.add_partitions()
        self.add_entity(entity_model)
    def read_from_cdm(self, entity_name):
        entity_index = next((index for (index, d) in enumerate(self.model_data['entities']) if d["name"] == entity_name), None)
        try:
          entity_info = self.model_data['entities'][entity_index]
        except IndexError:
          raise "Unable to find entity {entity_name} in common datamodel".format(entity_name = entity_name)
        partition_list = [x['location'].replace(datalake_location, cdm_folder) for x in entity_info['partitions']]
        dataframe_out = spark.read.option("header", "False").option("delimiter", ',').option('quote','"').option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'").csv(partition_list)
        return dataframe_out

class entity():
    def __init__(self, cdm_folder, datalake_location, entity_name):
        self.info = {"$type": 'LocalEntity','name': entity_name,'description' : '','annotations': [],'attributes': [],'partitions': []}
        self.cdm_folder = cdm_folder
        self.datalake_location = datalake_location
        self.entity_name = entity_name
    def add_attributes(self, dtypes):
        self.info['attributes'] = [{"name":  dtype[0], "dataType": self.spark_to_cdm_type(dtype[1])} for dtype in dtypes]
    def add_partitions(self):
        self.info['partitions'] = [{'location':self.datalake_location + '{entity_name}/{entity_name}.csv.snapshots/{file_name}'.format(entity_name = self.entity_name, file_name = x.name),
                                    'name': x.name.replace('.csv', '') } 
                                   for x in dbutils.fs.ls(self.cdm_folder + '{entity_name}/{entity_name}.csv.snapshots/'.format(entity_name = self.entity_name))
                                   if 'part-' in x.name]
    def spark_to_cdm_type(self, spark_type):
      #cdm_date_types = ['string', 'int64', 'double', 'dateTime', 'dateTimeOffset', 'decimal', 'boolean', 'GUID', 'JSON']
      conversion = {
        'int':'int64',
        'long':'int64',
        'float':'decimal',
        'decimal.Decimal':'decimal',
        'string': 'string',
        'bool': 'boolean',
        'timestamp': 'dateTime',
        'bigint':'int64'
      }
      return conversion.get(spark_type, 'string')  

# COMMAND ----------

# DBTITLE 1,Sample scenario 1: Directly adding data from wildcard pattern folder for each entity/dataset
name = 'databricks_flights_cdm' #CDM Name (will be in model.json file and appear in Power BI)
cdm_folder = 'mnt/cdm/flights/' #CDM location as mounted on dbfs
datalake_location = 'https://{storage_account_name}.dfs.core.windows.net/cdm/flights/'.format(storage_account_name=storage_account_name)  #Data lake path to CDM folder

#initialize commondatamodel class 
cdm = commondatamodel(name, cdm_folder, datalake_location)

#datasets to be transfered to CDM from multiple parquet files to CDM entity
datasets = ['aircrafts', 'airports', 'carriers', 'offer_sectors', 'offers', 'search', 'sectors', 'segments']

#creating entity for each dataset name
for dataset in datasets:
  try:
    path = 'mnt/flights/{{*}}/{dataset}.parquet'.format(dataset=dataset)
    df = spark.read.parquet(path)
    cdm.write_to_cdm(df, dataset)
  except Exception as e:
      print(e)
      pass
  
cdm.save_model_json()

# COMMAND ----------

# DBTITLE 1,Sample scenario 2: Copy all views from databricks database to CDM model
#required input parameters
name = 'model' #CDM Name (will be in model.json file and appear in Power BI)
cdm_folder = 'mnt/cdm/model/' #CDM location as mounted on dbfs
datalake_location = 'https://{storage_account_name}.dfs.core.windows.net/cdm/model/'.format(storage_account_name=storage_account_name) #Data lake path to CDM folder
cdm_database = 'model' #Databricks database name to move into CDM folder

#initialize commondatamodel class  
cdm = commondatamodel(name, cdm_folder, datalake_location)

#for all views in the selected database we create a common datamodel entity
for table in spark.catalog.listTables(cdm_database):
  if table.tableType == 'VIEW':
    try:
      df = spark.sql("SELECT * FROM {db}.{table}".format(db=table.database, table = table.name)).repartition(5)
      cdm.write_to_cdm(df, table.name)
    except Exception as e:
      print(e)
      pass

#method to update model.json file to reflect changes (partitions, new entity, etc)
cdm.save_model_json()