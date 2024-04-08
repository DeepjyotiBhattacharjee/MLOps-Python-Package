from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import json

uri = "mongodb+srv://deepjyotibhattacharjee217:Snape1993@cluster0.eiomi4b.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

DATA_FILE_PATH = 'notebooks/data/aps_failure_training_set.csv'
DATABASE_NAME = 'aps'
COLLECTION_NAME = 'sensor'

if __name__ == '__main__':
    df = pd.read_csv(DATA_FILE_PATH)
    print(f"Rows and columns  of data frame: {df.shape}")
    print(df.head())

    # Convert dataframe to json to dump it to mongo
    df.reset_index(drop=True,inplace=True)
    print(df.tail())

    json_records = list(json.loads(df.T.to_json()).values())
    print(json_records[0])

    # insert converted records to mongo db
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_records)