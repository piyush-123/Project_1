import pymongo
import pandas as pd
import json

client = pymongo.MongoClient("mongodb+srv://piyush1304:System911@cluster0.gocvn.mongodb.net/?retryWrites=true&w=majority")

DATABASE_NAME = 'app'
COLLECTION_NAME = 'sensor'
DATA_FILE_PATH = 'aps_failure_training_set1.csv'
database = client[DATABASE_NAME]

collection = database[COLLECTION_NAME]

if __name__ == "__main__":
    df = pd.read_csv(DATA_FILE_PATH)
    print(f"the number of rows and columns are : {df.shape}")

    #convert dataframe to json format
    df.reset_index(drop=True,inplace=True)

    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)
    






  
