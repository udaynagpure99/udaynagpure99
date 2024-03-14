import json
import sys
import pandas as pd
from equalexperts_dataeng_exercise.scripts.exercise import *

# The following code is purely illustrative
try:
    # filename = "uncommitted/votes.jsonl"
    data=[] # creating a list
    with open(sys.argv[1]) as votes_in:
        for line in votes_in:
    # print(json.loads(line))
            data.append(json.loads(line))
    # break
    df = pd.DataFrame(data)
    #df.to_csv("output/equal_experts-data.csv", index=False)
    num_of_rec_original = df.shape[0]
    print("-*-"*50)

    df_unique = df.drop_duplicates()
    
    num_of_unique_rec = df_unique.shape[0]
    print("-*-"*50)

    duplicate_rec = num_of_rec_original - num_of_unique_rec

    if duplicate_rec == 0:
        print("No duplicate records found\n")
    else:
        print(f"Duplicate records found: {duplicate_rec}\n")


    create_schema = "CREATE SCHEMA IF NOT EXISTS blog_analysis"
    drop_table = "DROP TABLE IF EXISTS blog_analysis.votes"
    create_table = """CREATE TABLE IF NOT EXISTS blog_analysis.votes(
        Id INTEGER,
        PostID INTEGER,
        VoteTypeId INTEGER,
        CreationDate TIMESTAMP,
        UserId INTEGER,
        BountyAmount INTEGER        
        )
    """
    df_rec = pd.DataFrame(df_unique,columns=['Id','PostID','VoteTypeId','CreationDate','UserId','BountyAmount'])
    # df_rec =df_unique.copy()
    df_rec['PostID']= df_rec['PostID'].fillna(-1) 
    df_rec['VoteTypeId']=df_rec['VoteTypeId'].fillna(-1)
    df_rec['UserId']=df_rec['UserId'].fillna(-1)    
    df_rec['BountyAmount']=df_rec['BountyAmount'].fillna(-1)

    # print(df_rec)
    # get_data = "SELECT * FROM blog_analysis.votes"

    run_query(query=create_schema)
    print("Schema Created Successfully\n")

    run_query(query=drop_table)
    run_query(query=create_table)
    print("Table Created Successfully\n")
   
    # run_query_output(query="INSERT INTO blog_analysis.votes SELECT Id,PostID,VoteTypeId,CreationDate,UserId,BountyAmount  FROM df_rec")
    run_query(query="INSERT INTO blog_analysis.votes SELECT Id,IF(PostID == '-1',NULL,PostID),IF(VoteTypeId == '-1',NULL,VoteTypeId),IF(CreationDate == '-1',NULL,CreationDate),IF(UserId == '-1',NULL,UserId),IF(BountyAmount == '-1',NULL,BountyAmount) FROM df_rec")
    print("Data Inserted Successfully\n")

except FileNotFoundError:
    print("Please download the dataset using 'poetry run exercise fetch-data")
