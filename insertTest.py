import json
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra.query import BatchStatement
import os
import time

#uuid.uuid4
pathToHere=os.getcwd()
def main():
    #Read the only one record in database in store in txt file
    cassandraBDProcess()


def cassandraBDProcess():

    
    print('START...')
    

    for i in range(1,226):
        #Connect to Cassandra
        objCC=CassandraConnection()
        cloud_config= {
            'secure_connect_bundle': pathToHere+'\\secure-connect-dbtest.zip'
        }
    
        auth_provider = PlainTextAuthProvider(objCC.cc_user_test,objCC.cc_pwd_test)

        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        
        session = cluster.connect()
        session.default_timeout=20

        #Start of the batch
        batchCounter=0
        batch = BatchStatement()
        while batchCounter<=100: 
            with open('demo.json',encoding='utf-8') as f:
                json_thesis = json.load(f)   
            json_thesis['guid_thesis']=str(uuid.uuid4())
            json_thesis=json.dumps(json_thesis)
            insertSt="INSERT INTO test.tbthesis JSON '"+json_thesis+"';" 
            batch.add(SimpleStatement(insertSt))
            batchCounter=batchCounter+1
            

        session.execute(batch)
        print("Batch:",str(i)) 
        time.sleep(6)   

        cluster.shutdown()
        
                                


class CassandraConnection():
    cc_user_test='test'
    cc_pwd_test='testquart'
   


if __name__=='__main__':
    main()
