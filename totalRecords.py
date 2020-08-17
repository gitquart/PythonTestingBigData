from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import os

#uuid.uuid4
pathToHere=os.getcwd()
def main():
    #Read the only one record in database in store in txt file
    cassandraBDProcess()


def cassandraBDProcess():
    
    
    #Connect to Cassandra
    objCC=CassandraConnection()
    cloud_config= {
            'secure_connect_bundle': pathToHere+'\\secure-connect-dbtest.zip'
    }
    
    auth_provider = PlainTextAuthProvider(objCC.cc_user_test,objCC.cc_pwd_test)

    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.default_timeout=70
  
    querySt="select * from test.tbthesis where period_number>4 ALLOW FILTERING "   
        
    count=0
    row=''
    statement = SimpleStatement(querySt, fetch_size=1000)
    
    for row in session.execute(statement):
        count=count+1
        
    print('Count',str(count)) 
       

    cluster.shutdown() 
                                


class CassandraConnection():
    cc_user_test='test'
    cc_pwd_test='testquart'
   


if __name__=='__main__':
    main()
