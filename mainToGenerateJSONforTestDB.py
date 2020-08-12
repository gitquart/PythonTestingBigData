import json
import uuid
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
    
    global row

    #Connect to Cassandra
    objCC=CassandraConnection()
    cloud_config= {
        'secure_connect_bundle': pathToHere+'\\secure-connect-dbquart.zip'
    }
    
    auth_provider = PlainTextAuthProvider(objCC.cc_user,objCC.cc_pwd)

    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.default_timeout=70
    row=''
    with open('json_base.json') as f:
        json_thesis = json.load(f)

    
    querySt="select * from thesis.tbthesis where period_number=10 ALLOW FILTERING "   
                
    future = session.execute_async(querySt)
    row=future.result()
    count=0
    statement = SimpleStatement(querySt, fetch_size=1000)
    for row in session.execute(statement):
        json_thesis['id_thesis']=str(row[0])
        json_thesis['book_number']=str(row[1])
        json_thesis['dt_publication_date']=str(row[2])
        json_thesis['heading']=str(row[3])
        json_thesis['instance']=str(row[4])
        json_thesis['jurisprudence_type']=str(row[5])
        json_thesis['lst_precedents']=str(row[6])
        json_thesis['multiple_subjects']=str(row[7])
        json_thesis['page']=str(row[8])
        json_thesis['period']=str(row[9])
        json_thesis['period_number']=str(row[10])
        json_thesis['publication']=str(row[11])
        json_thesis['publication_date']=str(row[12])
        json_thesis['source']=str(row[13])
        json_thesis['subject']=str(row[14])
        json_thesis['subject_1']=str(row[15])
        json_thesis['subject_2']=str(row[16])
        json_thesis['subject_3']=str(row[17])
        json_thesis['text_content']=str(row[18])
        json_thesis['thesis_number']=str(row[19])
        json_thesis['type_of_thesis']=str(row[20])


        #Insert Data as JSON
        json_thesis=json.dumps(json_thesis)

        f = open("demo.json", "a+")
        f.write(json_thesis)
        f.close()
        #wf.appendInfoToFile(dirquarttest,str(idThesis)+'.json', json_thesis)                
        insertSt="INSERT INTO thesis.tbthesis JSON '"+json_thesis+"';" 
        future = session.execute_async(insertSt)
        future.result()  
    
        count=count+1
        if count==1:
             break

    cluster.shutdown() 
                                


class CassandraConnection():
    cc_user='quartadmin'
    cc_pwd='P@ssw0rd33'
    cc_user_test='test'
    cc_pwd_test='testquart'
   


if __name__=='__main__':
    main()
