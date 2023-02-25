import requests
import os
from dotenv import load_dotenv
import mysql.connector
import random
import datetime
from datetime import date

env_loc=".env"
load_dotenv(env_loc)

def get_connection():
    return mysql.connector.connect(
    host=os.getenv('HOST'),
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD'),
    database=os.getenv('DATABASE')
    )

def get_cursor(conn):
    return conn.cursor()

def save_to_db(client,tests,add):
    client_insert="INSERT INTO clients VALUES(%s, %s, %s, %s, %s, %s)"
    test_insert="INSERT INTO tests (ClientID, status,date,ServiceID) VALUES(%s, %s, %s, %s)"
    get_serviceID_query="SELECT ServiceID FROM services WHERE LOWER(ServiceName) = LOWER(%s)"
    print(client)
    cursor.executemany(client_insert, add)
    test_list = [(test,) for test in tests]
    print(tests)
    print(test_list)
    for i in range(len(client)): 
        for j in range(len(tests)):
            if tests[j][0]==client[i][0]:
                print(tests[j][1])
                cursor.execute(get_serviceID_query,(tests[j][1],))
                ServiceID=cursor.fetchone()
                print(ServiceID)
                cursor.execute(test_insert,(client[i][1], "Pending", datetime.date.today(), ServiceID[0]))
                print("executed query:", test_insert, "with parameters:", client[i][1], "Pending", datetime.date.today(), ServiceID[0])
    
    db.commit()
    print("Data Extraction Success!")


def generateID():
    ID = random.randint(1, 9999)
    conn=get_connection()
    cursor=get_cursor(conn)
    query="SELECT id FROM medtechs"
    cursor.execute(query)
    result=cursor.fetchall()

    for i in result:
        if ID == i :
            ID = random.randint(1, 9999)
            break
        else:
            continue
    return ID

print('Connecting to Local Database')
db=get_connection()
print('Database Connection Established')
print("Establishing Cursor")
cursor=get_cursor(db)
print("Cursor Established")


URL=os.getenv('URL')
print("Getting Data")
re=requests.get(url=URL)
print("Data Extracted, Compiling")
data=re.json()

client_list=[]
client_to_add=[]
for client in data['Tests']:
    client_id=generateID()
    client_tup=(client['client_id'],client_id,client['name'],client['age'],client['gender'],client['birthdate'],client['address'])
    clientAdd=(client_id,client['name'],client['age'],client['gender'],client['birthdate'],client['address'])
    client_to_add.append(clientAdd)
    client_list.append(client_tup)


test_list=[]
test_client_list=[]
print(data['Tests'])
print(data['Clients'])
for test in data['Clients']:
    test_list.append(test['Service'])
    test_client_list.append(test['ClientId'])

test_tuple=list(zip(test_client_list,test_list))
print(test_tuple)
save_to_db(client_list,test_tuple,client_to_add)


