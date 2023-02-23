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

def save_to_db(client,tests):
    client_insert="INSERT INTO clients VALUES(%s, %s, %s, %s, %s, %s)"
    test_insert="INSERT INTO tests (ClientID, status,date,ServiceID) VALUES(%s, %s, %s, %s)"
    get_serviceID_query="SELECT ServiceID FROM services WHERE LOWER(ServiceName) = LOWER(%s)"
    print(client)
    cursor.executemany(client_insert, client)
    test_list = [(test,) for test in tests]
    print(test_list)
    for i in range(len(test_list)): 
        cursor.execute(get_serviceID_query,test_list[i])
        print("executed query:", get_serviceID_query, "with parameters:", test_list[i][0])
        ServiceID=cursor.fetchone()
        print(client[0][0])
        print(ServiceID)
        cursor.execute(test_insert,(client[0][0], "Pending", datetime.date.today(), ServiceID[0]))
    
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
for client in data['Tests']:
    client_id=generateID()
    client_tup=(client_id,client['name'],client['age'],client['gender'],client['birthdate'],client['address'])
    client_list.append(client_tup)


test_list=[]
for test in data['Clients']:
    test_list.append(test['Service'])
test_tuple=tuple(test_list)

save_to_db(client_list,test_tuple)


