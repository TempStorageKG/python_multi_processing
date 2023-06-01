import os
import time
import socket
import pickle
import pandas as pd
import sys
import psycopg2 as dba


def funct_1(p, ip, port):
    
    pid=os.fork()

    dict_a = {'index':[], 'from':[], 'to':[], 'pid':-1}
    
    if pid > 0:
        return(pid) #parent exits

    os.setsid() #move child to BG

    conn = dba.connect(dbname='test_gis', host='127.0.0.1', port='5432', user='uploadfs', password='upload99')
    cur = conn.cursor()
    sql_exec = '''select count(*) from mygeography4617'''

    cur.execute(sql_exec)
    print("Found", cur.fetchall())
    conn.close()

    time.sleep(1)
    pkl_name = str(p) + '-' + str(pid) + ".pkl"

    #Create a socket instance
    socketObject = socket.socket()
    #Using the socket connect to a server...in this case localhost
    socketObject.connect((ip, port))
    #print("Client connected to localhost")

    dict_a['index'].append(str(p) + pkl_name)
    dict_a['from'].append(str(p) + "Nenad")
    dict_a['to'].append(str(p) + "Jevtic")
    dict_a['pid'] = os.getpid()

    #print(sys.getsizeof(pickle.dumps(dict_a)))

    socketObject.sendall(pickle.dumps(dict_a))
    socketObject.close()

    os._exit(0) #child exit.

num_elements = 100

counter = 0 

processes_to_run = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]

# Create a server socket
serverSocket = socket.socket()

# Associate the server socket with the IP and Port
ip = "127.0.0.1"
port = 35491
serverSocket.bind((ip, port))

# Make the server listen for incoming connections load up to the number of processes


count = 0
i = 0
total = 0
file_list = []
tmp_dict = {'index':[], 'from':[], 'to':[], 'pid':[]}
start_time = time.time()

processes = 30
if (processes>len(processes_to_run)):
    processes = len(processes_to_run)-1

print(processes)
serverSocket.listen(processes)

runner = True

pids = []

while (total < len(processes_to_run)):

    if(count<len(processes_to_run)):
        pid=funct_1(processes_to_run[count], ip, port)  #PD.loc(count)
        pids.append(pid)
        
        count = count + 1

    #i = i + 1
 
    #if pid>0 and i == processes:
    if len(pids) == processes or ((len(processes_to_run)-count)<processes):
        #print("Adding", pids, len(pids), (len(processes_to_run)-count))        
        (clientConnection, clientAddress) = serverSocket.accept()


        data = pickle.loads(clientConnection.recv(15000))
        #print(data.decode('utf-8'))
        #print("Parent received text:", pickle.loads(data))

        for x in tmp_dict:
            if x == 'pid':
                pids.remove(data[x])
            else:
                tmp_dict[x].append(data[x])
      
        #print("Removed", pids)
        total = total + 1
        #i = i - 1
    
    #print("looping")


serverSocket.close()
print("exiting")
print(pids)
total_time = time.time() - start_time

print("Ended", total_time)
print(tmp_dict) 

'''if pid == 0:
    cmd = input("child command")
elif pid > 0:
    #cmd = input("parent command")
else:
    print("not sure")
'''
print("Server done")
   

exit()


