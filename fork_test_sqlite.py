import os
import time
import socket
import pickle
import sys
import random
import sqlite3 as slite


def funct_1(p, ip, port):
    
    pid=os.fork()
    dict_a = {'index':[], 'from':[], 'to':[]}
    if pid > 0:
        return(pid) #parent exits

    os.setsid() #move child to BG
    print ("Child process started: ", os.getpid())

    #sleep_time=random.randint(3,10)
    #sleep_time=1    
    #time.sleep(sleep_time)

    pkl_name = str(p) + '-' + str(os.getpid()) + ".pkl"

    #Create a socket instance
    socketObject = socket.socket()

    #Using the socket connect to a server...in this case localhost
    socketObject.connect((ip, port))

    dict_a['index'].append(pkl_name)
    dict_a['from'].append(str(p) + "--TMP_1")
    dict_a['to'].append(str(p) + "--TMP_2")

    conn = slite.connect("messages.db")
    cursor = conn.cursor()

    # create a table

    insert_str = 'insert into messages (pid, message) values (?,?)'
    insert_tuple = (os.getpid(), pickle.dumps(dict_a))
    #print()

    cursor.execute(insert_str, insert_tuple)

    conn.commit()
    conn.close()
 

    socketObject.sendall(pickle.dumps(os.getpid()))
    socketObject.close()

    #print ("Child process ended: ", str(os.getpid()), " slept for: ", sleep_time)

    os._exit(0) #child exit.


if __name__ == "__main__":

    processes_to_run = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,50]

    # Create a server socket
    serverSocket = socket.socket()

    # Associate the server socket with the IP and Port
    ip = "127.0.0.1"
    port = 35491
    serverSocket.bind((ip, port))

    # Make the server listen for incoming connections load up to the number of processes


    count = 0
    total = 0

    file_list = []
    tmp_dict = {'index':[], 'from':[], 'to':[], 'pid':[]}
    start_time = time.time()

    processes = 5
    if (processes>len(processes_to_run)):
        processes = len(processes_to_run)-1

    serverSocket.listen(processes)
    pids = []

    #create DB if not there and the table.
    conn = slite.connect("messages.db")
    cursor = conn.cursor()

    # create a table
    cursor.execute("""CREATE TABLE if not exists messages (pid int, message blob)""")
    cursor.execute('''delete from messages''')

    conn.commit()
    conn.close()
    

    while (total < len(processes_to_run)):

        if(count<len(processes_to_run)):
            pid=funct_1(processes_to_run[count], ip, port)  #PD.loc(count)
            pids.append(pid)

            count += 1
        
        if len(pids) == processes or ((len(processes_to_run)-count)<processes):
            (clientConnection, clientAddress) = serverSocket.accept()

            data = pickle.loads(clientConnection.recv(15000))
            pids.remove(data)

     
            total += 1


    serverSocket.close()

    total_time = time.time() - start_time
    
    #create DB if not there and the table.
    conn = slite.connect("messages.db")
    cursor = conn.cursor()

    # create a table
    cursor.execute("""select * from messages""")
    rows = cursor.fetchall()

    for row in rows:
        unpickled = pickle.loads(row[1])
        #print (row[0], unpickled)
        
        for x in unpickled:
            tmp_dict[x].append(unpickled[x])


    conn.close()

    print(tmp_dict)
    print("Ended", total_time)
    exit()


