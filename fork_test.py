import os
import time
import socket
import pickle
import sys


def funct_1(p, ip, port):
    
    pid=os.fork()
    dict_a = {'index':[], 'from':[], 'to':[], 'pid':-1}
    if pid > 0:
        return(pid) #parent exits

    os.setsid() #move child to BG

    time.sleep(1)

    pkl_name = str(p) + '-' + str(pid) + ".pkl"

    #Create a socket instance
    socketObject = socket.socket()

    #Using the socket connect to a server...in this case localhost
    socketObject.connect((ip, port))

    dict_a['index'].append(str(p) + pkl_name)
    dict_a['from'].append(str(p) + "TMP_1")
    dict_a['to'].append(str(p) + "TMP_2")
    dict_a['pid'] = os.getpid()

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

file_list = []
tmp_dict = {'index':[], 'from':[], 'to':[], 'pid':[]}
start_time = time.time()

processes = 5
if (processes>len(processes_to_run)):
    processes = len(processes_to_run)-1

serverSocket.listen(processes)
pids = []

while (total < len(processes_to_run)):

    if(count<len(processes_to_run)):
        pid=funct_1(processes_to_run[count], ip, port)  #PD.loc(count)
        pids.append(pid)

        count += 1
        
    if len(pids) == processes or ((len(processes_to_run)-count)<processes):
        (clientConnection, clientAddress) = serverSocket.accept()

        data = pickle.loads(clientConnection.recv(15000))
        for x in tmp_dict:
            if x == 'pid':
                pids.remove(data[x])
            else:
                tmp_dict[x].append(data[x])
      
        total += 1


serverSocket.close()

total_time = time.time() - start_time

print("Ended", total_time)
exit()


