import csv
import hashlib
import sys
import pickle
import socket

HOST = '127.0.0.1'
MAX_ROWS = 5000 # FIXME: Controling number of rows to add; Should be set as MAX rows in the sheet
START_AT_ROW = 1 # FIXME: Controling which row to start at; Should be set as 1
BUF_SZ = 4096  # socket recv arg

if __name__ == '__main__':

    if(len(sys.argv) != 3):
       print('Usage: python chord_populate.py port fileName')
       exit(1)

    port = int(sys.argv[1])
    fileName = sys.argv[2]

    #nodeName = HOST + str(port)
    #nodeId = hashlib.sha1(nodeName.encode()).digest()[19] & 0b111
    #print(nodeId)

    with open(fileName) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        i = 0
        for row in readCSV:
            if i==0 or i < START_AT_ROW:
                i += 1
                continue
            elif i >= START_AT_ROW + MAX_ROWS:
                break
            else:
                i += 1
                keyName = row[0] + row[3]
                keyId = hashlib.sha1(keyName.encode()).digest()[19] & 0b111
                print(keyId)
                #TODO Ask the nodeId to add this keyId into the DHT
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                   s.connect((HOST, port))
                   payload = ('add_key', keyId, row)
                   s.sendall(pickle.dumps(payload))
                   data = s.recv(BUF_SZ)
                print('Received - ', pickle.loads(data))

 




    
            
