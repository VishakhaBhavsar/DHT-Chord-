import hashlib
import sys
import pickle
import socket

HOST = '127.0.0.1'
BUF_SZ = 4096  # socket recv arg

if __name__ == '__main__':

    if(len(sys.argv) != 4):
       print('Usage: python chord_populate.py port playerid year')
       exit(1)

    port = int(sys.argv[1])
    playerid = sys.argv[2]
    year = sys.argv[3]

    keyName = playerid + year
    keyId = hashlib.sha1(keyName.encode()).digest()[19] & 0b111
    print(keyId)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, port))
        payload =('query_key', keyId, None)
        s.sendall(pickle.dumps(payload))
        data = s.recv(BUF_SZ)
    print('Received - ' , pickle.loads(data))


 




    
            
