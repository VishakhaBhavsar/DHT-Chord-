"""
Created on Sun Nov 17 14:50:29 2019
@author: vishakhabhavsar
"""

import socket
import pickle
import sys
import threading
import hashlib
import traceback

HOST = '127.0.0.1'

M = 3  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n

"""
ModRange Class
"""
class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr
    
    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    """

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ' [{},{}) '.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)

"""
ModRangeIter Class
"""
class ModRangeIter(object):
    """ Iterator class for ModRange """
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]

"""
FingerEntry Class
"""
class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe
    
    >>> fe.node = 1
    >>> fe
    
    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True
    """
    def __init__(self, n, k, node=None, nodePort=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node
        self.nodePort = nodePort

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return '\nstart={}, interval={}, node={}/{}'.format(self.start, self.interval, self.node, self.nodePort)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval
    

"""
ChordNode Class
"""
class ChordNode(object):
    def __init__(self, n, nPort):
        print('Created a node with id {}'.format(n))
        self.node = n
        self.nodePort = nPort
        self.finger = [None] + [FingerEntry(n, k) for k in range(1, M+1)]  # indexing starts at 1
        self.predecessor = None
        self.predecessorPort = None
        self.keys = {}
        
    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        self.finger[1].node = id
        
    def get_successor(self):
        return (self.finger[1].node, self.finger[1].nodePort)
    
    def get_predecessor(self):
        return (self.predecessor, self.predecessorPort)
    
    def set_predecessor(self, np, npPort):
         self.predecessor = np
         self.predecessorPort = npPort
         print(self)
         
           
    @staticmethod
    def start_a_server():
        """
        Start a listener on a random port
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('localhost', 0))  # subscriber binds the socket to the publishers address
        server.listen(BACKLOG)
        ip, port = server.getsockname()
        print('starting up on {} port {}'.format(ip, port))
        
        return server

    def update_others(self):
        """ 
        Update all other node that should have this node in their finger tables 
        Invoked when a new node joins an existing node
        """
        print('update_others()')
        for i in range(1, M+1):  # find last node p whose i-th finger might be this node
            np, npPort = self.find_predecessor(( self.node - 2**(i-1) + NODES) % NODES)
            self.call_rpc(np, npPort, 'update_finger_table', self.node, i)

    def update_finger_table(self, s, i):
        """ 
        if s (newly joined node) is i-th finger of n (current node), update current node's finger table with s 
        Invoked via RPC when a new node joins an existing node
        """
        print('update_finger_table()')
        if (self.finger[i].start != self.finger[i].node  # FIXME: don't want e.g. [1, 1) which is the whole circle
                and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):  # FIXME: bug in paper, [.start
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(s, i, self.node, i, s, s,
                                                                                       self.finger[i].start,
                                                                                       self.finger[i].node))
            self.finger[i].node = s
            print(self)
#            p = self.predecessor  # get first node preceding myself
#            if(s != self.predecessor):
            self.call_rpc(self.predecessor,self.predecessorPort, 'update_finger_table', s, i)
            return 'updated {}'.format(self.node) #str(self)
        return 'did nothing {}'.format(self.node)
    
    def handle_rpc(self, client):
        """
        Handle the incoming RPC call to this node by some other node
        """
        print('\nhandle_rpc()')
        rpc = client.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        print('Received RPC for {}({},{})'.format(method, arg1, arg2))
        result = self.dispatch_rpc(method, arg1, arg2)
        print('Returning - {}({},{}) = {}'.format(method, arg1, arg2, result))
        client.sendall(pickle.dumps(result))

    def dispatch_rpc(self, method, arg1, arg2):
         """
         Invoke the given method on this node(self) with given args and return the response
         """
         print('dispatch_rpc()')
         try:
            func = getattr(self, method)
            
            print(type(func).__name__)
            
            if(type(func).__name__ in ['function', 'method' ]):
                if(arg1 is None):
                    result = func()
                elif(arg2 is None):
                    result = func(arg1)
                else:
                    result = func(arg1, arg2)
            else: #if(type(func).__name__ == 'property'):
                result = func
         except AttributeError:
            traceback.print_exc()
            result = "{} not found".format(method)
            
         return result
         
    def call_rpc(self, nodeId, nodePort, method, arg1=None, arg2=None):
        """
        Connects to the remote node id and issues an RPC call for the given method with given args
        """
        print('call_rpc()')
        
        if(nodeId == self.node):
            return self.dispatch_rpc(method, arg1, arg2) # Saving from making a RPC to oneself
        else:
            print('Making RPC to {}/{}.{}({},{})'.format(nodeId, nodePort, method, arg1, arg2))
            # May be use node id to find out its listener ip+port, create/connect a socket and pass the payload
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((HOST, nodePort))
                payload =(method, arg1, arg2)
                s.sendall(pickle.dumps(payload))
                data = s.recv(BUF_SZ)
            print('Received RPC Response {}.{}({},{}) = {}'.format(nodeId, method, arg1, arg2, pickle.loads(data)))
            return pickle.loads(data)

    def find_successor(self, id):
        """
        Ask this node to find id's successor = successor(predecessor(id))
        Find successor works by finding the immediate predecessor node of the desired identifier; 
        the successor of that node must be the successor of the identifier.
        """
        print('find_successor()')
        
        for i in range(1, M+1):
            if(id == self.finger[i].start):
                return (self.finger[i].node, self.finger[i].nodePort)
        
        np, npPort = self.find_predecessor(id)
        np, npPort = self.call_rpc(np, npPort, 'get_successor')
        
        print('Found successor for id {} to be node {}'.format(id, np))
        return (np, npPort)
    
    def find_predecessor(self, id):
        print('find_predecessor()')
        np = self.node
        npPort = self.nodePort
#        npSuccessor = self.call_rpc(np, npPort, 'successor')
        npSuccessor = self.successor
        
        while id not in ModRange(np, npSuccessor, NODES):
            np, npPort = self.call_rpc(np, npPort, 'closest_preceding_finger', id)
        
        print('Found predecessor for id {} to be node {}'.format(id, np))
        return (np, npPort)
            
    def closest_preceding_finger(self, id):
        """
        closest_preceding_finger
        """
        np = self.node
        npPort = self.nodePort
        
        for i in range(M, 0, -1):
            if(id in self.finger[i]):
                np = self.finger[i].node
                npPort = self.finger[i].nodePort
                
        return (np, npPort)
        
    def init_finger_table(self, npId, npPort):
        """
        initialize finger table of local node by asking the given node to look them up
        id is an arbitrary node already in the network
        """
        print('init_finger_table()')
        # Set self successor i.e 1st finger's node/successor
        self.finger[1].node, self.finger[1].nodePort = self.call_rpc(npId, npPort, 'find_successor', self.finger[1].start)
        # Set self predesessor to successor's predesessor
        self.predecessor, self.predecessorPort = self.call_rpc(self.finger[1].node, self.finger[1].nodePort, 'get_predecessor')
        # Set successor's predesessor to self
        self.call_rpc(self.finger[1].node, self.finger[1].nodePort, 'set_predecessor', self.node, self.nodePort)
        
        #Set rest of the finger table entries
        for i in range(1, M):
            if(self.finger[i+1].start in ModRange(self.node, self.finger[i].node, NODES)):
                self.finger[i+1].node = self.finger[i].node
                self.finger[i+1].nodePort = self.finger[i].nodePort
            else:
                self.finger[i+1].node, self.finger[i+1].nodePort = self.call_rpc(npId, npPort, 'find_successor', self.finger[i+1].start)
        print(self) # TODO Remove this
    
    def join(self, npId=None, npPort=None):
        """
        id is an arbitrary node already in the network
        """
        print('join()')
        if(npPort != 0):
            print('Joining existing network')
            self.init_finger_table(npId, npPort)
            self.update_others()
            #TODO - Move keys
        else: #n is the only node in the network
            print('Starting a new network')
            for i in range(1, M+1):
                self.finger[i].node = self.node
                self.finger[i].nodePort = self.nodePort
            
            self.predecessor = self.node
            self.predecessorPort = self.nodePort
    
    def add_key(self, keyId, value):
        """
        chord_populate.py invokes this method on a node to add keys into DHT
        1. Find the successor node of given keyId
        2. Update key dictionary with given id and value on the successor
        Note - If successor is same as current node, direct add/update else RPC to successor node
        """
        print('add_key()')
        np, npPort = self.find_successor(keyId)
        
        if(np == self.node):
            self.keys[keyId] = value
            print('Added keyId {} at node {}'.format(keyId, np))
        else:
            self.call_rpc(np, npPort, 'add_key', keyId, value)
            
        print(self)
        return 'Stored key {} at node {}'.format(keyId, np)

    def query_key(self, keyId):
        """
        chord_query.py invokes this method on a node to find a key with keyId in DHT
        1. Find the successor node of given keyId
        2. Return the value stored in the key dictionary for the given keyId on the successor
        Note - If successor is same as current node, direct get else RPC to successor node
        """
        print('query_key()')
        np, npPort = self.find_successor(keyId)
        if(np == self.node):
            print('Found keyId {} at node {}'.format(keyId, np))
            return self.keys[keyId] if keyId in self.keys else "Not Found"
        else:
            return self.call_rpc(np, npPort, 'query_key', keyId)
        
    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return '[\nnode={}/{}\n FingerTable=\n{}, \n predecessor={}/{} \n keys={}\n]'.format(self.node, self.nodePort, self.finger, self.predecessor, self.predecessorPort, self.keys.keys())

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python chord_node.py port")
        print("port - The port number of an existing node (or 0 to indicate it should start a new network)")
        exit(1);
    npPort = int(sys.argv[1])
    
    server = ChordNode.start_a_server()
    
    serverIp , serverPort = server.getsockname()
    nodeName = serverIp + str(serverPort)
    nodeId = hashlib.sha1(nodeName.encode()).digest()[19] & 0b111
    node = ChordNode(nodeId, serverPort)
    
    npName = HOST + str(npPort)
    npId = hashlib.sha1(npName.encode()).digest()[19] & 0b111

#    node.join(npId, npPort)
    threading.Thread(target=node.join, args=(npId, npPort,)).start()
    
    print(node)
    
    while True:
            client, client_addr = server.accept()
            threading.Thread(target=node.handle_rpc, args=(client,)).start()


