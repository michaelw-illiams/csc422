# NOTE: The order of functions in this file has been modified.
import sys
from mpi4py import MPI
from dht_globals import *
from command import commandNode

data = {}
active = False
parentId = None
childRank = None
storageId = None
numProcesses = None
rank = None

# Finalize DHT system from the head node.
# Waits for END signal from command node then sends END to all storage nodes and exits.
def headEnd():
    dummy = MPI.COMM_WORLD.recv(source=numProcesses - 1, tag=END)
    for i in range(1, numProcesses - 1):
        MPI.COMM_WORLD.send(dummy, dest=i, tag=END)
    MPI.Finalize()
    sys.exit(0)

# Waits for END signal from the head and exits.
def storageEnd(): 
    dummy = MPI.COMM_WORLD.recv(source=0, tag=END)
    MPI.Finalize()
    sys.exit(0)

# Handles the END command from the command node.
# Call headEnd or storageEnd depending on the node's rank.
def end():
    if rank == 0:
        headEnd()
    else:
        storageEnd()

# Initialize node's storage ID, parent ID, and child rank. 
def initializeNode(source):
    global storageId, parentId, childRank, active
    parentRank, newChildRank, parentStorageId, id = MPI.COMM_WORLD.recv(source=source, tag=INIT_NODE)
    storageId = id
    parentId = parentStorageId
    childRank = newChildRank
    active = True

# Updates node’s parent ID after a neighboring node has been added or removed.
def updateParentId(source):
    global parentId
    parentId = MPI.COMM_WORLD.recv(source=source, tag=UPDATE_PARENT)

# Responds to signal for node's storage ID.
# Return -1 if node doesn't exist.
def processStorageId(source):
    MPI.COMM_WORLD.recv(source=source, tag=STORAGE_ID)
    response = storageId
    if not active:
        response = -1
    MPI.COMM_WORLD.send(response, dest=source, tag=STORAGE_ID)

# Transfer keys to new node and remove from original node. 
def sendMovedKeys(newRank, id):
    toSend = {}
    for k, v in data.items():
        if parentId < k and k <= id:
            toSend[k] = v
    for k in toSend:
        del data[k]
    MPI.COMM_WORLD.send(toSend, dest=newRank, tag=MOVE_KEYS)

# Receives and stores key-value pairs sent from another node.
def moveKeys(source):
    movedData = MPI.COMM_WORLD.recv(source=source, tag=MOVE_KEYS)
    data.update(movedData)

# Removes nodde from the DHT.
# Transfers its data back to the head and signal the node's parent.
def removeNode(source):
    global active, storageId, parentId, childRank
    parentRank = MPI.COMM_WORLD.recv(source=source, tag=REMOVE_NODE)
    for k, v in data.items():
        MPI.COMM_WORLD.send([k, v], dest=0, tag=PUT)
    data.clear()
    MPI.COMM_WORLD.send(childRank, dest=parentRank, tag=NEW_CHILD_RANK)
    active = False
    storageId = None
    parentId = None
    childRank = None

# Send info to the new node being added.
# Information consists of initial parent, child and storage ID. 
def sendInfoToNode(newRank, id):
    info = (rank, childRank, storageId, id)
    MPI.COMM_WORLD.send(info, dest=newRank, tag=INIT_NODE)

# Update node's child after inserting a new node.
def updateChildLink(newRank, id):
    global childRank
    MPI.COMM_WORLD.send(id, dest=childRank, tag=UPDATE_PARENT)
    childRank = newRank

# Insert a new node into the DHT.
# If node is the correct parent, it re-links child pointers,
# Transfers appropriate data, and notifies the head.
# Otherwise forwards request to its child.
def add(source):
    params = MPI.COMM_WORLD.recv(source=source, tag=ADD)
    newRank, id = params
    if active and (rank == 0 or parentId is not None):
        if storageId < id < getStorageId(childRank):
            sendInfoToNode(newRank, id)
            updateChildLink(newRank, id)
            sendMovedKeys(newRank, id)
            MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
        else:
            MPI.COMM_WORLD.send(params, dest=childRank, tag=ADD)
    else:
        MPI.COMM_WORLD.send(params, dest=childRank, tag=ADD)

# Manages removal of a node.
# If node is the parent of the node being removed, it coordinates removal by
# Requesting data from the child, updating child links, and notifying the head.
# Otherwise forward REMOVE to the next node.
def remove(source):
    global childRank
    id = MPI.COMM_WORLD.recv(source=source, tag=REMOVE)
    if active and getStorageId(childRank) == id:
        MPI.COMM_WORLD.send(rank, dest=childRank, tag=REMOVE_NODE)
        childRank = MPI.COMM_WORLD.recv(source=childRank, tag=NEW_CHILD_RANK)
        MPI.COMM_WORLD.send(storageId, dest=childRank, tag=UPDATE_PARENT)
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
    else:
        MPI.COMM_WORLD.send(id, dest=childRank, tag=REMOVE)

# Request a node's storage ID.
# Used for insertion or deletion to determine routing decisions.
def getStorageId(rank):
    MPI.COMM_WORLD.send(None, dest=rank, tag=STORAGE_ID)
    return MPI.COMM_WORLD.recv(source=rank, tag=STORAGE_ID)

# Store a key-value pair.
# Pair is either stored locally or forwarded to its child.
def put(source):
    keyVal = MPI.COMM_WORLD.recv(source=source, tag=PUT)
    key, value = keyVal
    if parentId is not None and key > parentId and key <= storageId:
        data[key] = value
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
    else:
        MPI.COMM_WORLD.send(keyVal, dest=childRank, tag=PUT)

# Process GET signal from another node and return value to head.
# otherwise forward GET request to the child. 
def getKeyVal(source):
    key = MPI.COMM_WORLD.recv(source=source, tag=GET)
    if parentId is not None and key > parentId and key <= storageId:
        value = data[key]
        args = (value, storageId)
        MPI.COMM_WORLD.send(args, dest=0, tag=RETVAL)
    else:
        MPI.COMM_WORLD.send(key, dest=childRank, tag=GET)

# Handles GET command.
# Send to getKeyVal.
def get(source):
    getKeyVal(source)

# Passes ACK signals back to the command node through the head node.
def ack():
    if rank == 0:
        dummy = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=ACK)
        MPI.COMM_WORLD.send(dummy, dest=numProcesses - 1, tag=ACK)

# Forwards result of a GET operation back to the command node.
def retval():
    if rank == 0:
        retval = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=RETVAL)
        MPI.COMM_WORLD.send(retval, dest=numProcesses - 1, tag=RETVAL)

# Main loop for each process node in the DHT.
def handleMessages():
    global parentId, storageId, childRank, active, data
    status = MPI.Status()
    while True:
        MPI.COMM_WORLD.probe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()
        if tag == END:
            end()
        elif tag == ADD:
            add(source)
        elif tag == REMOVE:
            remove(source)
        elif tag == PUT:
            put(source)
        elif tag == GET:
            get(source)
        elif tag == ACK:
            ack()
        elif tag == RETVAL:
            retval()
        elif tag == STORAGE_ID:
            processStorageId(source)
        elif tag == INIT_NODE:
            initializeNode(source)
        elif tag == UPDATE_PARENT:
            updateParentId(source)
        elif tag == MOVE_KEYS:
            moveKeys(source)
        elif tag == REMOVE_NODE: 
            removeNode(source)
        else:
            print(f"ERROR, my id is {rank}, source is {source}, tag is {tag}")
            sys.exit(1)

# main
if __name__ == "__main__":
    
    # get my rank and the total number of processes
    numProcesses = MPI.COMM_WORLD.Get_size()
    rank = MPI.COMM_WORLD.Get_rank()

    # set up the head node and the last storage node
    if rank == 0:
        storageId = 0
        childRank = numProcesses - 2
        active = True

    elif rank == numProcesses - 2:
        storageId = MAX
        childRank = 0
        active = True
        parentId = 0

    # the command node is handled separately
    if rank < numProcesses - 1:
        handleMessages()
    else:
        commandNode()