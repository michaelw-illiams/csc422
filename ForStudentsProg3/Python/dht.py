import sys
from mpi4py import MPI
from dht_globals import *
from command import commandNode

# Global state
data = {}
active = False
parentId = None
childRank = None
storageId = None
numProcesses = None
rank = None

def headEnd():
    dummy = MPI.COMM_WORLD.recv(source=numProcesses-1, tag=END)
    for i in range(1, numProcesses-1):
        MPI.COMM_WORLD.send(dummy, dest=i, tag=END)
    MPI.Finalize()
    sys.exit(0)

def storageEnd():
    dummy = MPI.COMM_WORLD.recv(source=0, tag=END)
    MPI.Finalize()
    sys.exit(0)

def getKeyVal(source):
    key = MPI.COMM_WORLD.recv(source=source, tag=GET)
    if parentId is not None and parentId < key <= storageId:
        value = data.get(key, -1)
        MPI.COMM_WORLD.send((value, storageId), dest=0, tag=RETVAL)
    else:
        MPI.COMM_WORLD.send(key, dest=childRank, tag=GET)

def getStorageId(remoteRank):
    MPI.COMM_WORLD.send(None, dest=remoteRank, tag=GET_STORAGE_ID)
    return MPI.COMM_WORLD.recv(source=remoteRank, tag=GET_STORAGE_ID)

def sendInfoToNewNode(newRank, newId):
    info = (rank, childRank, storageId, newId)
    MPI.COMM_WORLD.send(info, dest=newRank, tag=INIT_NODE)

def updateLinksForAdd(newRank, newId):
    global childRank
    MPI.COMM_WORLD.send(newId, dest=childRank, tag=UPDATE_PARENT)
    childRank = newRank

def sendMigratedKeys(newRank, newId):
    toSend = {k: v for k, v in data.items() if parentId < k <= newId}
    for k in toSend:
        del data[k]
    MPI.COMM_WORLD.send(toSend, dest=newRank, tag=REDIST)

def end():
    if rank == 0:
        headEnd()
    else:
        storageEnd()

def add(source):
    global childRank
    addParams = MPI.COMM_WORLD.recv(source=source, tag=ADD)
    newRank, newId = addParams
    if active and (rank == 0 or parentId is not None):
        if storageId < newId < getStorageId(childRank):
            sendInfoToNewNode(newRank, newId)
            updateLinksForAdd(newRank, newId)
            sendMigratedKeys(newRank, newId)
            MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
        else:
            MPI.COMM_WORLD.send(addParams, dest=childRank, tag=ADD)
    else:
        MPI.COMM_WORLD.send(addParams, dest=childRank, tag=ADD)

def remove(source):
    global childRank
    removeId = MPI.COMM_WORLD.recv(source=source, tag=REMOVE)
    if active and getStorageId(childRank) == removeId:
        MPI.COMM_WORLD.send(rank, dest=childRank, tag=REMOVE_NODE)
        childRank = MPI.COMM_WORLD.recv(source=childRank, tag=NEW_CHILD_RANK)
        MPI.COMM_WORLD.send(storageId, dest=childRank, tag=UPDATE_PARENT)
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
    else:
        MPI.COMM_WORLD.send(removeId, dest=childRank, tag=REMOVE)

def put(source):
    keyval = MPI.COMM_WORLD.recv(source=source, tag=PUT)
    key, value = keyval
    if parentId is not None and parentId < key <= storageId:
        data[key] = value
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
    else:
        MPI.COMM_WORLD.send(keyval, dest=childRank, tag=PUT)

def ack():
    if rank == 0:
        dummy = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=ACK)
        MPI.COMM_WORLD.send(dummy, dest=numProcesses - 1, tag=ACK)

def retval():
    if rank == 0:
        result = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=RETVAL)
        MPI.COMM_WORLD.send(result, dest=numProcesses - 1, tag=RETVAL)

def processStorageId(source):
    _ = MPI.COMM_WORLD.recv(source=source, tag=GET_STORAGE_ID)
    response = storageId if active else -1
    MPI.COMM_WORLD.send(response, dest=source, tag=GET_STORAGE_ID)

def initializeNode(source):
    global storageId, parentId, childRank, active
    parentRank, newChildRank, parentStorageId, newId = MPI.COMM_WORLD.recv(source=source, tag=INIT_NODE)
    storageId = newId
    parentId = parentStorageId
    childRank = newChildRank
    active = True

def updateParentId(source):
    global parentId
    parentId = MPI.COMM_WORLD.recv(source=source, tag=UPDATE_PARENT)

def moveKeys(source):
    movedData = MPI.COMM_WORLD.recv(source=source, tag=REDIST)
    data.update(movedData)

def removeNode(source):
    global active, storageId, parentId, childRank
    parentRank = MPI.COMM_WORLD.recv(source=source, tag=REMOVE_NODE)
    for k, v in data.items():
        MPI.COMM_WORLD.send((k, v), dest=parentRank, tag=PUT)
    data.clear()
    MPI.COMM_WORLD.send(childRank, dest=parentRank, tag=NEW_CHILD_RANK)
    active = False
    storageId = None
    parentId = None
    childRank = None

def handleMessages():
    status = MPI.Status()
    while True:
        MPI.COMM_WORLD.probe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()

        if tag == END: end()
        elif tag == ADD: add(source)
        elif tag == REMOVE: remove(source)
        elif tag == PUT: put(source)
        elif tag == GET: getKeyVal(source)
        elif tag == ACK: ack()
        elif tag == RETVAL: retval()
        elif tag == GET_STORAGE_ID: processStorageId(source)
        elif tag == INIT_NODE: initializeNode(source)
        elif tag == UPDATE_PARENT: updateParentId(source)
        elif tag == REDIST: moveKeys(source)
        elif tag == REMOVE_NODE: removeNode(source)
        else:
            print(f"[Rank {rank}] ERROR: Unexpected tag {tag}")
            sys.exit(1)

if __name__ == "__main__":
    numProcesses = MPI.COMM_WORLD.Get_size()
    rank = MPI.COMM_WORLD.Get_rank()

    if rank == 0:
        storageId = 0
        childRank = numProcesses - 2
        active = True

    elif rank == numProcesses - 2:
        storageId = MAX
        parentId = 0
        childRank = 0
        active = True

    if rank < numProcesses - 1:
        handleMessages()
    else:
        commandNode()
