import sys
from mpi4py import MPI #mpi4py library
from dht_globals import * #global variables
from command import commandNode #command node code
global active, myStorageId, parentRank, childRank, myData, storageIds
myData = {}  # key-value store for this node
active = False
parentRank = None
childRank = None
myStorageId = -1
storageIds = {}  # mapping of rank to ID (for coordination)

def getStorageId(rank):
    if rank == 0:
        return 0
    elif rank == numProcesses - 2:
        return MAX
    return storageIds.get(rank, MAX + 1)  # default to out of bounds

def getChildOf(rank):
    if rank == numProcesses - 2:
        return 0
    return storageIds.get(rank + 1, 0)


# on an END message, the head node is to contact all storage nodes and tell them
def headEnd():
    # tell all the storage nodes to END
    # the data sent is unimportant here, so just send a dummy value
    dummy = MPI.COMM_WORLD.recv(source=numProcesses-1, tag=END)
    for i in range(1,numProcesses-1):
        MPI.COMM_WORLD.send(dummy, dest=i, tag=END)
    MPI.Finalize()
    sys.exit(0)
    
# on an END message, a storage node just calls MPI_Finalize and exits
def storageEnd(): 
    # the data is unimportant for an END
    dummy = MPI.COMM_WORLD.recv(source=0, tag=END)
    MPI.Finalize()
    sys.exit(0)

def getKeyVal(source):
    #receive the GET message
    #note that at this point, we've only called MPI_Probe, which only peeks at the message
    #we are receiving the key from whoever sent us the message 
    key = MPI.COMM_WORLD.recv(source=source, tag=GET)
    print(f"[Rank {myRank}] Received GET {key}")

    if key <= myStorageId:
        value = myData.get(key, -1)
        print(f"[Rank {myRank}] Returning value {value} for key {key}")
        argsAdd = (value, myStorageId)
        MPI.COMM_WORLD.send(argsAdd, dest=0, tag=RETVAL)
    else:
        print(f"[Rank {myRank}] Forwarding GET {key} to child {childRank}")
        MPI.COMM_WORLD.send(key, dest=childRank, tag=GET)

def addCode(source):
    global active, myStorageId, parentRank, childRank, myData, storageIds
    if not active:
        # This node is the one being added
        parent, child, newId = MPI.COMM_WORLD.recv(source=source, tag=ADD)
        print(f"[Rank {myRank}] Being added as node {newId}")

        # Activate
        active = True
        myStorageId = newId
        parentRank = parent
        childRank = child

        # Receive key/value pairs
        myData.clear()
        newData = MPI.COMM_WORLD.recv(source=source, tag=PUT)
        myData.update(newData)

        # Send ACK back to parent
        MPI.COMM_WORLD.send(0, dest=parent, tag=ACK)

    else:
        # This is an active node forwarding the ADD
        rankToAdd, idToAdd = MPI.COMM_WORLD.recv(source=source, tag=ADD)
        print(f"[Rank {myRank}] ADD request: rank {rankToAdd}, id {idToAdd}")

        # Determine if this node is the correct parent
        if myStorageId < idToAdd and (childRank == 0 or idToAdd <= getStorageId(childRank)):
            print(f"[Rank {myRank}] Adding new node {rankToAdd} with ID {idToAdd}")
            oldChild = childRank
            childRank = rankToAdd

            # Notify new node
            MPI.COMM_WORLD.send((myRank, oldChild, idToAdd), dest=rankToAdd, tag=ADD)

            # Redistribute data
            keysToMove = {k: v for k, v in myData.items() if k > myStorageId and k <= idToAdd}
            for k in keysToMove:
                del myData[k]
            MPI.COMM_WORLD.send(keysToMove, dest=rankToAdd, tag=PUT)

            # Register the new ID (for helper use)
            storageIds[rankToAdd] = idToAdd

            # Wait for ACK from new node
            MPI.COMM_WORLD.recv(source=rankToAdd, tag=ACK)
            MPI.COMM_WORLD.send(0, dest=0, tag=ACK)

        else:
            # Forward to next active node
            MPI.COMM_WORLD.send((rankToAdd, idToAdd), dest=childRank, tag=ADD)

def removeCode(source):
    global active, myStorageId, parentRank, childRank, myData, storageIds

    idToRemove = MPI.COMM_WORLD.recv(source=source, tag=REMOVE)
    print(f"[Rank {myRank}] REMOVE request for ID {idToRemove}")

    # Check if my child has that ID (i.e., I'm the parent)
    if active and getStorageId(childRank) == idToRemove:
        print(f"[Rank {myRank}] Removing child {childRank} with ID {idToRemove}")

        MPI.COMM_WORLD.send(0, dest=childRank, tag=PUT)
        receivedData = MPI.COMM_WORLD.recv(source=childRank, tag=PUT)
        myData.update(receivedData)

        oldChild = childRank
        newChild = getChildOf(childRank)
        print(f"[Rank {myRank}] Updating child to {newChild}")
        childRank = newChild
        # Prevent circular self-linking
        if childRank == myRank:

            childRank = numProcesses - 2  # fallback to permanent node


        del storageIds[oldChild]  # ✅ Fix here

        print(f"[Rank {myRank}] REMOVE complete, sending ACK to head")
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
        return

    elif active and myStorageId == idToRemove:
        # I am being removed
        print(f"[Rank {myRank}] Preparing to send data and deactivate")

        # Send all my data to my parent
        MPI.COMM_WORLD.send(myData, dest=parentRank, tag=PUT)
        myData.clear()

        active = False
        myStorageId = -1
        parentRank = None
        childRank = None

        # Send ACK to parent
        MPI.COMM_WORLD.send(0, dest=parentRank, tag=ACK)

    else:
        # Forward REMOVE
        MPI.COMM_WORLD.send(idToRemove, dest=childRank, tag=REMOVE)


def handleMessages():
    global active, myStorageId, parentRank, childRank, myData, storageIds

    status = MPI.Status() # get a status object
    
    while True:
        # Peek at the message
        MPI.COMM_WORLD.probe( source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
              
        # get the source and the tag---which MPI rank sent the message, and
        # what the tag of that message was (the tag is the command)
        source = status.Get_source()
        tag = status.Get_tag()

        # now take the appropriate action
        # code for END and most of GET is given; others require your code
    
        # python 3.10 adds switch statements, but lectura has python 3.8
        if tag == END:
            if myRank == 0:
                headEnd()
            else:
                storageEnd()
            pass
        elif tag == ADD:
            addCode(source)
        elif tag == REMOVE:
            removeCode(source)
        elif tag == PUT:
            if active:
                msg = MPI.COMM_WORLD.recv(source=source, tag=PUT)

                # Detect special REMOVE-triggered message
                if isinstance(msg, int):
                    print(f"[Rank {myRank}] Received REMOVE shutdown request (special PUT)")
                    # This is the parent asking for data so we can be removed
                    MPI.COMM_WORLD.send(myData, dest=parentRank, tag=PUT)
                    myData.clear()

                    print(f"[Rank {myRank}] Deactivating myself")
                    active = False
                    myStorageId = -1
                    parentRank = None
                    childRank = None
                    return

                # Otherwise: normal PUT
                key, value = msg
                print(f"[Rank {myRank}] Storing key {key} = {value}")
                if key <= myStorageId:
                    myData[key] = value
                    MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
                else:
                    MPI.COMM_WORLD.send((key, value), dest=childRank, tag=PUT)

        elif tag == GET:
            getKeyVal(source)
        elif tag == ACK and myRank == 0:
            dummy = MPI.COMM_WORLD.recv(source=source, tag=ACK)
            MPI.COMM_WORLD.send(dummy, dest=numProcesses - 1, tag=ACK)
        elif tag == RETVAL and myRank == 0:
            result = MPI.COMM_WORLD.recv(source=source, tag=RETVAL)
            print(f"[Head] Forwarding RETVAL {result} to command node")
            MPI.COMM_WORLD.send(result, dest=numProcesses - 1, tag=RETVAL)
        # NOTE: you probably will want to add more cases here, e.g., to handle data redistribution
        else:
            # should never be reached---if it is, something is wrong with your code; just bail out
            print(f"ERROR, my id is {myRank}, source is{source}, tag is {tag}")
            sys.exit(1)

if __name__ == "__main__":
    # get my rank and the total number of processes
    numProcesses = MPI.COMM_WORLD.Get_size()
    myRank = MPI.COMM_WORLD.Get_rank()
    active = myRank == 0 or myRank == numProcesses - 2


    # set up the head node and the last storage node
    if myRank == 0:
        active = True
        myStorageId = 0
        parentRank = numProcesses - 2
        childRank = numProcesses - 2
    elif myRank == numProcesses - 2:
        active = True
        myStorageId = MAX
        parentRank = 0
        childRank = 0

    print(f"myRank = {myRank}")
    # the command node is handled separately
    if myRank < numProcesses-1:
        handleMessages()
    else:
        commandNode()