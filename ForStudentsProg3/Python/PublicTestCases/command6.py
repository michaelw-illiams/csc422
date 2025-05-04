import sys
from mpi4py import MPI #mpi4py library
from dht_globals import * #global variables

def commandNode(): 
    dummy = 0
    
    addArgs = [3, 5]
    MPI.COMM_WORLD.send(addArgs, dest=0, tag=ADD)
    MPI.COMM_WORLD.recv(source=0, tag=ACK)
    
    keyval = [9, 100]
    MPI.COMM_WORLD.send(keyval, dest=0, tag=PUT)
    MPI.COMM_WORLD.recv(source=0, tag=ACK)

    keyval = [19, 200]
    MPI.COMM_WORLD.send(keyval, dest=0, tag=PUT)
    MPI.COMM_WORLD.recv(source=0, tag=ACK)
    
    key = 19
    MPI.COMM_WORLD.send(key, dest=0, tag=GET)
    answer = MPI.COMM_WORLD.recv(source=0, tag=RETVAL)
    print(f"val is {answer[0]}, storage id is {answer[1]}")

    MPI.COMM_WORLD.send(dummy, dest=0, tag=END)
    print("command finalizing")
    exit(0)
