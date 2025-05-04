import sys
from mpi4py import MPI #mpi4py library
from dht_globals import * #global variables

def commandNode(): 
    dummy = 0

    MPI.COMM_WORLD.send(dummy, dest=0, tag=END)
    print("command finalizing")
    exit(0)
