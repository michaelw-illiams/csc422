import sys
from mpi4py import MPI #mpi4py library
from dht_globals import * #global variables

# def commandNode(): 
    
#     dummy = 0
#     MPI.COMM_WORLD.send(dummy, dest=0, tag=END)
#     print("command finalizing")
#     exit(0)


def commandNode():
    comm = MPI.COMM_WORLD
    head = 0

    # PUT key 999 with value 12345
    comm.send((999, 12345), dest=head, tag=PUT)
    comm.recv(source=head, tag=ACK)

    # GET key 999
    comm.send(999, dest=head, tag=GET)
    result = comm.recv(source=head, tag=RETVAL)
    value, storage_id = result
    print(f"{value} {storage_id}")  # Expect: 12345 1000

    # END program
    comm.send(0, dest=head, tag=END)
    print("command finalizing")
    exit(0)

