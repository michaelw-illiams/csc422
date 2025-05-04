#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "dht.h"
#include "dht-helper.h"

static int myStorageId, childRank, myRank, numProcesses;

// on an END message, the head node is to contact all storage nodes and tell them
void headEnd() {
  int i, dummy;

  // the head node knows there is an END message waiting to be received because
  // we just called MPI_Probe to peek at the message.  Now we just receive it.
  MPI_Recv(&dummy, 1, MPI_INT, numProcesses-1, END, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  // tell all the storage nodes to END
  // the data sent is unimportant here, so just send a dummy value
  for (i = 1; i < numProcesses-1; i++) {
    MPI_Send(&dummy, 1, MPI_INT, i, END, MPI_COMM_WORLD);
  }
  MPI_Finalize();
  exit(0);
}

// on an END message, a storage node just calls MPI_Finalize and exits
void storageEnd() {
  int dummy;  // the data is unimportant for an END

  MPI_Recv(&dummy, 1, MPI_INT, 0, END, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  MPI_Finalize();
  exit(0);
}

void getKeyVal(int source) {
  int *argsAdd;
  int key, value;

  // receive the GET message
  // note that at this point, we've only called MPI_Probe, which only peeks at the message
  // we are receiving the key from whoever sent us the message 
  MPI_Recv(&key, 1, MPI_INT, source, GET, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  if (key <= myStorageId) {  // I own this key/value pair
    // allocate two integers: the first will be the value, the second will be this storage id
    argsAdd = (int *) malloc (2 * sizeof(int));

    // find the associated value (called "value") using whatever data structure you use
    // you must add this code to find it (omitted here)

    argsAdd[0] = value;
    argsAdd[1] = myStorageId;
    // send this value around the ring
    MPI_Send(argsAdd, 2, MPI_INT, childRank, RETVAL, MPI_COMM_WORLD);
  }
  else {  // I do NOT own this key/value pair; just forward request to next hop
    MPI_Send(&key, 1, MPI_INT, childRank, GET, MPI_COMM_WORLD);
  }
}

// handleMessages repeatedly gets messages and performs the appropriate action
void handleMessages() {
  MPI_Status status;
  int count, source, tag;

  while (1) {
    // peek at the message
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    // skeleton doesn't use MPI_Get_count, but you will need to---
    // this call just gets how many integers are in the message
    MPI_Get_count(&status, MPI_INT, &count);
    // get the source and the tag---which MPI rank sent the message, and
    // what the tag of that message was (the tag is the command)
    source = status.MPI_SOURCE;
    tag = status.MPI_TAG;

    // now take the appropriate action
    // code for END and most of GET is given; others require your code
    switch(tag) {
      case END:
        if (myRank == 0) {
          headEnd();
        }
        else {
          storageEnd();
        }
        break;
      case ADD:
        break;
      case REMOVE:
        break;
      case PUT:
        break;
      case GET:
        getKeyVal(source);
        break;
      case ACK:
        break;
      case RETVAL:
        break;
      // NOTE: you probably will want to add more cases here, e.g., to handle data redistribution
      default:
        // should never be reached---if it is, something is wrong with your code; just bail out
        printf("ERROR, my id is %d, source is %d, tag is %d, count is %d\n", myRank, source, tag, count);
        exit(1);
    } 
  }
}

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);

  // get my rank and the total number of processes
  MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);

  // set up the head node and the last storage node
  if (myRank == 0) {
    myStorageId = 0;
    childRank = numProcesses-2;
  }
  else if (myRank == numProcesses-2) {
    myStorageId = MAX;
    childRank = 0;
  }

  // the command node is handled separately
  if (myRank < numProcesses-1) {
    handleMessages();
  }
  else {
    commandNode(); 
  }
  return 0;
}
