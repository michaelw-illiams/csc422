#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include "dht.h"

void commandNode() {
  int dummy;
  int keyval[2];
  int answer[2];
  int key;

  keyval[0] = 9;
  keyval[1] = 100;
  MPI_Send(keyval, 2, MPI_INT, 0, PUT, MPI_COMM_WORLD);
  MPI_Recv(&dummy, 1, MPI_INT, 0, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  keyval[0] = 19;
  keyval[1] = 200;
  MPI_Send(keyval, 2, MPI_INT, 0, PUT, MPI_COMM_WORLD);
  MPI_Recv(&dummy, 1, MPI_INT, 0, ACK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  key = 19;
  MPI_Send(&key, 1, MPI_INT, 0, GET, MPI_COMM_WORLD);
  MPI_Recv(answer, 2, MPI_INT, 0, RETVAL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  printf("val is %d, storage id is %d\n", answer[0], answer[1]);

  MPI_Send(&dummy, 1, MPI_INT, 0, END, MPI_COMM_WORLD);
  printf("command finalizing\n");
  MPI_Finalize();
  exit(0);
}

