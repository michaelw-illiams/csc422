#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include "myMalloc.h" 


int spin = 1;
void *worker(void *val) {
  int j, k;

  int id = *((int*) val);
  // if (id != 1 ) {
  //   while (spin) {
  //     int *array = (int*) myMalloc(sizeof(int) * 10);
  //     myFree(array);
  //   }
  // }
  // else {
    int* buffer[3144+393];
    for (k = 0; k < 3144+393; k++) {
      buffer[k] = (int*) myMalloc(sizeof(int));
    }
    for (k = 0; k < 3144+393; k++) {
      myFree(buffer[k]);
    }
  // }
   // spin = 0;
}

int main() {
    double startTime, endTime;
    struct timeval start, end;

    int i;
    pthread_t *pid_list;

    int p;
    int params[8] = {1,2,3,4,5,6,7,8};

    for (p = 2; p < 9; p++) {
      spin = 1;
      gettimeofday(&start, NULL);
      startTime = start.tv_sec + start.tv_usec / 1000000.0;
      myInit(p,2);

      pid_list = (pthread_t *)malloc(p*sizeof(pthread_t));
      for(i=0;i<p;i++){
        pthread_create(&pid_list[i],NULL,worker,(void *)(&params[i]));
      }
      for(i=0;i<p;i++){
        pthread_join(pid_list[i],NULL);
      }
      gettimeofday(&end, NULL);
      endTime = end.tv_sec + end.tv_usec / 1000000.0;
      printf("Parallel Time (FINE w/ P = %d): %f\n", p, endTime - startTime);

      printf("-----------------------------\n");
    }
}