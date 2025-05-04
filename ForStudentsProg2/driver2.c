#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include "myMalloc.h"

int sumArray(int *array, int size, int target) {
  int sum = 0;
  for (int i = 0; i < size; i++) {
    sum += array[i];
  }

  if (target != sum) {
    printf("%d\n", sum);
    printf("FAILURE\n");
    exit(1);
  }

  return 0;
}

void *worker(void *val) {
  int j, k;

  int toIter = *((int *) val);
  for (int i = 0; i < toIter; i++) {
    int *array = (int*) myMalloc(sizeof(int) * 10);
    for (k=0, j=1; k < 10; k++, j++) {
      array[k] = j;
    }
    sumArray(array, 10, 55);

    myFree(array);
  }
}

int seqWorker(int toIter) {
  int j, k;
  for (int i = 0; i < toIter; i++) {
    int *array = (int*) myMalloc(sizeof(int) * 10);
    for (k=0, j=1; k < 10; k++, j++) {
      array[k] = j;
    }
    sumArray(array, 10, 55);

    myFree(array);
  }
  return 0;
}

int main() {
    double startTime, endTime;
    struct timeval start, end;

    int i, *params;
    pthread_t *pid_list;

    int p;
    params = malloc(sizeof(int));  

    for (p = 2; p < 9; p++) {
      params[0] = 2520000 / p;
      gettimeofday(&start, NULL);
      startTime = start.tv_sec + start.tv_usec / 1000000.0;
      myInit(1,1);
      if (seqWorker(2520000) == 0) {
          printf("Sequential works\n");
      }
      gettimeofday(&end, NULL);
      endTime = end.tv_sec + end.tv_usec / 1000000.0;
      printf("Sequential Time: %f\n", endTime - startTime);

      gettimeofday(&start, NULL);
      startTime = start.tv_sec + start.tv_usec / 1000000.0;
      myInit(p,1);

      pid_list = (pthread_t *)malloc(p*sizeof(pthread_t));
      for(i=0;i<p;i++){
        pthread_create(&pid_list[i],NULL,worker,(void *)(&params[0]));
      }
      for(i=0;i<p;i++){
        pthread_join(pid_list[i],NULL);
      }
      gettimeofday(&end, NULL);
      endTime = end.tv_sec + end.tv_usec / 1000000.0;
      printf("Parallel Time (COARSE w/ P = %d): %f\n", p, endTime - startTime);


      gettimeofday(&start, NULL);
      startTime = start.tv_sec + start.tv_usec / 1000000.0;
      myInit(p,2);

      pid_list = (pthread_t *)malloc(p*sizeof(pthread_t));
      for(i=0;i<p;i++){
        pthread_create(&pid_list[i],NULL,worker,(void *)(&params[0]));
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