// sample driver for CSc 422, Program 2
// uses only one thread

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "myMalloc.h"

#define NUMS_64 10

int pass = 0;

void test() {
  int i, j, k;
  int *array[NUMS_64];

  for (j = 0; j < 8; j++) {
    for(i = 0; i < NUMS_64; i++) {
      array[i] = (int *) myMalloc (sizeof(int) * j);
      if(array[i]==NULL){
        printf("myMalloc() failed\n");
        return;
      }
      for (k = 0; k < j; k++) {
        array[i][k] = i + k;
      }
    }

    for (i = 0; i < NUMS_64;i++) {
      for (k = 0; k < j; k++) {
        if (array[i][k] != i + k) {
          printf("Invalid value\n");
          return;
        }
      }
      myFree(array[i]);
    }
  }

  pass = 1;
  return;
}

int main(int argc, char *argv[]){

  if (myInit(1, 1) == -1){
    printf("myInit() failed.\n");
    return 1;
  }

  test();
  
  if (pass != 1) {
    printf("sample test failed\n");
    return 1;
  }

  printf("sample test passed\n");
  return 0;
}
