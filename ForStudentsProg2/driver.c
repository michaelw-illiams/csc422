/**
 * tests fine grain mode MyMalloc.
 *  The main difference in this test is it'll test your overflow implementation as well, by overfilling the lists
 */

 #include <pthread.h>
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 
 #include "myMalloc.h"
 
 #define NUM_CHUNKS_64 10
 #define NUM_CHUNKS_1024 10
 
 #define MAX_NUM_CHUNKS_64 1572
 #define MAX_NUM_CHUNKS_1024 132
 #define OVERFLOW_AMOUNT 10  // don't make this too large otherwise you'll overflow the overflow list as well
 
 #define NUM_THREADS 8  // how many threads to use? max expected is 8 threads
 
 #define NUM_ITERATIONS 100  // how many times should it go through the process of allocating/deallocating memory?
 
 // this just makes sure the large chunk vals are unique from the small chunk vals. not that this should matter.
 #define VAL_OFFSET 2000
 
 volatile int g_passed = 1;
 
 #define NUM_NUMS 10  // lol
 typedef struct LargerThan64Bytes {
     long nums[NUM_NUMS];
 } LargerThan64Bytes;
 
 int test_small() {
     int i, t;
     long *array[NUM_CHUNKS_64];  // array of int pointers
     for (t = 0; t < NUM_ITERATIONS; t++) {
         // allocate small
         for (int i = 0; i < NUM_CHUNKS_64; i++) {
             array[i] = myMalloc(sizeof(long));
             if (array[i] == NULL) {
                 printf("myMalloc() failed\n");
                 return 1;
             }
             *array[i] = i + t;
         }
 
         // free small
         for (int i = 0; i < NUM_CHUNKS_64; i++) {
             int expectedNum = i + t;
             if (*array[i] != expectedNum) {
                 printf("Invalid large value. expected: %d. actual: %ld\n", expectedNum, *array[i]);
                 return 1;
             }
             myFree(array[i]);
         }
     }
     return 0;
 }
 
 int test_small_large() {
     long *array[NUM_CHUNKS_64];
     LargerThan64Bytes *largeArray[NUM_CHUNKS_1024];
 
     for (int t = 0; t < NUM_ITERATIONS; t++) {
         // allocate small
         for (int i = 0; i < NUM_CHUNKS_64; i++) {
             array[i] = myMalloc(sizeof(long));
             if (array[i] == NULL) {
                 printf("myMalloc() failed\n");
                 return 1;
             }
             *array[i] = i + t;
         }
 
         // allocate large
         for (int i = 0; i < NUM_CHUNKS_1024; i++) {
             largeArray[i] = myMalloc(sizeof(LargerThan64Bytes));
             if (array[i] == NULL) {
                 printf("myMalloc() failed\n");
                 return 1;
             }
             for (int k = 0; k < NUM_NUMS; k++) {
                 largeArray[i]->nums[k] = i + t + k + VAL_OFFSET;
             }
         }
 
         // free small
         for (int i = 0; i < NUM_CHUNKS_64; i++) {
             int expectedNum = i + t;
             if (*array[i] != expectedNum) {
                 printf("Invalid large value. expected: %d. actual: %ld\n", expectedNum, *array[i]);
                 return 1;
             }
             myFree(array[i]);
         }
 
         // free large
         for (int i = 0; i < NUM_CHUNKS_1024; i++) {
             long *nums = largeArray[i]->nums;
             for (int k = 0; k < NUM_NUMS; k++) {
                 int expectedNum = i + t + k + VAL_OFFSET;
                 if (nums[k] != expectedNum) {
                     printf("Invalid large value. expected: %d. actual: %ld", expectedNum, nums[k]);
                     return 1;
                 }
             }
             myFree(largeArray[i]);
         }
     }
     return 0;
 }
 
 int test_small_large_overflow() {
     // we'll be allocating more chunks than available in the list
     int *array[MAX_NUM_CHUNKS_64 + OVERFLOW_AMOUNT];
     LargerThan64Bytes *largeArray[MAX_NUM_CHUNKS_1024 + OVERFLOW_AMOUNT];
 
     for (int t = 0; t < NUM_ITERATIONS + OVERFLOW_AMOUNT; t++) {
         // allocate small
         for (int i = 0; i < MAX_NUM_CHUNKS_64 + OVERFLOW_AMOUNT; i++) {
             array[i] = myMalloc(sizeof(int));
             if (array[i] == NULL) {
                 printf("myMalloc() failed\n");
                 return 1;
             }
             *array[i] = i + t;
         }
 
         // allocate large
         for (int i = 0; i < MAX_NUM_CHUNKS_1024 + OVERFLOW_AMOUNT; i++) {
             largeArray[i] = myMalloc(sizeof(LargerThan64Bytes));
             if (largeArray[i] == NULL) {
                 printf("myMalloc() failed\n");
                 return 1;
             }
             for (int k = 0; k < NUM_NUMS; k++) {
                 largeArray[i]->nums[k] = VAL_OFFSET + i + t + k;
             }
         }
 
         // free small
         for (int i = 0; i < MAX_NUM_CHUNKS_64 + OVERFLOW_AMOUNT; i++) {
             int expectedNum = i + t;
             if (*array[i] != expectedNum) {
                 printf("Invalid large value. expected: %d. actual: %d\n", expectedNum, *array[i]);
                 return 1;
             }
             myFree(array[i]);
         }
 
         // free large
         for (int i = 0; i < MAX_NUM_CHUNKS_1024 + OVERFLOW_AMOUNT; i++) {
             for (int k = 0; k < NUM_NUMS; k++) {
                 int expectedNum = i + t + k + VAL_OFFSET;
                 if (largeArray[i]->nums[k] != expectedNum) {
                     printf("Invalid large value. expected: %d. actual: %ld", expectedNum, largeArray[i]->nums[k]);
                     printf("t = %d, i = %d, k = %d\n", t, i, k);
                     return 1;
                 }
             }
             myFree(largeArray[i]);
         }
     }
     return 0;
 }
 
 void *worker(void *temp) {
     int my_id = *((int *)temp);
 
     if (test_small() == 1) {
         printf("test_small FAILED in thread %d\n", my_id);
         g_passed = 0;
     } else {
         printf("test_small PASSED in thread %d\n", my_id);
     }
 
     if (test_small_large() == 1) {
         printf("test_small_large FAILED in thread %d\n", my_id);
         g_passed = 0;
     } else {
         printf("test_small_large PASSED in thread %d\n", my_id);
     }
 
     if (test_small_large_overflow() == 1) {
         printf("test_small_large_overflow FAILED in thread %d\n", my_id);
         g_passed = 0;
     } else {
         printf("test_small_large_overflow PASSED in thread %d\n", my_id);
     }
 
     return NULL;
 }
 
 void parallel(int p) {
     int i, *params;
     pthread_t *pid_list;
 
     params = (int *)malloc(p * sizeof(int));
     for (i = 0; i < p; i++) params[i] = i;
 
     pid_list = (pthread_t *)malloc(p * sizeof(pthread_t));
     for (i = 0; i < p; i++) {
         pthread_create(&pid_list[i], NULL, worker, (void *)(&params[i]));
     }
     for (i = 0; i < p; i++) {
         pthread_join(pid_list[i], NULL);
     }
 
     free(params);
     free(pid_list);
     return;
 }
 
 int main(int argc, char *argv[]) {
     if (myInit(NUM_THREADS, 2) == -1) {
         printf("myInit() failed.\n");
         return 1;
     }
     parallel(NUM_THREADS);
 
     if (g_passed) {
         printf("Passed testFine! :D\n");
     } else {
         printf("Failed testFine! D:\n");
     }
     return 0;
 }
 