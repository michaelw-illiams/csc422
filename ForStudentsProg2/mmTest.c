#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include "myMalloc-helper.h"

int myInit(int numCores, int flag);
void *myMalloc(int size);
void myFree(void *ptr);

int verifyPattern(unsigned char *ptr, int size, unsigned char pattern) {
    for (int i = 0; i < size; i++) {
        if (ptr[i] != pattern) {
            return 0;
        }
    }
    return 1;
}

void testMyMalloc() {
    printf("Initializing memory manager...\n");
    if (myInit(1, 0) != 0) {
        printf("Memory initialization failed.\n");
        return;
    }
    printf("Initialization successful.\n");

    // --- Small Block Test ---
    printf("\nAllocating and testing small blocks...\n");
    void *smallPtrs[10];
    for (int i = 0; i < 10; i++) {
        smallPtrs[i] = myMalloc(32);
        if (smallPtrs[i] == NULL) {
            printf("Failed to allocate small block %d.\n", i);
        } else {
            memset(smallPtrs[i], 0xAA, 32);
            if (verifyPattern((unsigned char *)smallPtrs[i], 32, 0xAA)) {
                printf("Small block %d passed memory test.\n", i);
            } else {
                printf("Small block %d failed memory test!\n", i);
            }
        }
    }

    for (int i = 0; i < 10; i++) {
        if (smallPtrs[i]) myFree(smallPtrs[i]);
    }

    // --- Large Block Test ---
    printf("\nAllocating and testing large blocks...\n");
    void *largePtrs[5];
    for (int i = 0; i < 10; i++) {
        largePtrs[i] = myMalloc(800);
        if (largePtrs[i] == NULL) {
            printf("Failed to allocate large block %d.\n", i);
        } else {
            memset(largePtrs[i], 0xBB, 800);
            if (verifyPattern((unsigned char *)largePtrs[i], 800, 0xBB)) {
                printf("Large block %d passed memory test.\n", i);
            } else {
                printf("Large block %d failed memory test!\n", i);
            }
        }
    }

    for (int i = 0; i < 10; i++) {
        if (largePtrs[i]) myFree(largePtrs[i]);
    }
}

int main() {
    double startTime, endTime;
    struct timeval start, end;
    gettimeofday(&start, NULL);
    startTime = start.tv_sec + start.tv_usec / 1000000.0;
    testMyMalloc();
    gettimeofday(&end, NULL);
    endTime = end.tv_sec + end.tv_usec / 1000000.0;
    printf("Time: %f\n", endTime - startTime);
    return 0;
}