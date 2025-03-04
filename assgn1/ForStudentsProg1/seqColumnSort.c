#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "columnSort.h"

// Function to compare integers (used in qsort)
int compareInts(const void *a, const void *b) {
    return (*(int *)a - *(int *)b);
}

// Function to transpose an r x s matrix
void transpose(int *matrix, int r, int s) {
    int *temp = (int *)malloc(r * s * sizeof(int));
    for (int i = 0; i < r; i++) {
        for (int j = 0; j < s; j++) {
            temp[j * r + i] = matrix[i * s + j];
        }
    }
    for (int i = 0; i < r * s; i++) {
        matrix[i] = temp[i];
    }
    free(temp);
}

// Function to shift down by r/2 with wrap-around
void shiftDown(int *matrix, int r, int s) {
    int half_r = r / 2;
    int *temp = (int *)malloc(r * s * sizeof(int));
    
    for (int j = 0; j < s; j++) {
        for (int i = 0; i < r; i++) {
            int new_i = (i + half_r) % r;
            temp[new_i * s + j] = matrix[i * s + j];
        }
    }
    for (int i = 0; i < r * s; i++) {
        matrix[i] = temp[i];
    }
    free(temp);
}

// Function to shift up by r/2 with wrap-around
void shiftUp(int *matrix, int r, int s) {
    int half_r = r / 2;
    int *temp = (int *)malloc(r * s * sizeof(int));
    
    for (int j = 0; j < s; j++) {
        for (int i = 0; i < r; i++) {
            int new_i = (i - half_r + r) % r;
            temp[new_i * s + j] = matrix[i * s + j];
        }
    }
    for (int i = 0; i < r * s; i++) {
        matrix[i] = temp[i];
    }
    free(temp);
}

void columnSort(int *A, int numThreads, int r, int s, double *elapsedTime) {
    struct timeval start, end;
    gettimeofday(&start, NULL);
    
    // Allocate matrix
    int *matrix = (int *)malloc(r * s * sizeof(int));
    for (int i = 0; i < r * s; i++) {
        matrix[i] = A[i];
    }
    
    // Step 1: Sort each column
    for (int j = 0; j < s; j++) {
        qsort(&matrix[j * r], r, sizeof(int), compareInts);
    }
    
    // Step 2: Transpose and reshape
    transpose(matrix, r, s);
    
    // Step 3: Sort each column
    for (int j = 0; j < r; j++) {
        qsort(&matrix[j * s], s, sizeof(int), compareInts);
    }
    
    // Step 4: Reshape and transpose
    transpose(matrix, s, r);
    
    // Step 5: Sort each column
    for (int j = 0; j < s; j++) {
        qsort(&matrix[j * r], r, sizeof(int), compareInts);
    }
    
    // Step 6: Shift down by r/2
    shiftDown(matrix, r, s);
    
    // Step 7: Sort each column
    for (int j = 0; j < s; j++) {
        qsort(&matrix[j * r], r, sizeof(int), compareInts);
    }
    
    // Step 8: Shift up by r/2
    shiftUp(matrix, r, s);
    
    // Copy back sorted values
    for (int i = 0; i < r * s; i++) {
        A[i] = matrix[i];
    }
    
    free(matrix);
    
    gettimeofday(&end, NULL);
    *elapsedTime = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
}