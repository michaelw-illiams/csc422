#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <limits.h>
#include <pthread.h>
#include "columnSort.h"

pthread_mutex_t lock;
pthread_barrier_t barrier;

typedef struct {
    int thread_id;
    int numThreads;
    int step;
    int **matrix;
    int **shiftMatrix;
    int length;
    int width;
} ThreadData;

// use same comparator as driver
int compareInts(const void *a, const void *b) {
    return *((int *) a) - *((int *) b);
}

// Sort each column in the matrix Individually
void columnSortInd(int **matrix, int length, int width) {
    // Iterate over each column
    int *tempArray = (int *)malloc(length * sizeof(int));
        if (!tempArray) {
            printf("Memory allocation failed for tempColumn\n");
            return;
        }
    for (int j = 0; j < width; j++) {
        // Create a temporary array to store the column
        for (int i = 0; i < length; i++) {
            tempArray[i] = matrix[i][j];
        }
        // Sort each column using qsort
        qsort(tempArray, length, sizeof(int), compareInts);

        // Copy the sorted values back to the matrix
        for (int i = 0; i < length; i++) {
            matrix[i][j] = tempArray[i];
        }
    }
    free(tempArray);
}

// print function to help debug
void printMatrix(int **matrix, int length, int width) {
    printf("Matrix (%d x %d):\n", length, width);
    for (int i = 0; i < length; i++) {
        
        for (int j = 0; j < width; j++) {
            printf("%d ", matrix[i][j]);
        }
        printf("\n");
    }
    printf("\n");
}

// allocate matrix based on class code 
int **allocateMatrix(int rows, int cols) {
    int i;
    int *vals, **temp;
    // allocate values
    vals = (int *)malloc(rows * cols * sizeof(int));
    // allocate vector of pointers
    temp = (int **)malloc(rows * sizeof(int *));
    for (i = 0; i < rows; i++) {
        temp[i] = &(vals[i * cols]);
    }
    return temp;
}

// Function to free the matrix memory
void freeMatrix(int **matrix) {
    free(matrix[0]);
    free(matrix);   
}

// function following McCann's column major transpose for non-square matrix
void transpose(int **matrix, int rows, int cols, int step) {
    int *tempArray = (int *)malloc(rows * cols * sizeof(int)); 
    int index = 0;
    // step 2 calls for columns to rows
    // flatten matrix to 1d
    if (step == 2) { 
        for (int a = 0; a < cols; a++) {
            for (int b = 0; b < rows; b++) {
                tempArray[index++] = matrix[b][a];
            }
        }
        index = 0;
        // rewrite 1d matrix back in row major
        for (int a = 0; a < rows; a++) {
            for (int b = 0; b < cols; b++) {
                matrix[a][b] = tempArray[index++];
            }
        }
    // step 4 calls for rows to columns
    } else {
        // inverted from step 2
        for (int a = 0; a < rows; a++) {
            for (int b = 0; b < cols; b++) {
                tempArray[index++] = matrix[a][b];
            }
        }
        index = 0;
        for (int a = 0; a < cols; a++) {
            for (int b = 0; b < rows; b++) {
                matrix[b][a] = tempArray[index++];
            }
        }
    }
    free(tempArray);
}

// function following McCann's shift algorithm
void shiftForward(int **matrix, int **newMatrix, int rows, int cols) {
    int index;
    // Calculate shift value as floor(rows / 2)
    int shift = rows / 2; // will be floor because int division
    int *tempArray = (int *)malloc(rows * cols * sizeof(int));
    index = 0;
    // flatten matrix
    for (int a = 0; a < cols; a++) {
        for (int b = 0; b < rows; b++) {
            tempArray[index++] = matrix[b][a];
        }
    }
    // use flattened matrix + padding to make new matrix with shift
    index = 0;
    for (int a = 0; a < cols+1; a++) {
        for (int b = 0; b < rows; b++) {
            if (a == 0 && b < shift) {
                newMatrix[b][a] = -1; // function only expects non negatives
            } else if (a == cols && b >= shift) {
                newMatrix[b][a] = INT_MAX;
            } else {
                newMatrix[b][a] = tempArray[index++];
            }
            
        }
    }
    free(tempArray);
}

// function following McCann's shift algorithm
void shiftBack(int **matrix, int **newMatrix, int rows, int cols) {
    int index;
    // Calculate shift value as floor(rows / 2)
    int shift = rows / 2; //will be floor because int division
    int *tempArray = (int *)malloc(rows * cols * sizeof(int));
    index = 0;
    // flatten temp matrix ignoring padding
    for (int a = 0; a < cols; a++) {
        for (int b = 0; b < rows; b++) {
            if ( (a == 0 && b < shift) || (a == cols && b >= shift) ) {
                continue;
            }
            tempArray[index++] = newMatrix[b][a];
        }
    }
    // store values in original matrix
    index = 0;
    for (int a = 0; a < cols; a++) {
        for (int b = 0; b < rows; b++) {
            matrix[b][a] = tempArray[index++];
        }
    }
    free(tempArray);
}

void *worker(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    int tid = data->thread_id;
    int step = data->step;
    int chunkSize = data->length / data->numThreads;
    int start = tid * chunkSize;
    int end = (tid == data->numThreads - 1) ? data->length : start + chunkSize;

    switch (step) {
        case 1:
        case 3:
        case 5:
        case 7:
            // Sort columns in parallel
            for (int j = 0; j < data->width; j++) {
                qsort(&data->matrix[start][j], chunkSize, sizeof(int), compareInts);
            }
            break;

        case 2:
        case 4:
            if (tid == 0) transpose(data->matrix, data->length, data->width, step);
            break;

        case 6:
            if (tid == 0) shiftForward(data->matrix, data->shiftMatrix, data->length, data->width);
            break;

        case 8:
            if (tid == 0) shiftBack(data->matrix, data->shiftMatrix, data->length, data->width);
            break;
    }

    pthread_barrier_wait(&barrier);
    return NULL;
}

void columnSort(int *A, int numThreads, int length, int width, double *elapsedTime) {
    int i, step;
    struct timeval start, stop;
    pthread_t *threads = (pthread_t *)malloc(numThreads * sizeof(pthread_t));
    ThreadData *threadData = (ThreadData *)malloc(numThreads * sizeof(ThreadData));

    int **matrix = allocateMatrix(length, width);
    int **shiftMatrix = allocateMatrix(length, width + 1);

    for (i = 0; i < length; i++) {
        for (int j = 0; j < width; j++) {
            matrix[i][j] = A[i * width + j];
        }
    }

    pthread_mutex_init(&lock, NULL);
    pthread_barrier_init(&barrier, NULL, numThreads);

    gettimeofday(&start, NULL);

    for (step = 1; step <= 8; step++) {
        for (i = 0; i < numThreads; i++) {
            threadData[i] = (ThreadData){i, numThreads, step, matrix, shiftMatrix, length, width};
            pthread_create(&threads[i], NULL, worker, (void *)&threadData[i]);
        }
        for (i = 0; i < numThreads; i++) {
            pthread_join(threads[i], NULL);
        }
    }

    gettimeofday(&stop, NULL);
    *elapsedTime = ((stop.tv_sec - start.tv_sec) * 1000000 + (stop.tv_usec - start.tv_usec)) / 1000000.0;

    freeMatrix(matrix); 
    freeMatrix(shiftMatrix);
    free(threads);
    free(threadData);
    pthread_mutex_destroy(&lock);
    pthread_barrier_destroy(&barrier);
}