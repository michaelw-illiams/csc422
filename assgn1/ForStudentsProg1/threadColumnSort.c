#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <limits.h>
#include <pthread.h>
#include <math.h>
#include "columnSort.h"

int numThreads, rows, cols;
int currentStep = 1;
int **matrix, **shiftMatrix;
volatile int *arrive;  // Dissemination barrier
pthread_mutex_t stepMutex;
pthread_cond_t stepCond;

// dissemination barrier bases on class psuedocode
void barrier_wait(int id) {
    int logP = ceil(log2(numThreads));
    printf("Thread %d waiting at barrier during step %d\n", id, currentStep);
    // Print the arrive array
    // printf("id = %d, logP = %d\n", id, logP);
    // printf("Arrive array: ");
    // for (int i = 0; i < numThreads; i++) {
        // printf("%d ", arrive[i]);
    // }
    // printf("\n");
    int j, waitFor;
    for (j = 1; j <= logP; j++) {
        while (arrive[id] != 0);  // Wait for reset
        arrive[id] = j;
        // printf("Arrive[%d] = %d\n", id, j);
        waitFor = (id + (int)pow(2, j-1)) % numThreads;  // Compute partner
        // printf("waitfor at id:%d = %d\n", id, waitFor);
        // printf("id = %d Arrive[waitfor] = %d\n", id, arrive[waitFor]);
        while (arrive[waitFor] != j);  // Wait for partner
        arrive[id] = 0;  // Reset
    }
    printf("Thread %d passed barrier during step %d\n", id, currentStep);
}

// use same comparator as driver
int compareInts(const void *a, const void *b) {
    return *((int *) a) - *((int *) b);
}

// Sort each column in the matrix Individually
void columnSortInd(int id, int **matrix, int rows, int cols) {

    // Compute the number of columns each thread should handle
    int baseCols = cols / numThreads;
    int extraCols = cols % numThreads;

    // Calculate the start and end columns for this thread
    int startCol, endCol;
    if (id < extraCols) {
        startCol = id * (baseCols + 1);
        endCol = startCol + baseCols + 1;
    } else {
        startCol = id * baseCols + extraCols;
        endCol = startCol + baseCols;
    }

    // printf("Threadid = %d, startcol = %d, endcol = %d\n", id, startCol, endCol);


    // Iterate over each column
    int *tempArray = (int *)malloc(rows * sizeof(int));
        if (!tempArray) {
            printf("Memory allocation failed for tempColumn\n");
            return;
        }
    for (int j = startCol; j < endCol; j++) {
        // Create a temporary array to store the column
        for (int i = 0; i < rows; i++) {
            tempArray[i] = matrix[i][j];
        }
        // Sort each column using qsort
        qsort(tempArray, rows, sizeof(int), compareInts);

        // Copy the sorted values back to the matrix
        for (int i = 0; i < rows; i++) {
            matrix[i][j] = tempArray[i];
        }
    }
    free(tempArray);
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
void transpose(int id, int **matrix, int rows, int cols, int step) {
    
    int *tempArray = (int *)malloc(rows * cols * sizeof(int)); 
    int index = 0;

    // Compute the number of columns each thread should handle
    int baseCols = cols / numThreads;
    int extraCols = cols % numThreads;

    // Calculate the start and end columns for this thread
    int startCol, endCol;
    if (id < extraCols) {
        startCol = id * (baseCols + 1);
        endCol = startCol + baseCols + 1;
    } else {
        startCol = id * baseCols + extraCols;
        endCol = startCol + baseCols;
    }

    int rowsPerCol = rows / cols;
    int startRow = startCol * rowsPerCol;
    int endRow = endCol * rowsPerCol;
    printf("id:%d - startCol = %d, endcol = %d\n", id, startCol, endCol);
    printf("id:%d - rowspercol = %d\n", id, rowsPerCol);
    printf("id:%d - startrow = %d, endrow = %d\n", id, startRow, endRow);
    
    // step 2 calls for columns to rows
    // flatten matrix to 1d
    if (step == 2) { 
        for (int a = startCol; a < endCol; a++) {
            for (int b = 0; b < rows; b++) {
                tempArray[index++] = matrix[b][a];
            }
        }
        barrier_wait(id);
        index = 0;
        // rewrite 1d matrix back in row major
        for (int a = startRow; a < endRow; a++) {
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

// print function to help debug
void printMatrix(int **matrix, int length, int width) {
    printf("Step %d Matrix (%d x %d):\n", currentStep, length, width);
    for (int i = 0; i < length; i++) {
        
        for (int j = 0; j < width; j++) {
            printf("%d ", matrix[i][j]);
        }
        printf("\n");
    }
    printf("\n");
}

void *worker(void *arg) {
    int id = *((int *) arg);
    while (currentStep <= 8) {
        printf("Thread %d Starting step %d\n", id, currentStep);
        switch (currentStep) {
            case 1:
            case 3:
            case 5:
                printf("threadid = %d, part1\n", id);
                columnSortInd(id, matrix, rows, cols);
                break;
            case 2:
            // case 4:
                printf("threadid = %d, part3\n", id);
                transpose(id, matrix, rows, cols, currentStep);
                break;
            // case 6:
            //     shiftForward(matrix, shiftMatrix, rows, cols);
            //     break;
            // case 7:
            //     columnSortInd(id, shiftMatrix, rows, cols + 1);
            //     break;
            // case 8:
            //     shiftBack(matrix, shiftMatrix, rows, cols); 
            //     break;
            default:
                printf("Unknown step: %d\n", currentStep);
                break;
        }
        barrier_wait(id); // Synchronize all threads
        printMatrix(matrix, rows, cols);
        if (id == 0) { 
            currentStep++; 
            printf("All threads finished step %d\n", currentStep - 1);
        } // Move to next step only after all threads finish

        if (currentStep == 4) {
            exit(0);
        }     
    }
    return NULL;
}

void columnSort(int *A, int threads, int length, int width, double *elapsedTime) {
    int i;
    int *params;
    numThreads = threads;
    rows = length;
    cols = width; 
    struct timeval start, stop;
    matrix = allocateMatrix(length, width); // make the matrix with temp vals
    shiftMatrix = allocateMatrix(length, width+1);// Allocate new matrix with an extra column because of shift

    // Copy array values to matrix
    for (i = 0; i < length; i++) {
        for (int j = 0; j < width; j++) {
            matrix[i][j] = A[i * width + j];
        }
    }
    // Allocate memory for row pointers
    arrive = (volatile int*)malloc(numThreads * sizeof(int));
    if (!arrive) {
        printf("Memory allocation failed for arrive\n");
        exit(1);
    }
    for (int i = 0; i < numThreads; i++) {
        arrive[i] = 0;
    }

    pthread_mutex_init(&stepMutex, NULL);
    pthread_cond_init(&stepCond, NULL);
    
    gettimeofday(&start, NULL);

    // Allocate thread handles
    pthread_t *threadHandles = (pthread_t *)malloc(numThreads * sizeof(pthread_t));
    params = (int *)malloc(numThreads * sizeof(int));
    //create threads
    for (i = 0; i < numThreads; i++) {
        params[i] = i;
        pthread_create(&threadHandles[i], NULL, worker, (void *)&params[i]);
    }

    for (i = 0; i < numThreads; i++) {
        pthread_join(threadHandles[i], NULL);
    }

    // write final matrix back to 1d array
    int index = 0;
    for (int a = 0; a < width; a++) {
        for (int b = 0; b < length; b++) {
            A[index++] = matrix[b][a];
        }
    }
    gettimeofday(&stop, NULL);
    *elapsedTime = ((stop.tv_sec - start.tv_sec) * 1000000+(stop.tv_usec-start.tv_usec))/1000000.0;
    free(params);

    free((void *)arrive);
    pthread_mutex_destroy(&stepMutex);
    pthread_cond_destroy(&stepCond);
    freeMatrix(matrix);
    freeMatrix(shiftMatrix);

}