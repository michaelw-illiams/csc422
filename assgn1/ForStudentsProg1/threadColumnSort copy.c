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
int *finalArr;
volatile int *arrive;  // Dissemination barrier

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
        while (arrive[id] != 0 && currentStep != 11);  // Wait for reset
        arrive[id] = j;
        // printf("Arrive[%d] = %d\n", id, j);
        waitFor = (id + (int)pow(2, j-1)) % numThreads;  // Compute partner
        // printf("waitfor at id:%d = %d\n", id, waitFor);
        // printf("id = %d Arrive[waitfor] = %d\n", id, arrive[waitFor]);
        while (arrive[waitFor] != j && currentStep != 11);  // Wait for partner
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

int* flatten(int **matrix, int id, int rows, int cols, int step) {
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
    printf("id:%d - startCol = %d, endcol = %d\n", id, startCol, endCol);
    int *tempArray = (int *)malloc( (rows*(endCol-startCol) )* sizeof(int)); 
    int index = 0;
    if (step == 2) { 
        for (int a = startCol; a < endCol; a++) {
            for (int b = 0; b < rows; b++) {
                tempArray[index++] = matrix[b][a];
            }
        }
        for (int i = 0; i < (rows*(endCol-startCol)); i++) {  // Adjust this based on the size of tempArray
            printf("%d ", tempArray[i]);
        }
        printf("\n");
    } else {
        int rowsPerCol = rows / cols;
        int startRow = startCol * rowsPerCol;
        int endRow = endCol * rowsPerCol;
        // inverted from step 2
        for (int a = startRow; a < endRow; a++) {
            for (int b = 0; b < cols; b++) {
                tempArray[index++] = matrix[a][b];
            }
        }
        for (int i = 0; i < (rows*(endCol-startCol)); i++) {  // Adjust this based on the size of tempArray
            printf("%d ", tempArray[i]);
        }
        printf("\n");
    }
    return tempArray;
}

void rewrite(int **matrix, int id, int *tempArray, int rows, int cols, int step) {
    int index;
    int startCol, endCol;
    int baseCols = cols / numThreads;
    int extraCols = cols % numThreads;
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

    if (step == 2) {
        index = 0;
        // rewrite 1d matrix back in row major
        for (int a = startRow; a < endRow; a++) {
            for (int b = 0; b < cols; b++) {
                matrix[a][b] = tempArray[index++];
            }
        }
    } else {
        index = 0;
        for (int a = startCol; a < endCol; a++) {
            for (int b = 0; b < rows; b++) {
                matrix[b][a] = tempArray[index++];
            }
        }
    }
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
}

// function following McCann's shift algorithm
void shiftForward(int **newMatrix, int *tempArray, int id, int rows, int cols) {

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

    int index;
    // Calculate shift value as floor(rows / 2)
    int shift = rows / 2; // will be floor because int division
    // use flattened matrix + padding to make new matrix with shift
    index = 0;
    for (int a = startCol; a < endCol+1; a++) {
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
void shiftBack(int **newMatrix, int id, int rows, int cols) {
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

    int numRowsProcessed = rows * (endCol - startCol);
    int index = id * numRowsProcessed; 
    printf("numRowsProcessed = %d\n", numRowsProcessed);
    printf("index = %d\n", index);
    // Calculate shift value as floor(rows / 2)
    int shift = rows / 2; //will be floor because int division
    // flatten temp matrix ignoring padding
    printf("final values\n");
    for (int a = startCol; a < endCol+1; a++) {
        for (int b = 0; b < rows; b++) {
            if ( (a == 0 && b < shift) || (a == cols && b >= shift) ) {
                continue;
            }
            finalArr[index++] = newMatrix[b][a];
            printf("b = %d, a = %d, newmatval = %d\n", b, a, newMatrix[b][a]);
        }
        printf("\n");
    }
}

// print function to help debug
void printMatrix(int **matrix, int id, int length, int width) {
    printf("Step %d id %d Matrix (%d x %d):\n", currentStep, id, length, width);
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
    int *tempArray;
    while (currentStep <= 11) {
        printf("Thread %d Starting step %d\n", id, currentStep);
        switch (currentStep) {
            case 1: // step 1
            case 4: // step 3
            case 7: // step 5
                columnSortInd(id, matrix, rows, cols);
                break;
            case 2: // part 1 of step 2
            case 8: //part 1 of step 6
                tempArray = flatten(matrix, id, rows, cols, 2);
                break;
            case 3: // part 2 of step 2
                rewrite(matrix, id, tempArray, rows, cols, 2);
                break;
            case 5: // part 1 of step 4
                tempArray = flatten(matrix, id, rows, cols, 4); 
                break;
            case 6: // part 2 of step 4
                rewrite(matrix, id, tempArray, rows, cols, 4);
                break;
            case 9: // step 6
                shiftForward(shiftMatrix, tempArray, id, rows, cols);
                break;
            case 10: // step 7
                columnSortInd(id, shiftMatrix, rows, cols + 1);
                break;
            case 11: // step 8
                shiftBack(shiftMatrix, id, rows, cols); 
                break;
            default:
                printf("Unknown step: %d\n", currentStep);
                break;
        }
        barrier_wait(id); // Synchronize all threads
        printMatrix(matrix, id, rows, cols);
        if (id == 0) { 
            currentStep++; 
            printf("All threads finished step %d\n", currentStep - 1);
        } // Move to next step only after all threads finish   
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
    finalArr = (int *)malloc(rows * cols * sizeof(int));

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

    
    gettimeofday(&start, NULL);

    // Allocate thread handles
    pthread_t *threadHandles = (pthread_t *)malloc(numThreads * sizeof(pthread_t));
    params = (int *)malloc(numThreads * sizeof(int));
    //create threads
    for (i = 0; i < numThreads; i++) {
        params[i] = i;
        pthread_create(&threadHandles[i], NULL, worker, (void *)&params[i]);
    }
    printf("aiygfsaiuoyfgsuifg\n");
    for (i = 0; i < numThreads; i++) {
        pthread_join(threadHandles[i], NULL);
    }

    // write final matrix back to 1d array
    for (int a = 0; a < (length*width); a++) {
        A[a] = finalArr[a]; 
    }
    printf("final print [");
    for (int a = 0; a < (length*width); a++) {
        printf("%d, ", finalArr[a]); 
    }
    printf("]\n");
    gettimeofday(&stop, NULL);
    *elapsedTime = ((stop.tv_sec - start.tv_sec) * 1000000+(stop.tv_usec-start.tv_usec))/1000000.0;
    free(params);

    free((void *)arrive);
    freeMatrix(matrix);
    freeMatrix(shiftMatrix);

}