#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <limits.h>
#include "columnSort.h"

int **matrix;
int **shiftMatrix;

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
int **  allocateMatrix(int rows, int cols) {
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

int* flatten(int **matrix, int rows, int cols, int step) {
    int *tempArray = (int *)malloc(rows * cols * sizeof(int)); 
    int index = 0;
    if (step == 2) { 
        for (int a = 0; a < cols; a++) {
            for (int b = 0; b < rows; b++) {
                tempArray[index++] = matrix[b][a];
            }
        }
    } else {
        // inverted from step 2
        for (int a = 0; a < rows; a++) {
            for (int b = 0; b < cols; b++) {
                tempArray[index++] = matrix[a][b];
            }
        }
    }
    return tempArray;
}

void rewrite(int **matrix, int *tempArray, int rows, int cols, int step) {
    int index;
    if (step == 2) {
        index = 0;
        // rewrite 1d matrix back in row major
        for (int a = 0; a < rows; a++) {
            for (int b = 0; b < cols; b++) {
                matrix[a][b] = tempArray[index++];
            }
        }
    } else {
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
    int *tempArray = flatten(matrix, rows, cols, 2); // flatten matrix using col major
    index = 0;
    
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
int* shiftBack(int **newMatrix, int rows, int cols) {
    int index;
    // Calculate shift value as floor(rows / 2)
    int shift = rows / 2; //will be floor because int division
    int *tempArray = (int *)malloc(rows * cols * sizeof(int));
    index = 0;
    // flatten temp matrix ignoring padding
    for (int a = 0; a < cols; a++) {
        for (int b = 0; b < rows; b++) {
            if ( (a == 0 && b < shift) || (a == cols-1 && b >= shift) ) {
                continue;
            }
            tempArray[index++] = newMatrix[b][a];
        }
    }
    return tempArray;
}

void columnSort(int *A, int numThreads, int length, int width, double *elapsedTime) {
    int step;
    struct timeval start, stop;
    matrix = allocateMatrix(length, width); // make the matrix with temp vals
    shiftMatrix = allocateMatrix(length, width+1);// Allocate new matrix with an extra column because of shift

    // Copy array values to matrix
    for (int i = 0; i < length; i++) {
        for (int j = 0; j < width; j++) {
            matrix[i][j] = A[i * width + j];
        }
    }
    gettimeofday(&start, NULL);
    int *tempArray;
    for (step = 1; step <= 8; step++) {
        switch(step) {
            case 1:
            case 3:
            case 5:
                // Steps 1, 3, 5, and 7 are all the same: sort each column individually
                columnSortInd(matrix, length, width);
                break;
            case 2:
            case 4:
                // Step 4: Reverse Step 2’s Transposition
                // Step 2: Transpose (Turn Columns Into Rows)
                tempArray = flatten(matrix, length, width, step);
                rewrite(matrix, tempArray, length, width, step);
                break;
            case 6:
                // Step 6: Shift ‘Forward’ by ⌊r/2⌋ Positions
                shiftForward(matrix, shiftMatrix, length, width);
                break;
            
            case 7:
                columnSortInd(shiftMatrix, length, width+1);
                break;
            case 8:
                // Step 8: Shift ‘Back’ by ⌊r/2⌋ Positions
                tempArray = shiftBack(shiftMatrix, length, width+1);
                break;
            default:
                printf("Unknown step: %d\n", step);
                exit(0);
                break;
        }
    }  
    // write final matrix back to 1d array
    for (int a = 0; a < (length*width); a++) {
        A[a] = tempArray[a]; 
    }
    gettimeofday(&stop, NULL);
    *elapsedTime = ((stop.tv_sec - start.tv_sec) * 1000000+(stop.tv_usec-start.tv_usec))/1000000.0;
    freeMatrix(matrix);
    freeMatrix(shiftMatrix);

}