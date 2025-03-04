#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "columnSort.h"

// Function to compare integers for qsort
int compare(const void *a, const void *b) {
    return (*(int *)a - *(int *)b);
}

// Function to allocate a 2D matrix in row-major order
int **allocateMatrix(int rows, int cols) {
    int *data = (int *)malloc(rows * cols * sizeof(int));
    int **matrix = (int **)malloc(rows * sizeof(int *));
    
    for (int i = 0; i < rows; i++)
        matrix[i] = &data[i * cols];

    return matrix;
}

// Function to perform row-major transpose
void transpose(int **matrix, int rows, int cols) {
    int **temp = allocateMatrix(cols, rows); // Transposed matrix
    for (int i = 0; i < rows; i++)
        for (int j = 0; j < cols; j++)
            temp[j][i] = matrix[i][j];

    // Copy back the transposed values
    for (int i = 0; i < cols; i++)
        for (int j = 0; j < rows; j++)
            matrix[i][j] = temp[i][j];

    free(temp[0]); // Free contiguous data block
    free(temp);
}

// Function to shift rows down by r/2 (wrapping around)
void shiftDown(int **matrix, int rows, int cols) {
    int halfRows = rows / 2;
    int **temp = allocateMatrix(rows, cols);

    for (int j = 0; j < cols; j++) {
        for (int i = 0; i < halfRows; i++)
            temp[i][j] = -1; // Fill top half with -∞
        
        for (int i = halfRows; i < rows; i++)
            temp[i][j] = matrix[i - halfRows][j]; // Shift lower half up

        if (j + 1 < cols) { // Wrap to next column
            for (int i = 0; i < halfRows; i++)
                temp[i][j + 1] = matrix[i + halfRows][j];
        }
    }

    // Copy back the shifted values
    for (int i = 0; i < rows; i++)
        for (int j = 0; j < cols; j++)
            matrix[i][j] = temp[i][j];

    free(temp[0]); // Free contiguous data block
    free(temp);
}

// Function to shift rows up by r/2 (wrapping around)
void shiftUp(int **matrix, int rows, int cols) {
    int halfRows = rows / 2;
    int **temp = allocateMatrix(rows, cols);

    for (int j = 0; j < cols; j++) {
        for (int i = 0; i < halfRows; i++)
            temp[i][j] = matrix[i + halfRows][j]; // Shift upper half down

        if (j > 0) { // Wrap from previous column
            for (int i = halfRows; i < rows; i++)
                temp[i][j] = matrix[i - halfRows][j - 1];
        } else {
            for (int i = halfRows; i < rows; i++)
                temp[i][j] = 10000; // Fill bottom half with ∞
        }
    }

    // Copy back the shifted values
    for (int i = 0; i < rows; i++)
        for (int j = 0; j < cols; j++)
            matrix[i][j] = temp[i][j];

    free(temp[0]); // Free contiguous data block
    free(temp);
}

// The columnSort function as per specification
void columnSort(int *A, int numThreads, int length, int width, double *elapsedTime) {
    struct timeval start, end;
    
    int **matrix = allocateMatrix(width, length); // Allocate s × r matrix

    // Copy 1D array to 2D matrix
    for (int i = 0, index = 0; i < width; i++)
        for (int j = 0; j < length; j++)
            matrix[i][j] = A[index++];

    gettimeofday(&start, NULL); // Start timing

    // Step 1: Sort rows
    for (int i = 0; i < width; i++)
        qsort(matrix[i], length, sizeof(int), compare);

    // Step 2: Transpose and reshape
    transpose(matrix, width, length);

    // Step 3: Sort rows
    for (int i = 0; i < length; i++)
        qsort(matrix[i], width, sizeof(int), compare);

    // Step 4: Reshape and transpose back
    transpose(matrix, length, width);

    // Step 5: Sort rows
    for (int i = 0; i < width; i++)
        qsort(matrix[i], length, sizeof(int), compare);

    // Step 6: Shift down by r/2
    shiftDown(matrix, width, length);

    // Step 7: Sort rows
    for (int i = 0; i < width; i++)
        
    (matrix[i], length, sizeof(int), compare);

    // Step 8: Shift up by r/2
    shiftUp(matrix, width, length);

    gettimeofday(&end, NULL); // End timing

    // Copy sorted matrix back to A
    for (int i = 0, index = 0; i < width; i++)
        for (int j = 0; j < length; j++)
            A[index++] = matrix[i][j];

    // Compute elapsed time
    *elapsedTime = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;

    free(matrix[0]); // Free contiguous data block
    free(matrix);
}
