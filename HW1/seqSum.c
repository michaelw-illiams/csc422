#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

int main(int argc, char **argv) {
    int i;
    int arraySize;
    long long sum = 0;
    struct timeval start, stop;
    double elapsed;

    arraySize = atoi(argv[1]) + 1;
    long long *array = (long long *)malloc(sizeof(long long) * arraySize);
    if (array == NULL) {
        printf("malloc fail\n");
        return 1;
    }

    for (i = 0; i < arraySize; i++) {
        array[i] = i;
    }
    
    gettimeofday(&start, NULL);
    for (i = 0; i < arraySize; i++) {
        sum += array[i];
    }
    printf("%lld\n", sum);
    gettimeofday(&stop, NULL);

    // elapsed = ((stop.tv_sec - start.tv_sec)*1000000 +(stop.tv_usec-start.tv_usec)) / 1000000.0;
    // printf("Elapsed time: %f seconds\n", elapsed);

    free(array);
    return 0;
}
