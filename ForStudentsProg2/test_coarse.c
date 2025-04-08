#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/time.h>
#include "myMalloc.h"

#define ALLOCS_PER_THREAD 1000

void *threadRoutine(void *arg) {
    int tid = *(int *)arg;

    for (int i = 0; i < ALLOCS_PER_THREAD; i++) {
        int size = (i % 2 == 0) ? 32 : 800;  // alternate small/large
        void *ptr = myMalloc(size);
        if (!ptr) {
            printf("Thread %d: Allocation %d FAILED (size=%d)\n", tid, i, size);
            continue;
        }

        memset(ptr, tid, size);  // write pattern
        for (int j = 0; j < size; j++) {
            if (((unsigned char *)ptr)[j] != tid) {
                printf("Thread %d: Memory validation FAILED at alloc %d\n", tid, i);
                break;
            }
        }

        myFree(ptr);
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <num_threads>\n", argv[0]);
        return 1;
    }

    int numThreads = atoi(argv[1]);
    if (numThreads <= 0 || numThreads > 8) {
        fprintf(stderr, "Error: num_threads must be between 1 and 8.\n");
        return 1;
    }

    pthread_t threads[numThreads];
    int tids[numThreads];

    if (myInit(numThreads, 1) != 0) {
        printf("myInit failed.\n");
        return 1;
    }

    struct timeval start, end;
    gettimeofday(&start, NULL);

    for (int i = 0; i < numThreads; i++) {
        tids[i] = i;
        pthread_create(&threads[i], NULL, threadRoutine, &tids[i]);
    }

    for (int i = 0; i < numThreads; i++) {
        pthread_join(threads[i], NULL);
    }

    gettimeofday(&end, NULL);
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1e6;

    printf("All threads completed. Time elapsed: %.4f seconds\n", elapsed);
    return 0;
}
