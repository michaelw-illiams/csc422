#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>

long long *A;
long long *partialsums;
int n, q; 
pthread_mutex_t L;

void *worker(void *arg) {
    int j;
    int i = *((int *)arg);
    int size = n / q;
    int start_idx = i * size;
    int end_idx;
    if (i == q - 1) {
        end_idx = n;
    } else {
        end_idx = start_idx + size;
    }
    long long local_sum = 0;
    for (j = start_idx; j < end_idx; j++) {
        local_sum += A[j];
    }

    pthread_mutex_lock(&L);  
    partialsums[i] = local_sum;
    pthread_mutex_unlock(&L);
}

int main(int argc, char **argv) {
    int i;
    int *params;
    long long total = 0;
    struct timeval start, stop;
    n = atoi(argv[1]) + 1;
    q = atoi(argv[2]);

    A = (long long *)malloc(sizeof(long long) * n);
    partialsums = (long long *)malloc(sizeof(long long) * q);

    for (i = 0; i < n; i++) {
        A[i] = i;
    }

    pthread_mutex_init(&L, NULL);

    gettimeofday(&start, NULL);
    // Allocate thread handles
    pthread_t *threads = (pthread_t *)malloc(q * sizeof(pthread_t));
    params = (int *)malloc(q * sizeof(int));

    for (i = 0; i < q; i++) {
        params[i] = i;
        pthread_create(&threads[i], NULL, worker, (void *)&params[i]);
    }
    for (i = 0; i < q; i++) {
        pthread_join(threads[i], NULL);
    }

    for (i = 0; i < q; i++) {
        total += partialsums[i];
    }

    printf("%lld\n", total);
    gettimeofday(&stop, NULL);

    // double elapsed = ((stop.tv_sec - start.tv_sec) * 1000000 + (stop.tv_usec - start.tv_usec)) / 1000000.0;
    // printf("Elapsed time: %f seconds\n", elapsed);

    free(A);
    free(partialsums);
    free(threads);
    free(params);
    return 0;
}
