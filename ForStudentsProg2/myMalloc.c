// CSc 422
// Program 2 code for sequential myMalloc (small blocks *only*)
// You must add support for large blocks and then concurrency (coarse- and fine-grain)

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "myMalloc-helper.h"

// total amount of memory to allocate
#define SIZE_TOTAL 276672
#define SIZE_SMALL 64
#define MAX_THREADS 8
#define SIZE_OVERFLOW SIZE_TOTAL

// maintain lists of free blocks and allocated blocks
typedef struct memoryManager {
  chunk *freeSmall;
  chunk *allocSmall;
  chunk *freeLarge;
  chunk *allocLarge;
  void *startLargeMem;  // start of large block memory for pointer checks
  void *endLargeMem;
} memManager;

// Thread-local keys and global structures
static memManager *threadManagers[MAX_THREADS+1];     // 1 per thread
static memManager *overflowManager;
static pthread_mutex_t overflowLockSmall = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t overflowLockLarge = PTHREAD_MUTEX_INITIALIZER;

static pthread_key_t threadKey;
static int threadCount;
static pthread_mutex_t idAssignLock = PTHREAD_MUTEX_INITIALIZER;
static int globalMode = 0;

int myInit(int numCores, int flag) {
  globalMode = flag;
  threadCount = 0;
  overflowManager = NULL;
  // Create thread-local key
  pthread_key_create(&threadKey, NULL);

  void *overflowMem = malloc(SIZE_OVERFLOW);
  if (!overflowMem) return -1;

  overflowManager = malloc(sizeof(memManager));
  overflowManager->freeSmall = createList();
  overflowManager->allocSmall = createList();
  overflowManager->freeLarge = createList();
  overflowManager->allocLarge = createList();

  int numSmallOverflow = (SIZE_OVERFLOW / 2) / (SIZE_SMALL + sizeof(chunk));
  int numLargeOverflow = (SIZE_OVERFLOW / 2) / (1024 + sizeof(chunk));
  void *overflowSmallMem = overflowMem;
  void *overflowLargeMem = (char *)overflowMem + SIZE_OVERFLOW / 2;

  setUpChunks(overflowManager->freeSmall, overflowSmallMem, numSmallOverflow, SIZE_SMALL);
  setUpChunks(overflowManager->freeLarge, overflowLargeMem, numLargeOverflow, 1024);

  overflowManager->startLargeMem = overflowLargeMem;
  overflowManager->endLargeMem = (char *)overflowLargeMem + numLargeOverflow * (1024 + sizeof(chunk));
  int numToInit = (flag == 0) ? 1 : numCores+1;

  for (int i = 0; i < numToInit; i++) {
    threadManagers[i] = malloc(sizeof(memManager));

    if (flag == 1) {
        // Coarse-grained: zero-sized lists (force overflow use)
        threadManagers[i]->freeSmall = createList();
        threadManagers[i]->allocSmall = createList();
        threadManagers[i]->freeLarge = createList();
        threadManagers[i]->allocLarge = createList();
        continue;
    }

    // Fine-grained or single-threaded: allocate per-thread pool
    void *mem = malloc(SIZE_TOTAL);
    if (!mem) return -1;
    threadManagers[i]->freeSmall = createList();
    threadManagers[i]->allocSmall = createList();
    threadManagers[i]->freeLarge = createList();
    threadManagers[i]->allocLarge = createList();

    int numSmall = (SIZE_TOTAL / 2) / (SIZE_SMALL + sizeof(chunk));
    int numLarge = (SIZE_TOTAL / 2) / (1024 + sizeof(chunk));

    void *memSmall = mem;
    void *memLarge = (char *)mem + SIZE_TOTAL / 2;

    setUpChunks(threadManagers[i]->freeSmall, memSmall, numSmall, SIZE_SMALL);
    setUpChunks(threadManagers[i]->freeLarge, memLarge, numLarge, 1024);

    threadManagers[i]->startLargeMem = memLarge;
    threadManagers[i]->endLargeMem = (char *)memLarge + numLarge * (1024 + sizeof(chunk));
  }
  
  return 0;
}

// empty list check
static int isEmptyList(chunk *list) {
  return (list->next == list);
}

void assignThreadManager() {
  void *current = pthread_getspecific(threadKey);
  if (current != NULL) return;
  pthread_mutex_lock(&idAssignLock);
  int id = threadCount;
  // printf("Thread assigned ID %d\n", id);
  if (id >= MAX_THREADS) {
      // fprintf(stderr, "Error: too many threads\n");
      exit(1);
  }
  memManager *mgr = threadManagers[id];
  if (!mgr) {
      // fprintf(stderr, "threadManagers[%d] is NULL!\n", id);
      exit(1);
  }
  pthread_setspecific(threadKey, mgr);
  threadCount++;
  // First-time touch for output file
  char str[32];
  snprintf(str, sizeof(str), "touch Id-%d\n", id + 1);
  system(str);
  pthread_mutex_unlock(&idAssignLock);
}

// myMalloc just needs to get the next chunk and return a pointer to its data
// note the pointer arithmetic that makes sure to skip over our metadata and
// return the user a pointer to the data
void *myMalloc(int size) {
  if (size > 1024) {
    return NULL;
  } 
  assignThreadManager();
  memManager *mgr = (memManager *) pthread_getspecific(threadKey);
  if (!mgr) {
    // fprintf(stderr, "Thread-local memory manager is NULL!\n");
    exit(1);
  } 
  // get a chunk
  chunk *toAlloc = NULL;
  if (size <= 64) {
      if (!isEmptyList(mgr->freeSmall)) {
          toAlloc = getChunk(mgr->freeSmall, mgr->allocSmall);
      } else {
          pthread_mutex_lock(&overflowLockSmall);
          if (!isEmptyList(overflowManager->freeSmall)) {
              toAlloc = getChunk(overflowManager->freeSmall, overflowManager->allocSmall);
              static int overflowTouched = 0;
              if (!overflowTouched) {
                  system("touch Overflow");
                  overflowTouched = 1;
              }
          }
          pthread_mutex_unlock(&overflowLockSmall);
      }
  } else {
      if (!isEmptyList(mgr->freeLarge)) {
          toAlloc = getChunk(mgr->freeLarge, mgr->allocLarge);
      } else {
          pthread_mutex_lock(&overflowLockLarge);
          if (!isEmptyList(overflowManager->freeLarge)) {
              toAlloc = getChunk(overflowManager->freeLarge, overflowManager->allocLarge);
              static int overflowTouched = 0;
              if (!overflowTouched) {
                  system("touch Overflow");
                  overflowTouched = 1;
              }
          }
          pthread_mutex_unlock(&overflowLockLarge);
      }
  }
  return toAlloc ? (void *)((char *)toAlloc + sizeof(chunk)) : NULL;
}

// myFree just needs to put the block back on the free list
// note that this involves taking the pointer that is passed in by the user and
// getting the pointer to the beginning of the chunk (so moving backwards chunk bytes)
void myFree(void *ptr) {
  if (!ptr) {
    return;
  }
  // find the front of the chunk
  chunk *toFree = (chunk *) ((char *) ptr - sizeof(chunk));
  assignThreadManager();
  memManager *mgr = (memManager *) pthread_getspecific(threadKey);
  if (!mgr) {
    fprintf(stderr, "Thread-local memory manager is NULL!\n");
    exit(1);
  } 

  // Determine if this pointer is from overflow
  if (toFree >= (chunk *)overflowManager->startLargeMem && toFree < (chunk *)overflowManager->endLargeMem) {
    if (toFree->allocSize <= 64) {
        pthread_mutex_lock(&overflowLockSmall);
        returnChunk(overflowManager->freeSmall, overflowManager->allocSmall, toFree);
        pthread_mutex_unlock(&overflowLockSmall);
    } else {
        pthread_mutex_lock(&overflowLockLarge);
        returnChunk(overflowManager->freeLarge, overflowManager->allocLarge, toFree);
        pthread_mutex_unlock(&overflowLockLarge);
    }
  } else {
    if (toFree->allocSize <= 64) {
        returnChunk(mgr->freeSmall, mgr->allocSmall, toFree);
    } else {
        returnChunk(mgr->freeLarge, mgr->allocLarge, toFree);
    }
  }
}
