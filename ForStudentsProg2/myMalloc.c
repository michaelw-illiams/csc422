// CSc 422
// Program 2 code for sequential myMalloc (small blocks *only*)
// You must add support for large blocks and then concurrency (coarse- and fine-grain)

#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include "myMalloc-helper.h"

pthread_mutex_t globalLock = PTHREAD_MUTEX_INITIALIZER;
int globalMode = 0;

// total amount of memory to allocate, and size of each small chunk
#define SIZE_TOTAL 276672
#define SIZE_SMALL 64

// maintain lists of free blocks and allocated blocks
typedef struct memoryManager {
  chunk *freeSmall;
  chunk *allocSmall;
  chunk *freeLarge;
  chunk *allocLarge;
  void *startLargeMem;  // start of large block memory for pointer checks
  void *endLargeMem;
} memManager;
	
memManager *mMan;

// note that flag is not used here because this is only the sequential version
int myInit(int numCores, int flag) {
  globalMode = flag;
  int numSmall, numLarge;
  int chunkSizeSmall = SIZE_SMALL + sizeof(chunk);
  int chunkSizeLarge = 1024 + sizeof(chunk);

  // allocate SIZE_TOTAL of memory that will be split into small chunks below
  void *mem = malloc(SIZE_TOTAL); 
  if (mem == NULL) { // make sure malloc succeeded
    return -1;
  } 
 
  // set up memory manager
  mMan = (memManager *) malloc(sizeof(memManager));
  mMan->freeSmall = createList();
  mMan->allocSmall = createList();
  mMan->freeLarge = createList();
  mMan->allocLarge = createList();

  void *memSmall = mem;
  void *memLarge = (void *)((char *)mem + SIZE_TOTAL / 2);

  // how many small chunks are there?
  // note that we divide by the *sum* of 64 bytes (for what user gets) and our metadata.
  // the division by 2 is because when you extend to handle large blocks, the small blocks
  // are to take up only half of the total.
  numSmall = (SIZE_TOTAL/2) / (SIZE_SMALL + sizeof(chunk));
  numLarge = (SIZE_TOTAL / 2) / chunkSizeLarge;
    
  // set up free list chunks
  setUpChunks(mMan->freeSmall, mem, numSmall, SIZE_SMALL);
  setUpChunks(mMan->freeLarge, memLarge, numLarge, 1024);

  mMan->startLargeMem = memLarge;
  mMan->endLargeMem = (void *)((char *)memLarge + numLarge * chunkSizeLarge);

  return 0;
}

// myMalloc just needs to get the next chunk and return a pointer to its data
// note the pointer arithmetic that makes sure to skip over our metadata and
// return the user a pointer to the data
void *myMalloc(int size) {

  if (globalMode == 1) {
    pthread_mutex_lock(&globalLock);
  }

  // get a chunk
  chunk *toAlloc;

  if (size <= 64) {
    toAlloc = getChunk(mMan->freeSmall, mMan->allocSmall);
  } else {
    toAlloc = getChunk(mMan->freeLarge, mMan->allocLarge);
  }    
  
  if (globalMode == 1) {
    pthread_mutex_unlock(&globalLock);
  }

  return ((void *) ((char *) toAlloc) + sizeof(chunk));
}

// myFree just needs to put the block back on the free list
// note that this involves taking the pointer that is passed in by the user and
// getting the pointer to the beginning of the chunk (so moving backwards chunk bytes)
void myFree(void *ptr) {
  // find the front of the chunk
  chunk *toFree = (chunk *) ((char *) ptr - sizeof(chunk));

  if (globalMode == 1) {
    pthread_mutex_lock(&globalLock);
  }

  if (toFree->allocSize <= 64) {
    returnChunk(mMan->freeSmall, mMan->allocSmall, toFree);
  } else {
    returnChunk(mMan->freeLarge, mMan->allocLarge, toFree);
  }

  if (globalMode == 1) {
    pthread_mutex_unlock(&globalLock);
  }
  
}
