// CSc 422
// Program 2 header file for sequential myMalloc-helper

// a chunk is a integer (allocSize) along with prev and next pointers
// implicitly, after each of these, there is data of allocSize
typedef struct chunk {
  int allocSize;
  struct chunk *prev;
  struct chunk *next;
} chunk;

chunk *createList();
void setUpChunks(chunk *list, void *mem, int num, int blockSize);
chunk *getChunk(chunk *freeList, chunk *allocList);
void returnChunk(chunk *freeList, chunk *allocList, chunk *toFree);
