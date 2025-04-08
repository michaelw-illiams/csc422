#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

pthread_key_t key;
pthread_mutex_t idLock;
int IDarray[4] = {1,2,3,4};

void print(){
  // getspecific call---retrieve the id
  int my_id = *((int *) pthread_getspecific(key));
  printf("my id is %d\n", my_id);
}

void *worker(void *temp){
  int my_id = *((int *)temp);

  // NOTE: the lock isn't strictly necessary in *this* program because
  // pthread_setspecific is "MT-safe", meaning that it has synchronization
  // to make sure that there is no race within it.  However, in your myMalloc
  // program, you will not know the id to be assigned to each thread ahead of
  // time, so you will need a lock to make sure that you do not pass the same value
  // to pthread_setspecific for two different threads

  pthread_mutex_lock(&idLock);
  // setspecific call---store id for this thread 
  pthread_setspecific(key, (const void *)(&IDarray[my_id]));
  pthread_mutex_unlock(&idLock);
  
  print();

  return NULL;
}

void parallel(int p){
  int i, *params;
  pthread_t *pid_list;
  pthread_mutex_init(&idLock, NULL);

  // this is the initialization for key
  pthread_key_create(&key, NULL);

  params = (int *) malloc(p * sizeof(int));
  for (i = 0; i < p; i++)
    params[i] = i;

  pid_list = (pthread_t *)malloc(p*sizeof(pthread_t));
  for(i=0;i<p;i++){
    pthread_create(&pid_list[i],NULL,worker,(void *)(&params[i]));
  }
  for(i=0;i<p;i++){
    pthread_join(pid_list[i],NULL);
  }
  return;
}

int main(int argc, char **argv){
  int pr = atoi(argv[1]);
  parallel(pr);
  return 0;
}

