driver:	driver.o myMalloc.o myMalloc-helper.o
	gcc -o driver driver.o myMalloc.o myMalloc-helper.o -lpthread

driver.o:	driver.c myMalloc.h
	gcc -g -c driver.c

myMalloc.o:	myMalloc.c myMalloc.h myMalloc-helper.h
	gcc -g -c myMalloc.c

myMalloc-helper.o:	myMalloc-helper.c myMalloc-helper.h
	gcc -g -c myMalloc-helper.c

mmTest: mmTest.o myMalloc.o myMalloc-helper.o
	gcc -o mmTest mmTest.o myMalloc.o myMalloc-helper.o -lpthread

mmTest.o: mmTest.c myMalloc.h myMalloc-helper.h
	gcc -g -c mmTest.c

clean:
	rm -f *.o driver mmTest
