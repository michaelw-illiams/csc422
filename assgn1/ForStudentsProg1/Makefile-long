seqsort:	seqColumnSort.o columnSortHelper.o driverColumnSort.o
	gcc -o seqsort seqColumnSort.o columnSortHelper.o driverColumnSort.o -lm -lpthread

parsort:	threadColumnSort.o columnSortHelper.o  driverColumnSort.o
	gcc -o parsort threadColumnSort.o columnSortHelper.o driverColumnSort.o -lm -lpthread
	
driverColumnSort.o:	driverColumnSort.c columnSort.h columnSortHelper.h
	gcc -c -O2 -std=c99 driverColumnSort.c

seqColumnSort.o:	seqColumnSort.c columnSortHelper.h
	gcc -c -O2 -std=c99 seqColumnSort.c

columnSortHelper.o:	columnSortHelper.c columnSortHelper.h 
	gcc -c -O2 -std=c99 columnSortHelper.c

threadColumnSort.o:	threadColumnSort.c columnSortHelper.h
	gcc -c -O2 -std=c99 threadColumnSort.c

clean:
	rm -f *.o seqsort parsort 
