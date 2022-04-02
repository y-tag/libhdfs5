SRCTOP = .

CC = gcc

INCLUDES += -I$(SRCTOP)
LIBS += 

CCFLAG = -std=c99 -O3 -Wno-unused-value -Wall

all:libhdfs.so

libhdfs.so: libhdfs5.c libhdfs5.a
	$(CC) -shared -fPIC -o $@ $^ $(CCFLAG) $(INCLUDES) $(LIBS)

libhdfs5.a: libhdfs5.go
	go vet $^
	gofmt -s -w $^
	goimports -w $^
	golangci-lint run $^
	go build -buildmode=c-archive -o $@ $^

clean:
	rm -f *~ *.a *.so libhdfs5.h
