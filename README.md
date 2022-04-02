# libhdfs5

**A libhdfs compatible library written in Go**

Currently, some limited functions has been implemented.
The functions are listed in `hdfs.h`.


## Build

With Docker, simply run a build script.
```
$ ./build-in-docker.bash
```

If it succeeded, we got sligtly fat `libhdfs.so`.
```
$ ls -sh libhdfs.so 
15M libhdfs.so
```

This `libhdfs.so` has minimal dependency.
```
$ ldd libhdfs.so 
        linux-vdso.so.1 (0x00007fffeabfd000)
        libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f013d1c9000)
        /lib64/ld-linux-x86-64.so.2 (0x00007f013da86000)
```


## Why libhdfs"5"?

- In honer of libhdfs3, which is written in C/C++
- 5 is called "Go" in Japanese and libhdfs5 is mainly written in Go
