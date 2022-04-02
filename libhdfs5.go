package main

/*
#include "hdfs.h"

#include <stdlib.h>
#include <errno.h>

struct hdfsBuilder {
	size_t opts_id;
};

struct hdfs_internal {
	size_t client_id;
};

struct hdfsFile_internal {
	size_t reader_id;
	size_t writer_id;
};
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/user"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
	krb "gopkg.in/jcmturner/gokrb5.v7/client"
	"gopkg.in/jcmturner/gokrb5.v7/config"
	"gopkg.in/jcmturner/gokrb5.v7/credentials"
)

const (
	// EINTERNAL represents the code of unknown error.
	EINTERNAL C.int = 255
)

var (
	hadoopConf hadoopconf.HadoopConf
	optsMap    map[uintptr]*hdfs.ClientOptions
	clientMap  map[uintptr]*hdfs.Client
	readerMap  map[uintptr]*hdfs.FileReader
	writerMap  map[uintptr]*hdfs.FileWriter
	mux        sync.RWMutex
)

func init() {
	hadoopConf, _ = hadoopconf.LoadFromEnvironment()
	optsMap = make(map[uintptr]*hdfs.ClientOptions)
	clientMap = make(map[uintptr]*hdfs.Client)
	readerMap = make(map[uintptr]*hdfs.FileReader)
	writerMap = make(map[uintptr]*hdfs.FileWriter)
}

func getClientFromFS(fs C.hdfsFS) (*hdfs.Client, error) {
	if fs == nil {
		return nil, fmt.Errorf("fs is nil")
	}

	mux.RLock()
	defer mux.RUnlock()

	clientID := uintptr(fs.client_id)
	if client, ok := clientMap[clientID]; ok {
		return client, nil
	}
	return nil, fmt.Errorf("client not found in fs")
}

func getReaderFromFile(file C.hdfsFile) (*hdfs.FileReader, error) {
	mux.RLock()
	defer mux.RUnlock()

	readerID := uintptr(file.reader_id)
	if reader, ok := readerMap[readerID]; ok {
		return reader, nil
	}
	return nil, fmt.Errorf("reader not found in file")
}

func getWriterFromFile(file C.hdfsFile) (*hdfs.FileWriter, error) {
	mux.RLock()
	defer mux.RUnlock()

	writerID := uintptr(file.writer_id)
	if writer, ok := writerMap[writerID]; ok {
		return writer, nil
	}
	return nil, fmt.Errorf("writer not found in file")
}

func getErrnoFromErr(err error) C.int {

	switch {
	case err == nil:
		return 0
	case errors.Is(err, os.ErrInvalid):
		return C.EINVAL
	case errors.Is(err, os.ErrPermission):
		return C.EPERM
	case errors.Is(err, os.ErrExist):
		return C.EEXIST
	case errors.Is(err, os.ErrNotExist):
		return C.ENOENT
	}
	return EINTERNAL
}

//export hdfs5BuilderConnect
func hdfs5BuilderConnect(bld *C.struct_hdfsBuilder, errno *C.int) C.hdfsFS {
	optsID := uintptr(bld.opts_id)
	mux.RLock()
	opts, ok := optsMap[optsID]
	mux.RUnlock()
	if !ok {
		*errno = EINTERNAL
		return nil
	}

	// Update namenode addresses if needed.
	nns, err := getNamenodeAddresses(opts.Addresses)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return nil
	}
	opts.Addresses = nns

	client, err := hdfs.NewClient(*opts)
	if err != nil {
		log.Print("2")
		log.Print(err)
		*errno = getErrnoFromErr(err)
		return nil
	}

	size := C.uint64_t(C.sizeof_struct_hdfs_internal)
	fs := (C.hdfsFS)(C.calloc(1, size))
	if fs == nil {
		*errno = C.ENOMEM
		return nil
	}

	clientID := uintptr(unsafe.Pointer(client))
	mux.Lock()
	clientMap[clientID] = client
	mux.Unlock()
	fs.client_id = C.size_t(clientID)

	hdfs5FreeBuilder(bld)

	return fs
}

//export hdfs5NewBuilder
func hdfs5NewBuilder(errno *C.int) *C.struct_hdfsBuilder {
	mux.Lock()
	defer mux.Unlock()

	size := C.uint64_t(C.sizeof_struct_hdfsBuilder)
	bld := (*C.struct_hdfsBuilder)(C.calloc(1, size))
	if bld == nil {
		*errno = C.ENOMEM
		return nil
	}

	if opts, err := getClientOptions(); err == nil {
		optsID := uintptr(unsafe.Pointer(opts))
		optsMap[optsID] = opts
		bld.opts_id = C.size_t(optsID)
	}

	return bld
}

//export hdfs5BuilderSetNameNode
func hdfs5BuilderSetNameNode(bld *C.struct_hdfsBuilder, nn *C.char) {
	mux.Lock()
	defer mux.Unlock()

	if bld == nil {
		return
	}

	optsID := uintptr(bld.opts_id)
	if opts, ok := optsMap[optsID]; ok {
		nnString := C.GoString(nn)
		opts.Addresses = []string{nnString}
	}
}

//export hdfs5BuilderSetUserName
func hdfs5BuilderSetUserName(bld *C.struct_hdfsBuilder, userName *C.char) {
	mux.Lock()
	defer mux.Unlock()

	if bld == nil {
		return
	}

	optsID := uintptr(bld.opts_id)
	if opts, ok := optsMap[optsID]; ok {
		opts.User = C.GoString(userName)
	}
}

//export hdfs5FreeBuilder
func hdfs5FreeBuilder(bld *C.struct_hdfsBuilder) {
	if bld == nil {
		return
	}

	mux.Lock()
	defer mux.Unlock()

	optsID := uintptr(bld.opts_id)
	if _, ok := optsMap[optsID]; ok {
		delete(optsMap, optsID)
	}
	C.free(unsafe.Pointer(bld))
}

//export hdfs5ConfGetStr
func hdfs5ConfGetStr(key *C.char) *C.char {
	if key == nil {
		return nil
	}
	keyString := C.GoString(key)

	if val, ok := hadoopConf[keyString]; ok {
		return C.CString(val)
	}
	return nil
}

//export hdfs5Disconnect
func hdfs5Disconnect(fs C.hdfsFS) C.int {
	if fs == nil {
		return C.EBADF
	}

	mux.Lock()
	defer mux.Unlock()

	clientID := uintptr(fs.client_id)
	if client, ok := clientMap[clientID]; ok {
		client.Close()
		delete(clientMap, clientID)
		return 0
	}

	return -1
}

//export hdfs5OpenFile
func hdfs5OpenFile(fs C.hdfsFS, path *C.char, flags C.int, bufferSize C.int, replication C.short, blocksize C.tSize, errno *C.int) C.hdfsFile {
	client, err := getClientFromFS(fs)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return nil
	}

	mux.Lock()
	defer mux.Unlock()

	size := C.uint64_t(C.sizeof_struct_hdfsFile_internal)
	file := (C.hdfsFile)(C.calloc(1, size))
	if file == nil {
		*errno = C.ENOMEM
		return nil
	}
	file.reader_id = 0
	file.writer_id = 0

	pathString := C.GoString(path)
	switch int(flags) {
	case os.O_RDONLY:
		reader, err := client.Open(pathString)
		if err != nil {
			*errno = getErrnoFromErr(err)
			break
		}
		readerID := uintptr(unsafe.Pointer(reader))
		readerMap[readerID] = reader
		file.reader_id = C.size_t(readerID)
	case os.O_WRONLY:
		// This flags imply os.O_TRUNCAT
		if _, err := client.Stat(pathString); err == nil {
			// Remove this file, since it maybe alredy exist
			if err := client.Remove(pathString); err != nil {
				*errno = getErrnoFromErr(err)
				break
			}
		}

		writer, err := client.CreateFile(pathString, int(replication), int64(blocksize), os.FileMode(0644))
		if err != nil {
			*errno = getErrnoFromErr(err)
			break
		}
		writerID := uintptr(unsafe.Pointer(writer))
		writerMap[writerID] = writer
		file.writer_id = C.size_t(writerID)
	case os.O_WRONLY | os.O_APPEND:
		writer, err := client.Append(pathString)
		if err != nil {
			*errno = getErrnoFromErr(err)
			break
		}
		writerID := uintptr(unsafe.Pointer(writer))
		writerMap[writerID] = writer
		file.writer_id = C.size_t(writerID)
	case os.O_RDWR:
		*errno = C.ENOTSUP
		return nil
	default:
		*errno = C.EINVAL
		return nil
	}

	if file.reader_id == 0 && file.writer_id == 0 {
		C.free(unsafe.Pointer(file))
		return nil
	}
	return file
}

//export hdfs5CloseFile
func hdfs5CloseFile(_ C.hdfsFS, file C.hdfsFile) C.int {
	mux.Lock()
	defer mux.Unlock()

	if file == nil {
		return C.EBADF
	}

	readerID := uintptr(file.reader_id)
	if reader, ok := readerMap[readerID]; ok {
		reader.Close()
		delete(readerMap, readerID)
	}

	writerID := uintptr(file.writer_id)
	if writer, ok := writerMap[writerID]; ok {
		writer.Close()
		delete(writerMap, writerID)
	}

	C.free(unsafe.Pointer(file))
	return 0
}

//export hdfs5Exists
func hdfs5Exists(fs C.hdfsFS, path *C.char) C.int {
	client, err := getClientFromFS(fs)
	if err != nil {
		return getErrnoFromErr(err)
	}

	pathString := C.GoString(path)
	if _, err := client.Stat(pathString); err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

//export hdfs5Seek
func hdfs5Seek(_ C.hdfsFS, file C.hdfsFile, desiredPos C.tOffset) C.int {
	reader, err := getReaderFromFile(file)
	if err != nil {
		return C.EBADF
	}

	pos := int64(desiredPos)
	if _, err := reader.Seek(pos, io.SeekStart); err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

//export hdfs5Tell
func hdfs5Tell(_ C.hdfsFS, file C.hdfsFile, errno *C.int) C.tOffset {
	reader, err := getReaderFromFile(file)
	if err != nil {
		*errno = C.EBADF
		return -1
	}

	ret, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return -1
	}
	return C.tOffset(ret)
}

//export hdfs5Pread
func hdfs5Pread(_ C.hdfsFS, file C.hdfsFile, position C.tOffset, buffer unsafe.Pointer, length C.tSize, errno *C.int) C.tSize {
	reader, err := getReaderFromFile(file)
	if err != nil {
		*errno = C.EBADF
		return -1
	}

	var data []byte
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Data = uintptr(buffer)
	sh.Len = int(length)
	sh.Cap = int(length)

	n, err := reader.ReadAt(data, int64(position))
	if err != nil && !errors.Is(err, io.EOF) {
		*errno = getErrnoFromErr(err)
		return -1
	}

	return C.tSize(n)
}

//export hdfs5Write
func hdfs5Write(_ C.hdfsFS, file C.hdfsFile, buffer unsafe.Pointer, length C.tSize, errno *C.int) C.tSize {
	writer, err := getWriterFromFile(file)
	if err != nil {
		*errno = C.EBADF
		return -1
	}

	data := C.GoBytes(buffer, C.int(length))
	ret, err := writer.Write(data)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return -1
	}

	return C.tSize(ret)
}

//export hdfs5Flush
func hdfs5Flush(_ C.hdfsFS, file C.hdfsFile) C.int {
	writer, err := getWriterFromFile(file)
	if err != nil {
		return C.EBADF
	}

	if err := writer.Flush(); err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

//export hdfs5HFlush
func hdfs5HFlush(fs C.hdfsFS, file C.hdfsFile) C.int {
	return hdfs5Flush(fs, file)
}

//export hdfs5HSync
func hdfs5HSync(fs C.hdfsFS, file C.hdfsFile) C.int {
	return hdfs5HFlush(fs, file)
}

//export hdfs5Delete
func hdfs5Delete(fs C.hdfsFS, path *C.char, recursive C.int) C.int {
	client, err := getClientFromFS(fs)
	if err != nil {
		return C.EBADF
	}

	pathString := C.GoString(path)
	stat, err := client.Stat(pathString)
	if err != nil {
		return getErrnoFromErr(err)
	}

	if stat.IsDir() && recursive != 0 {
		err = client.RemoveAll(pathString)
	} else {
		err = client.Remove(pathString)
	}

	if err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

//export hdfs5Rename
func hdfs5Rename(fs C.hdfsFS, oldPath *C.char, newPath *C.char) C.int {
	client, err := getClientFromFS(fs)
	if err != nil {
		return getErrnoFromErr(err)
	}

	oldPathString := C.GoString(oldPath)
	newPathString := C.GoString(newPath)
	if err := client.Rename(oldPathString, newPathString); err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

//export hdfs5CreateDirectory
func hdfs5CreateDirectory(fs C.hdfsFS, path *C.char) C.int {
	client, err := getClientFromFS(fs)
	if err != nil {
		return getErrnoFromErr(err)
	}

	pathString := C.GoString(path)
	if err := client.MkdirAll(pathString, 0755); err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

//export hdfs5ListDirectory
func hdfs5ListDirectory(fs C.hdfsFS, path *C.char, numEntries *C.int, errno *C.int) *C.hdfsFileInfo {
	client, err := getClientFromFS(fs)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return nil
	}

	pathString := C.GoString(path)
	stats, err := client.ReadDir(pathString)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return nil
	}

	n := len(stats)
	*numEntries = C.int(n)
	if n == 0 {
		// Empty dir, but not error
		return nil
	}

	size := C.uint64_t(C.sizeof_hdfsFileInfo)
	fis := (*C.hdfsFileInfo)(C.calloc(C.uint64_t(n), size))
	if fis == nil {
		*errno = C.ENOMEM
		return nil
	}

	var fiSlice []C.hdfsFileInfo
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&fiSlice))
	sh.Data = uintptr(unsafe.Pointer(fis))
	sh.Len = n
	sh.Cap = n

	for i, stat := range stats {
		fi := &fiSlice[i]
		if stat.IsDir() {
			fi.mKind = C.kObjectKindDirectory
		} else {
			fi.mKind = C.kObjectKindFile
		}
		fi.mName = C.CString(stat.Name())
		fi.mLastMod = C.time_t(stat.ModTime().Unix())
		fi.mSize = C.tOffset(stat.Size())
		fi.mPermissions = C.short(stat.Mode())
		if hdfsFi, ok := stat.(*hdfs.FileInfo); ok {
			fi.mReplication = C.short(hdfsFi.BlockReplication())
			fi.mBlockSize = C.tOffset(hdfsFi.BlockSize())
			fi.mOwner = C.CString(hdfsFi.Owner())
			fi.mGroup = C.CString(hdfsFi.OwnerGroup())
			fi.mLastAccess = C.tTime(hdfsFi.AccessTime().Unix())
		}
	}

	return fis
}

//export hdfs5GetPathInfo
func hdfs5GetPathInfo(fs C.hdfsFS, path *C.char, errno *C.int) *C.hdfsFileInfo {
	client, err := getClientFromFS(fs)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return nil
	}

	pathString := C.GoString(path)
	stat, err := client.Stat(pathString)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return nil
	}

	size := C.uint64_t(C.sizeof_hdfsFileInfo)
	fi := (*C.hdfsFileInfo)(C.calloc(1, size))
	if fi == nil {
		*errno = C.ENOMEM
		return nil
	}

	if stat.IsDir() {
		fi.mKind = C.kObjectKindDirectory
	} else {
		fi.mKind = C.kObjectKindFile
	}
	fi.mName = C.CString(stat.Name())
	fi.mLastMod = C.tTime(stat.ModTime().Unix())
	fi.mSize = C.tOffset(stat.Size())
	fi.mPermissions = C.short(stat.Mode())
	if hdfsFi, ok := stat.(*hdfs.FileInfo); ok {
		fi.mReplication = C.short(hdfsFi.BlockReplication())
		fi.mBlockSize = C.tOffset(hdfsFi.BlockSize())
		fi.mOwner = C.CString(hdfsFi.Owner())
		fi.mGroup = C.CString(hdfsFi.OwnerGroup())
		fi.mLastAccess = C.tTime(hdfsFi.AccessTime().Unix())
	}

	return fi
}

//export hdfs5GetCapacity
func hdfs5GetCapacity(fs C.hdfsFS, errno *C.int) C.tOffset {
	client, err := getClientFromFS(fs)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return -1
	}

	fsStat, err := client.StatFs()
	if err != nil {
		*errno = getErrnoFromErr(err)
		return -1
	}

	return C.tOffset(fsStat.Capacity)
}

//export hdfs5GetUsed
func hdfs5GetUsed(fs C.hdfsFS, errno *C.int) C.tOffset {
	client, err := getClientFromFS(fs)
	if err != nil {
		*errno = getErrnoFromErr(err)
		return -1
	}

	fsStat, err := client.StatFs()
	if err != nil {
		*errno = getErrnoFromErr(err)
		return -1
	}

	return C.tOffset(fsStat.Used)
}

//export hdfs5Chown
func hdfs5Chown(fs C.hdfsFS, path *C.char, owner *C.char, group *C.char) C.int {
	client, err := getClientFromFS(fs)
	if err != nil {
		return getErrnoFromErr(err)
	}

	pathString := C.GoString(path)
	ownerString := C.GoString(owner)
	groupString := C.GoString(group)

	if err := client.Chown(pathString, ownerString, groupString); err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

//export hdfs5Chmod
func hdfs5Chmod(fs C.hdfsFS, path *C.char, mode C.short) C.int {
	client, err := getClientFromFS(fs)
	if err != nil {
		return getErrnoFromErr(err)
	}

	pathString := C.GoString(path)
	fileMode := os.FileMode(mode)
	if err := client.Chmod(pathString, fileMode); err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

//export hdfs5Utime
func hdfs5Utime(fs C.hdfsFS, path *C.char, mtime C.tTime, atime C.tTime) C.int {
	client, err := getClientFromFS(fs)
	if err != nil {
		return getErrnoFromErr(err)
	}

	pathString := C.GoString(path)
	aTime := time.Unix(int64(atime), 0)
	mTime := time.Unix(int64(mtime), 0)
	if err := client.Chtimes(pathString, aTime, mTime); err != nil {
		return getErrnoFromErr(err)
	}
	return 0
}

func getClientOptions() (*hdfs.ClientOptions, error) {
	options := hdfs.ClientOptionsFromConf(hadoopConf)

	if options.KerberosClient != nil {
		krbClient, err := getKerberosClient()
		if err != nil {
			return nil, err
		}
		options.KerberosClient = krbClient
	} else {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		options.User = user.Username
	}

	// Set some basic defaults.
	dialFunc := (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 5 * time.Second,
		DualStack: true,
	}).DialContext

	options.NamenodeDialFunc = dialFunc
	options.DatanodeDialFunc = dialFunc

	return &options, nil
}

func getKerberosClient() (*krb.Client, error) {
	configPath := os.Getenv("KRB5_CONFIG")
	if configPath == "" {
		configPath = "/etc/krb5.conf"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}

	ccachePath := os.Getenv("KRB5CCNAME")
	if ccachePath == "" {
		u, err := user.Current()
		if err != nil {
			return nil, err
		}
		ccachePath = fmt.Sprintf("/tmp/krb5cc_%s", u.Uid)
	}

	ccache, err := credentials.LoadCCache(ccachePath)
	if err != nil {
		return nil, err
	}

	client, err := krb.NewClientFromCCache(ccache, cfg)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getNamenodeAddresses(addresses []string) ([]string, error) {
	// namenode or nameservices
	nnOrNsName := "default"

	switch {
	// If multiple namenodes have been set, we just use them.
	case len(addresses) > 1:
		return addresses, nil
	case len(addresses) == 1:
		nnOrNsName = addresses[0]
	}

	ns2nnsMap := make(map[string][]string)
	for key, value := range hadoopConf {
		if strings.HasPrefix(key, "fs.default") {
			u, err := url.Parse(value)
			if nnOrNsName == "default" && err == nil {
				// This host may indicate a nameservice
				nnOrNsName = u.Host
			}
		} else if strings.HasPrefix(key, "dfs.namenode.rpc-address.") {
			tokens := strings.Split(key, ".")
			if len(tokens) <= 3 {
				continue
			}
			nsName := tokens[3]
			ns2nnsMap[nsName] = append(ns2nnsMap[nsName], value)
		}
	}

	if nns, ok := ns2nnsMap[nnOrNsName]; ok {
		sort.Strings(nns)
		return nns, nil
	}

	return []string{nnOrNsName}, nil
}

func main() {
}
