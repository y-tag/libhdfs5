#include "hdfs.h"

#include <stdlib.h>
#include <errno.h>

#include "libhdfs5.h"

hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld) {
  int err = 0;
  hdfsFS ret = hdfs5BuilderConnect(bld, &err);
  if (err != 0) {
    errno = err;
    return NULL;
  }
  return ret;
}

struct hdfsBuilder *hdfsNewBuilder(void) {
  int err = 0;
  struct hdfsBuilder *ret = hdfs5NewBuilder(&err);
  if (err != 0) {
    errno = err;
    return NULL;
  }
  return ret;
}

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn) {
  hdfs5BuilderSetNameNode(bld, (char*)nn);
}

void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName) {
  hdfs5BuilderSetUserName(bld, (char*)userName);
}

void hdfsFreeBuilder(struct hdfsBuilder *bld) {
  hdfs5FreeBuilder(bld);
}

int hdfsConfGetStr(const char *key, char **val) {
  if (key == NULL) {
    return -1;
  }

  *val = hdfs5ConfGetStr((char*)key);
  return 0;
}

int hdfsDisconnect(hdfsFS fs) {
  int ret = hdfs5Disconnect(fs);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize,
                      short replication, tSize blocksize) {
  int err = 0;
  hdfsFile f = hdfs5OpenFile(fs, (char*)path, flags, bufferSize, replication, blocksize, &err);
  if (err != 0) {
    errno = err;
  }
  return f;
}

int hdfsCloseFile(hdfsFS fs, hdfsFile file) {
  int ret = hdfs5CloseFile(fs, file);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsExists(hdfsFS fs, const char *path) {
  int ret = hdfs5Exists(fs, (char*)path);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos) {
  int ret = hdfs5Seek(fs, file, desiredPos);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

tOffset hdfsTell(hdfsFS fs, hdfsFile file) {
  int err = 0;
  tOffset ret = hdfs5Tell(fs, file, &err);
  if (err != 0) {
    errno = err;
    return -1;
  }
  return ret;
}

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length) {
  return hdfsPread(fs, file, 0, buffer, length);
}

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void *buffer, tSize length) {
  int err = 0;
  tSize ret = hdfs5Pread(fs, file, position, buffer, length, &err);
  if (err != 0) {
    errno = err;
    return -1;
  }
  return ret;
}

tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void *buffer, tSize length) {
  int err = 0;
  tSize ret =  hdfs5Write(fs, file, (void*)buffer, length, &err);
  if (err != 0) {
    errno = err;
    return -1;
  }
  return ret;
}

int hdfsFlush(hdfsFS fs, hdfsFile file) {
  int ret = hdfs5Flush(fs, file);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsHFlush(hdfsFS fs, hdfsFile file) {
  int ret = hdfs5HFlush(fs, file);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsHSync(hdfsFS fs, hdfsFile file) {
  int ret = hdfs5HSync(fs, file);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsDelete(hdfsFS fs, const char *path, int recursive) {
  int ret = hdfs5Delete(fs, (char*)path, recursive);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsRename(hdfsFS fs, const char *oldPath, const char *newPath) {
  int ret = hdfs5Rename(fs, (char*)oldPath, (char*)newPath);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsCreateDirectory(hdfsFS fs, const char *path) {
  int ret = hdfs5CreateDirectory(fs, (char*)path);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char *path, int *numEntries) {
  int err = 0;
  hdfsFileInfo *ret = hdfs5ListDirectory(fs, (char*)path, numEntries, &err);
  if (err != 0) {
    errno = err;
    return NULL;
  }
  return ret;
}

hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char *path) {
  int err = 0;
  hdfsFileInfo *ret = hdfs5GetPathInfo(fs, (char*)path, &err);
  if (err != 0) {
    errno = err;
    return NULL;
  }
  return ret;
}

void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries) {
  for (int i = 0; i < numEntries; ++i) {
    free(hdfsFileInfo[i].mName);
    free(hdfsFileInfo[i].mOwner);
    free(hdfsFileInfo[i].mGroup);
  }
  free(hdfsFileInfo);
}

tOffset hdfsGetCapacity(hdfsFS fs) {
  int err = 0;
  tOffset ret = hdfs5GetCapacity(fs, &err);
  if (err != 0) {
    errno = err;
    return -1;
  }
  return ret;
}

tOffset hdfsGetUsed(hdfsFS fs) {
  int err = 0;
  tOffset ret = hdfs5GetUsed(fs, &err);
  if (err != 0) {
    errno = err;
    return -1;
  }
  return ret;
}

int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group) {
  int ret = hdfs5Chown(fs, (char*)path, (char*)owner, (char*)group);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsChmod(hdfsFS fs, const char* path, short mode) {
  int ret = hdfs5Chmod(fs, (char*)path, mode);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime) {
  int ret = hdfs5Utime(fs, (char*)path, mtime, atime);
  if (ret != 0) {
    errno = ret;
    return -1;
  }
  return 0;
}
