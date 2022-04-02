#ifndef _HDFS5_H_
#define _HDFS5_H_

#include <errno.h>  /* for EINTERNAL, etc. */
#include <fcntl.h>  /* for O_RDONLY, O_WRONLY */
#include <stdint.h> /* for uint64_t, etc. */
#include <time.h>   /* for time_t */

#ifdef __cplusplus
extern "C" {
#endif

struct hdfsBuilder;
typedef int32_t tSize;    /// size of data for read/write io ops
typedef time_t tTime;     /// time type in seconds
typedef int64_t tOffset;  /// offset within the file
typedef uint16_t tPort;   /// port
typedef enum tObjectKind {
  kObjectKindFile = 'F',
  kObjectKindDirectory = 'D',
} tObjectKind;

struct hdfs_internal;
typedef struct hdfs_internal *hdfsFS;

struct hdfsFile_internal;
typedef struct hdfsFile_internal *hdfsFile;

typedef struct {
  tObjectKind mKind;  /* file or directory */
  char *mName;        /* the name of the file */
  tTime mLastMod;     /* the last modification time for the file in seconds */
  tOffset mSize;      /* the size of the file in bytes */
  short mReplication; /* the count of replicas */
  tOffset mBlockSize; /* the block size for the file */
  char *mOwner;       /* the owner of the file */
  char *mGroup;       /* the group associated with the file */
  short mPermissions; /* the permissions associated with the file */
  tTime mLastAccess;  /* the last access time for the file in seconds */
} hdfsFileInfo;


hdfsFS hdfsBuilderConnect(struct hdfsBuilder *bld);

struct hdfsBuilder *hdfsNewBuilder(void);

void hdfsBuilderSetForceNewInstance(struct hdfsBuilder *bld);

void hdfsBuilderSetNameNode(struct hdfsBuilder *bld, const char *nn);

void hdfsBuilderSetNameNodePort(struct hdfsBuilder *bld, tPort port);

void hdfsBuilderSetUserName(struct hdfsBuilder *bld, const char *userName);

void hdfsBuilderSetKerbTicketCachePath(struct hdfsBuilder *bld,
							   const char *kerbTicketCachePath);

void hdfsFreeBuilder(struct hdfsBuilder *bld);

int hdfsBuilderConfSetStr(struct hdfsBuilder *bld, const char *key,
						  const char *val);

int hdfsConfGetStr(const char *key, char **val);

int hdfsDisconnect(hdfsFS fs);

hdfsFile hdfsOpenFile(hdfsFS fs, const char *path, int flags, int bufferSize,
                      short replication, tSize blocksize);

int hdfsCloseFile(hdfsFS fs, hdfsFile file);

int hdfsExists(hdfsFS fs, const char *path);

int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos); 

tOffset hdfsTell(hdfsFS fs, hdfsFile file);

tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length);

tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position, void *buffer,
                tSize length);

tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void *buffer, tSize length);

int hdfsFlush(hdfsFS fs, hdfsFile file);

int hdfsHFlush(hdfsFS fs, hdfsFile file);

int hdfsHSync(hdfsFS fs, hdfsFile file);

int hdfsAvailable(hdfsFS fs, hdfsFile file);

int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);

int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);

int hdfsDelete(hdfsFS fs, const char *path, int recursive);

int hdfsRename(hdfsFS fs, const char *oldPath, const char *newPath);

char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize);

int hdfsSetWorkingDirectory(hdfsFS fs, const char* path);

int hdfsCreateDirectory(hdfsFS fs, const char *path);

int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication);

hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char *path, int *numEntries);

hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char *path);

void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries);

char*** hdfsGetHosts(hdfsFS fs, const char* path, 
	 	tOffset start, tOffset length);

void hdfsFreeHosts(char ***blockHosts);

tOffset hdfsGetDefaultBlockSize(hdfsFS fs);

tOffset hdfsGetCapacity(hdfsFS fs);

tOffset hdfsGetUsed(hdfsFS fs);

int hdfsChown(hdfsFS fs, const char* path, const char *owner,
			  const char *group);

int hdfsChmod(hdfsFS fs, const char* path, short mode);

int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime);


#ifdef __cplusplus
}
#endif

#undef LIBHDFS_EXTERNAL
#endif  // _HDFS5_H_
