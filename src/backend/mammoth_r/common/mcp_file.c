/*-----------------------
 * mcp_file.c
 * 		MCP File implementation.
 *
 * This is just an abstraction over <unistd.h> calls for the MCPQueue
 * routines.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_file.c 2159 2009-05-29 01:08:51Z alvherre $
 * ------------------------
 */
#include "postgres.h"

#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "mammoth_r/mcp_file.h"

/*
 * struct MCPFile
 * 		An opaque representation of a <unistd.h> file.
 */
struct MCPFile
{
	int		mf_fd;
	char   *mf_path;
};
/* typedef appears in mcp_file.h */

/*
 * MCPFileCreate
 *
 * Create a MCPFile struct, given a pathname.
 */
MCPFile *
MCPFileCreate(char *path)
{
	MCPFile	   *mcpf;

	Assert(path != NULL);

	mcpf = palloc(sizeof(MCPFile));

	mcpf->mf_fd = -1;
	mcpf->mf_path = pstrdup(path);

	return mcpf;
}

/*
 * MCPFileDestroy
 *
 * Destroy a given MCPFile struct, closing the underlying file if it's open.
 */
void
MCPFileDestroy(MCPFile *mcpf)
{
	Assert(mcpf != NULL);

	if (mcpf->mf_fd != -1 && close(mcpf->mf_fd) != 0)
			elog(WARNING, "could not close MCPFile %s: %m", mcpf->mf_path);

	pfree(mcpf->mf_path);
	pfree(mcpf);
}

/*
 * MCPFileGetPath
 *
 * Return MCPFile pathname.
 */
char *
MCPFileGetPath(MCPFile *mcpf)
{
	Assert(mcpf != NULL);

	return mcpf->mf_path;
}

/*
 * MCPFileCreateFile
 *
 * Attempt to create a new file.  Returns true if successful, false if the file
 * already exists, is a regular file and it belongs to the current user;
 * elog(ERROR) if anything else happens.  After this call, the file is left open.
 */
bool
MCPFileCreateFile(MCPFile *mcpf)
{
	int		fd;

	Assert(mcpf != NULL);
	Assert(mcpf->mf_path != NULL);

	fd = open(mcpf->mf_path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	if (fd >= 0)
	{
		/* the file was successfully created */
		mcpf->mf_fd = fd;

		return true;
	}

	/* failed to create the file, but why? */
	if (errno != EEXIST)
		elog(ERROR, "could not create file \"%s\": %m", mcpf->mf_path);

	return !MCPFileExists(mcpf);
}

/*
 * MCPFileExists
 * 		Verify whether a file exists and has acceptable permissions
 * 		(File remains open after call as a side effect)
 *
 * Return true if the file exists and is OK to read and write; return false if
 * it doesn't exist; error out if the file exists but is not readable for
 * whatever reason.
 */
bool
MCPFileExists(MCPFile *mcpf)
{
	int		ret;
	int		fd;
	struct stat statbuf;

	fd = open(mcpf->mf_path, O_RDWR, S_IRUSR | S_IWUSR);
	if (fd >= 0)
	{
		mcpf->mf_fd = fd;

		/* The file exists.  Check that it's an acceptable file. */
		ret = fstat(fd, &statbuf);
		if (ret < 0)
			elog(ERROR, "could not fstat \"%s\": %m", mcpf->mf_path);

		if (!S_ISREG(statbuf.st_mode))
			elog(ERROR, "%s is not a regular file",
				 mcpf->mf_path);

		if (statbuf.st_uid != getuid())
			elog(ERROR, "%s exists but is not owned by %d",
				 mcpf->mf_path, getuid());

		return true;
	}

	if (errno != ENOENT)
		elog(FATAL,
			 "could not open %s: %m", mcpf->mf_path);

	/* does not exist */
	return false;
}

/*
 * MCPFileOpen
 *
 * Open the underlying file associated with the given MCPFile.  It is an error
 * to attempt to open a file that doesn't exist -- use MCPFileCreateFile
 * beforehand.
 *
 * mf_errno and mf_errstr will be set to appropiate values in case of failure
 * (unless the failure is having passed a NULL MCPFile).
 *
 * Attempting to open an already open file is harmless.
 */
void
MCPFileOpen(MCPFile *mcpf)
{
	Assert(mcpf != NULL);
	Assert(mcpf->mf_path != NULL);

	if (mcpf->mf_fd != -1)
		return;

	mcpf->mf_fd = open(mcpf->mf_path, O_RDWR, S_IRUSR | S_IWUSR);

	if (mcpf->mf_fd == -1)
		elog(ERROR, "could not open MCPFile %s: %m", mcpf->mf_path);
}

/*
 * MCPFileClose
 *
 * Close the underlying file of a MCPFile.
 *
 * Closing a closed file is harmless.
 */
void
MCPFileClose(MCPFile *mcpf)
{
	int		ret;

	Assert(mcpf != NULL);
	Assert(mcpf->mf_path != NULL);

	if (mcpf->mf_fd == -1)
		return;

	ret = close(mcpf->mf_fd);

	mcpf->mf_fd = -1;

	if (ret != 0)
		elog(ERROR, "could not close MCPFile %s: %m", mcpf->mf_path);
}

/*
 * MCPFileRead
 *
 * Read "bytes" bytes from a MCPFile, writing them to the user-supplied "buf",
 * which must be at least "bytes" bytes long.
 *
 * Partial reads are not allowed --- if the underlying read() system call
 * returns less bytes than the caller wanted, an error will be raised.
 *
 * Returns true, unless EOF is found and eof_allowed is true.  If not
 * eof_allowed, then it raises an error on EOF.
 */
bool
MCPFileRead(MCPFile *mcpf, void *buf, size_t bytes, bool eof_allowed)
{
	ssize_t		ret;

	Assert(mcpf != NULL);

	if (mcpf->mf_fd == -1)
		elog(ERROR, "can't read from unopened MCPFile %s", mcpf->mf_path);

	ret = read(mcpf->mf_fd, buf, bytes);

	if (ret == (ssize_t) -1)
		elog(ERROR, "could not read from MCPFile %s: %m", mcpf->mf_path);
	if (ret == (ssize_t) 0)
	{
		off_t	oldpos;
		off_t	length;

		if (eof_allowed)
			return false;

		oldpos = lseek(mcpf->mf_fd, 0, SEEK_END);
		length = lseek(mcpf->mf_fd, oldpos, SEEK_SET);
		ereport(ERROR,
				(errmsg("EOF encountered while reading %d bytes",
						(uint32) bytes),
				 errcontext("MCPFile %s of length "UINT64_FORMAT", positioned at "UINT64_FORMAT,
							mcpf->mf_path, (uint64) length, (uint64) oldpos)));
	}
	if (ret < bytes)
		elog(ERROR,
			 "could not read requested bytes from MCPFile %s (wanted %d, got %d)",
			 mcpf->mf_path, (int32) bytes, (int32) ret);

	return true;
}

/*
 * MCPFileWrite
 *
 * Write "num" bytes to the given MCPFile, reading them from the user-supplied
 * "buf".
 *
 * A partial write is never acceptable, i.e. we will elog(ERROR) if the
 * underlying write() system call does not write all bytes.
 */
void
MCPFileWrite(MCPFile *mcpf, const void *buf, size_t num)
{
	ssize_t		ret;

	Assert(mcpf != NULL);
	Assert(num > 0);

	if (mcpf->mf_fd == -1)
		elog(ERROR, "can't write to unopened MCPFile %s", mcpf->mf_path);

	ret = write(mcpf->mf_fd, buf, num);

	if (ret == (ssize_t) -1)
		elog(ERROR, "could not write to MCPFile %s: %m", mcpf->mf_path);

	if (ret < num)
		elog(ERROR,
			 "could not write all bytes to MCPFile %s (wanted %d, wrote %d)",
			 mcpf->mf_path, (int32) num, (int32) ret);
}

/*
 * MCPFileSeek
 *
 * Change the current position of the read/write cursor in the given MCPFile.
 * whence values are taken from lseek() valid values.
 */
off_t
MCPFileSeek(MCPFile *mcpf, off_t offset, int whence)
{
	off_t	ret;

	Assert(mcpf != NULL);

	if (mcpf->mf_fd == -1)
		elog(ERROR, "can't seek in unopened MCPFile %s", mcpf->mf_path);

	ret = lseek(mcpf->mf_fd, offset, whence);

	if (ret == (off_t) -1)
		elog(ERROR, "could not seek to "UINT64_FORMAT" in MCPFile %s: %m",
			 (uint64) offset, mcpf->mf_path);

	return ret;
}

/*
 * MCPFileRename
 *		Rename MCPFile.
 *
 * NOTE: the original pathname is retained as attribute of the MCPFile, so if
 * the file is closed and reopened, it will open a file by the original name,
 * not the new one!  This is a curious API, but it's exactly what the callers
 * of this routine want.
 *
 * XXX: Windows part is missing
 */
void 
MCPFileRename(MCPFile *mcpf, char *mf_newpath)
{
	int		ret;

	Assert(mcpf != NULL);

	ret = rename(mcpf->mf_path, mf_newpath);
	
	if (ret == -1)
		elog(ERROR, "could not rename MCPFile %s to %s: %m", mcpf->mf_path,
					mf_newpath);
}	
	
/*
 * MCPFileUnlink
 *
 * Remove a link to an MCPFile.  The file is required to be closed.  (Thus most
 * likely the file itself will be removed, unless other process holds the file
 * open.)
 */
void
MCPFileUnlink(MCPFile *mcpf)
{
	int		ret;

	Assert(mcpf != NULL);

	if (mcpf->mf_fd != -1)
		elog(ERROR, "can't unlink open MCPFile %s", mcpf->mf_path);

	ret = unlink(mcpf->mf_path);

	if (ret == -1)
		elog(ERROR, "could not unlink MCPFile %s: %m", mcpf->mf_path);
}

/*
 * MCPFileTruncate
 *
 * Truncate the MCPFile to the given number of bytes.  The file must be
 * open.
 */
void
MCPFileTruncate(MCPFile *mcpf, off_t length)
{
	int		ret;

	Assert(mcpf != NULL);

	if (mcpf->mf_fd == -1)
		elog(ERROR, "can't truncate unopened file %s", mcpf->mf_path);

#ifndef WIN32
	ret = ftruncate(mcpf->mf_fd, length);
#else
	ret = _chsize(mcpf->mf_fd, length);
#endif

	if (ret == -1)
		elog(ERROR, "could not truncate MCPFile %s: %m", mcpf->mf_path);
}

/*
 * MCPFileCleanup
 *
 * Cleanup routine for the MCPFile module.
 */
void
MCPFileCleanup(void)
{
	/* nothing to do at present */
}
