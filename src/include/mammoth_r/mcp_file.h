/*-----------------------
 * mcp_file.h
 * 		MCP File header definitions
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_file.h 2093 2009-04-09 19:05:33Z alvherre $
 * ------------------------
 */
#ifndef MCP_FILE_H
#define MCP_FILE_H


/* struct declaration appears in mcp_file.c */
typedef struct MCPFile MCPFile;

extern MCPFile *MCPFileCreate(char *path);
extern void		MCPFileDestroy(MCPFile *mcpf);

extern bool		MCPFileCreateFile(MCPFile *mcpf);
extern bool		MCPFileExists(MCPFile *mcpf);
extern void		MCPFileOpen(MCPFile *mcpf);
extern void		MCPFileClose(MCPFile *mcpf);
extern char	   *MCPFileGetPath(MCPFile *mcpf);
extern bool		MCPFileRead(MCPFile *mcpf, void *buf, size_t bytes,
							bool eof_allowed);
extern void		MCPFileWrite(MCPFile *mcpf, const void *buf, size_t num);
extern off_t	MCPFileSeek(MCPFile *mcpf, off_t offset, int whence);
extern void		MCPFileUnlink(MCPFile *mcpf);
extern void		MCPFileRename(MCPFile *mcpf, char *mf_newpath);
extern void		MCPFileTruncate(MCPFile *mcpf, off_t length);

extern void		MCPFileCleanup(void);

#endif /* MCP_FILE_H */
