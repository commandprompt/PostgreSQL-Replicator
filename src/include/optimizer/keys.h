/*-------------------------------------------------------------------------
 *
 * keys.h--
 *	  prototypes for keys.c.
 *
 *
 * Copyright (c) 1994, Regents of the University of California
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#ifndef KEYS_H
#define KEYS_H

extern bool match_indexkey_operand(int indexkey, Var * operand, Rel * rel);
extern Var *extract_subkey(JoinKey * jk, int which_subkey);
extern bool samekeys(List * keys1, List * keys2);
extern List *collect_index_pathkeys(int *index_keys, List * tlist);

#endif							/* KEYS_H */
