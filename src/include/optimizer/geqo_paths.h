/*-------------------------------------------------------------------------
 *
 * geqo_paths.h--
 *	  prototypes for various subroutines in geqo_path.c
 *
 * Copyright (c) 1994, Regents of the University of California
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

/* contributed by:
   =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
   *  Martin Utesch				 * Institute of Automatic Control	   *
   =							 = University of Mining and Technology =
   *  utesch@aut.tu-freiberg.de  * Freiberg, Germany				   *
   =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
 */

#ifndef GEQO_PATHS_H
#define GEQO_PATHS_H


extern List *geqo_prune_rels(List * rel_list);
extern void geqo_rel_paths(Rel * rel);

#endif							/* GEQO_PATHS_H */
