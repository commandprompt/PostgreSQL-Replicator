/*-------------------------------------------------------------------------
 *
 * geqo_gene.h--
 *	  genome representation in optimizer/geqo
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


#ifndef GEQO_GENE_H
#define GEQO_GENE_H


/* we presume that int instead of Relid
   is o.k. for Gene; so don't change it! */
typedef
int			Gene;

typedef struct Chromosome
{
	Gene	   *string;
	Cost		worth;
}			Chromosome;

typedef struct Pool
{
	Chromosome *data;
	int			size;
	int			string_length;
}			Pool;

#endif							/* GEQO_GENE_H */
