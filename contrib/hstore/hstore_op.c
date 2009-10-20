/*
 * $PostgreSQL
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/builtins.h"

#include "hstore.h"


static HEntry *
findkey(HStore *hs, char *key, int keylen)
{
	HEntry	   *StopLow = ARRPTR(hs);
	HEntry	   *StopHigh = StopLow + hs->size;
	HEntry	   *StopMiddle;
	int			difference;
	char	   *base = STRPTR(hs);

	while (StopLow < StopHigh)
	{
		StopMiddle = StopLow + (StopHigh - StopLow) / 2;

		if (StopMiddle->keylen == keylen)
			difference = strncmp(base + StopMiddle->pos, key, StopMiddle->keylen);
		else
			difference = (StopMiddle->keylen > keylen) ? 1 : -1;

		if (difference == 0)
			return StopMiddle;
		else if (difference < 0)
			StopLow = StopMiddle + 1;
		else
			StopHigh = StopMiddle;
	}

	return NULL;
}

PG_FUNCTION_INFO_V1(fetchval);
Datum		fetchval(PG_FUNCTION_ARGS);
Datum
fetchval(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	text	   *key = PG_GETARG_TEXT_P(1);
	HEntry	   *entry;
	text	   *out;

	if ((entry = findkey(hs, VARDATA(key), VARSIZE(key) - VARHDRSZ)) == NULL || entry->valisnull)
	{
		PG_FREE_IF_COPY(hs, 0);
		PG_FREE_IF_COPY(key, 1);
		PG_RETURN_NULL();
	}

	out = cstring_to_text_with_len(STRPTR(hs) + entry->pos + entry->keylen,
								   entry->vallen);

	PG_FREE_IF_COPY(hs, 0);
	PG_FREE_IF_COPY(key, 1);
	PG_RETURN_TEXT_P(out);
}

PG_FUNCTION_INFO_V1(exists);
Datum		exists(PG_FUNCTION_ARGS);
Datum
exists(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	text	   *key = PG_GETARG_TEXT_P(1);
	HEntry	   *entry;

	entry = findkey(hs, VARDATA(key), VARSIZE(key) - VARHDRSZ);

	PG_FREE_IF_COPY(hs, 0);
	PG_FREE_IF_COPY(key, 1);

	PG_RETURN_BOOL(entry);
}

PG_FUNCTION_INFO_V1(defined);
Datum		defined(PG_FUNCTION_ARGS);
Datum
defined(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	text	   *key = PG_GETARG_TEXT_P(1);
	HEntry	   *entry;
	bool		res;

	entry = findkey(hs, VARDATA(key), VARSIZE(key) - VARHDRSZ);

	res = (entry && !entry->valisnull) ? true : false;

	PG_FREE_IF_COPY(hs, 0);
	PG_FREE_IF_COPY(key, 1);

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(delete);
Datum		delete(PG_FUNCTION_ARGS);
Datum
delete(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	text	   *key = PG_GETARG_TEXT_P(1);
	HStore	   *out = palloc(VARSIZE(hs));
	char	   *ptrs,
			   *ptrd;
	HEntry	   *es,
			   *ed;

	SET_VARSIZE(out, VARSIZE(hs));
	out->size = hs->size;		/* temporary! */

	ptrs = STRPTR(hs);
	es = ARRPTR(hs);
	ptrd = STRPTR(out);
	ed = ARRPTR(out);

	while (es - ARRPTR(hs) < hs->size)
	{
		if (!(es->keylen == VARSIZE(key) - VARHDRSZ && strncmp(ptrs, VARDATA(key), es->keylen) == 0))
		{
			memcpy(ed, es, sizeof(HEntry));
			memcpy(ptrd, ptrs, es->keylen + ((es->valisnull) ? 0 : es->vallen));
			ed->pos = ptrd - STRPTR(out);
			ptrd += es->keylen + ((es->valisnull) ? 0 : es->vallen);
			ed++;
		}
		ptrs += es->keylen + ((es->valisnull) ? 0 : es->vallen);
		es++;
	}

	if (ed - ARRPTR(out) != out->size)
	{
		int			buflen = ptrd - STRPTR(out);

		ptrd = STRPTR(out);

		out->size = ed - ARRPTR(out);

		memmove(STRPTR(out), ptrd, buflen);
		SET_VARSIZE(out, CALCDATASIZE(out->size, buflen));
	}


	PG_FREE_IF_COPY(hs, 0);
	PG_FREE_IF_COPY(key, 1);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(hs_concat);
Datum		hs_concat(PG_FUNCTION_ARGS);
Datum
hs_concat(PG_FUNCTION_ARGS)
{
	HStore	   *s1 = PG_GETARG_HS(0);
	HStore	   *s2 = PG_GETARG_HS(1);
	HStore	   *out = palloc(VARSIZE(s1) + VARSIZE(s2));
	char	   *ps1,
			   *ps2,
			   *pd;
	HEntry	   *es1,
			   *es2,
			   *ed;

	SET_VARSIZE(out, VARSIZE(s1) + VARSIZE(s2));
	out->size = s1->size + s2->size;

	ps1 = STRPTR(s1);
	ps2 = STRPTR(s2);
	pd = STRPTR(out);
	es1 = ARRPTR(s1);
	es2 = ARRPTR(s2);
	ed = ARRPTR(out);

	while (es1 - ARRPTR(s1) < s1->size && es2 - ARRPTR(s2) < s2->size)
	{
		int			difference;

		if (es1->keylen == es2->keylen)
			difference = strncmp(ps1, ps2, es1->keylen);
		else
			difference = (es1->keylen > es2->keylen) ? 1 : -1;

		if (difference == 0)
		{
			memcpy(ed, es2, sizeof(HEntry));
			memcpy(pd, ps2, es2->keylen + ((es2->valisnull) ? 0 : es2->vallen));
			ed->pos = pd - STRPTR(out);
			pd += es2->keylen + ((es2->valisnull) ? 0 : es2->vallen);
			ed++;

			ps1 += es1->keylen + ((es1->valisnull) ? 0 : es1->vallen);
			es1++;
			ps2 += es2->keylen + ((es2->valisnull) ? 0 : es2->vallen);
			es2++;
		}
		else if (difference > 0)
		{
			memcpy(ed, es2, sizeof(HEntry));
			memcpy(pd, ps2, es2->keylen + ((es2->valisnull) ? 0 : es2->vallen));
			ed->pos = pd - STRPTR(out);
			pd += es2->keylen + ((es2->valisnull) ? 0 : es2->vallen);
			ed++;

			ps2 += es2->keylen + ((es2->valisnull) ? 0 : es2->vallen);
			es2++;
		}
		else
		{
			memcpy(ed, es1, sizeof(HEntry));
			memcpy(pd, ps1, es1->keylen + ((es1->valisnull) ? 0 : es1->vallen));
			ed->pos = pd - STRPTR(out);
			pd += es1->keylen + ((es1->valisnull) ? 0 : es1->vallen);
			ed++;

			ps1 += es1->keylen + ((es1->valisnull) ? 0 : es1->vallen);
			es1++;
		}
	}

	while (es1 - ARRPTR(s1) < s1->size)
	{
		memcpy(ed, es1, sizeof(HEntry));
		memcpy(pd, ps1, es1->keylen + ((es1->valisnull) ? 0 : es1->vallen));
		ed->pos = pd - STRPTR(out);
		pd += es1->keylen + ((es1->valisnull) ? 0 : es1->vallen);
		ed++;

		ps1 += es1->keylen + ((es1->valisnull) ? 0 : es1->vallen);
		es1++;
	}

	while (es2 - ARRPTR(s2) < s2->size)
	{
		memcpy(ed, es2, sizeof(HEntry));
		memcpy(pd, ps2, es2->keylen + ((es2->valisnull) ? 0 : es2->vallen));
		ed->pos = pd - STRPTR(out);
		pd += es2->keylen + ((es2->valisnull) ? 0 : es2->vallen);
		ed++;

		ps2 += es2->keylen + ((es2->valisnull) ? 0 : es2->vallen);
		es2++;
	}

	if (ed - ARRPTR(out) != out->size)
	{
		int			buflen = pd - STRPTR(out);

		pd = STRPTR(out);

		out->size = ed - ARRPTR(out);

		memmove(STRPTR(out), pd, buflen);
		SET_VARSIZE(out, CALCDATASIZE(out->size, buflen));
	}

	PG_FREE_IF_COPY(s1, 0);
	PG_FREE_IF_COPY(s2, 1);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(tconvert);
Datum		tconvert(PG_FUNCTION_ARGS);
Datum
tconvert(PG_FUNCTION_ARGS)
{
	text	   *key;
	text	   *val = NULL;
	int			len;
	HStore	   *out;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	key = PG_GETARG_TEXT_P(0);

	if (PG_ARGISNULL(1))
		len = CALCDATASIZE(1, VARSIZE(key));
	else
	{
		val = PG_GETARG_TEXT_P(1);
		len = CALCDATASIZE(1, VARSIZE(key) + VARSIZE(val) - 2 * VARHDRSZ);
	}
	out = palloc(len);
	SET_VARSIZE(out, len);
	out->size = 1;

	ARRPTR(out)->keylen = hstoreCheckKeyLen(VARSIZE(key) - VARHDRSZ);
	if (PG_ARGISNULL(1))
	{
		ARRPTR(out)->vallen = 0;
		ARRPTR(out)->valisnull = true;
	}
	else
	{
		ARRPTR(out)->vallen = hstoreCheckValLen(VARSIZE(val) - VARHDRSZ);
		ARRPTR(out)->valisnull = false;
	}
	ARRPTR(out)->pos = 0;

	memcpy(STRPTR(out), VARDATA(key), ARRPTR(out)->keylen);
	if (!PG_ARGISNULL(1))
	{
		memcpy(STRPTR(out) + ARRPTR(out)->keylen, VARDATA(val), ARRPTR(out)->vallen);
		PG_FREE_IF_COPY(val, 1);
	}

	PG_FREE_IF_COPY(key, 0);

	PG_RETURN_POINTER(out);
}

PG_FUNCTION_INFO_V1(akeys);
Datum		akeys(PG_FUNCTION_ARGS);
Datum
akeys(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	Datum	   *d;
	ArrayType  *a;
	HEntry	   *ptr = ARRPTR(hs);
	char	   *base = STRPTR(hs);

	d = (Datum *) palloc(sizeof(Datum) * (hs->size + 1));
	while (ptr - ARRPTR(hs) < hs->size)
	{
		text	   *item;

		item = cstring_to_text_with_len(base + ptr->pos, ptr->keylen);
		d[ptr - ARRPTR(hs)] = PointerGetDatum(item);
		ptr++;
	}

	a = construct_array(d,
						hs->size,
						TEXTOID,
						-1,
						false,
						'i');

	ptr = ARRPTR(hs);
	while (ptr - ARRPTR(hs) < hs->size)
	{
		pfree(DatumGetPointer(d[ptr - ARRPTR(hs)]));
		ptr++;
	}

	pfree(d);
	PG_FREE_IF_COPY(hs, 0);

	PG_RETURN_POINTER(a);
}

PG_FUNCTION_INFO_V1(avals);
Datum		avals(PG_FUNCTION_ARGS);
Datum
avals(PG_FUNCTION_ARGS)
{
	HStore	   *hs = PG_GETARG_HS(0);
	Datum	   *d;
	ArrayType  *a;
	HEntry	   *ptr = ARRPTR(hs);
	char	   *base = STRPTR(hs);

	d = (Datum *) palloc(sizeof(Datum) * (hs->size + 1));
	while (ptr - ARRPTR(hs) < hs->size)
	{
		text	   *item;

		item = cstring_to_text_with_len(base + ptr->pos + ptr->keylen,
										(ptr->valisnull) ? 0 : ptr->vallen);
		d[ptr - ARRPTR(hs)] = PointerGetDatum(item);
		ptr++;
	}

	a = construct_array(d,
						hs->size,
						TEXTOID,
						-1,
						false,
						'i');

	ptr = ARRPTR(hs);
	while (ptr - ARRPTR(hs) < hs->size)
	{
		pfree(DatumGetPointer(d[ptr - ARRPTR(hs)]));
		ptr++;
	}

	pfree(d);
	PG_FREE_IF_COPY(hs, 0);

	PG_RETURN_POINTER(a);
}

typedef struct
{
	HStore	   *hs;
	int			i;
} AKStore;

static void
setup_firstcall(FuncCallContext *funcctx, HStore *hs)
{
	MemoryContext oldcontext;
	AKStore    *st;

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	st = (AKStore *) palloc(sizeof(AKStore));
	st->i = 0;
	st->hs = (HStore *) palloc(VARSIZE(hs));
	memcpy(st->hs, hs, VARSIZE(hs));

	funcctx->user_fctx = (void *) st;
	MemoryContextSwitchTo(oldcontext);
}

PG_FUNCTION_INFO_V1(skeys);
Datum		skeys(PG_FUNCTION_ARGS);
Datum
skeys(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AKStore    *st;

	if (SRF_IS_FIRSTCALL())
	{
		HStore	   *hs = PG_GETARG_HS(0);

		funcctx = SRF_FIRSTCALL_INIT();
		setup_firstcall(funcctx, hs);
		PG_FREE_IF_COPY(hs, 0);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (AKStore *) funcctx->user_fctx;

	if (st->i < st->hs->size)
	{
		HEntry	   *ptr = &(ARRPTR(st->hs)[st->i]);
		text	   *item;

		item = cstring_to_text_with_len(STRPTR(st->hs) + ptr->pos,
										ptr->keylen);
		st->i++;

		SRF_RETURN_NEXT(funcctx, PointerGetDatum(item));
	}

	pfree(st->hs);
	pfree(st);

	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(svals);
Datum		svals(PG_FUNCTION_ARGS);
Datum
svals(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AKStore    *st;

	if (SRF_IS_FIRSTCALL())
	{
		HStore	   *hs = PG_GETARG_HS(0);

		funcctx = SRF_FIRSTCALL_INIT();
		setup_firstcall(funcctx, hs);
		PG_FREE_IF_COPY(hs, 0);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (AKStore *) funcctx->user_fctx;

	if (st->i < st->hs->size)
	{
		HEntry	   *ptr = &(ARRPTR(st->hs)[st->i]);

		if (ptr->valisnull)
		{
			ReturnSetInfo *rsi;

			st->i++;
			(funcctx)->call_cntr++;
			rsi = (ReturnSetInfo *) fcinfo->resultinfo;
			rsi->isDone = ExprMultipleResult;
			PG_RETURN_NULL();
		}
		else
		{
			text	   *item;

			item = cstring_to_text_with_len(STRPTR(st->hs) + ptr->pos + ptr->keylen,
											ptr->vallen);
			st->i++;

			SRF_RETURN_NEXT(funcctx, PointerGetDatum(item));
		}
	}

	pfree(st->hs);
	pfree(st);

	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(hs_contains);
Datum		hs_contains(PG_FUNCTION_ARGS);
Datum
hs_contains(PG_FUNCTION_ARGS)
{
	HStore	   *val = PG_GETARG_HS(0);
	HStore	   *tmpl = PG_GETARG_HS(1);
	bool		res = true;
	HEntry	   *te = ARRPTR(tmpl);
	char	   *vv = STRPTR(val);
	char	   *tv = STRPTR(tmpl);

	while (res && te - ARRPTR(tmpl) < tmpl->size)
	{
		HEntry	   *entry = findkey(val, tv + te->pos, te->keylen);

		if (entry)
		{
			if (te->valisnull || entry->valisnull)
			{
				if (!(te->valisnull && entry->valisnull))
					res = false;
			}
			else if (te->vallen != entry->vallen ||
					 strncmp(vv + entry->pos + entry->keylen,
							 tv + te->pos + te->keylen,
							 te->vallen))
				res = false;
		}
		else
			res = false;
		te++;
	}

	PG_FREE_IF_COPY(val, 0);
	PG_FREE_IF_COPY(tmpl, 1);

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(hs_contained);
Datum		hs_contained(PG_FUNCTION_ARGS);
Datum
hs_contained(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall2(
										hs_contains,
										PG_GETARG_DATUM(1),
										PG_GETARG_DATUM(0)
										));
}

PG_FUNCTION_INFO_V1(each);
Datum		each(PG_FUNCTION_ARGS);
Datum
each(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AKStore    *st;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;
		HStore	   *hs = PG_GETARG_HS(0);

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		st = (AKStore *) palloc(sizeof(AKStore));
		st->i = 0;
		st->hs = (HStore *) palloc(VARSIZE(hs));
		memcpy(st->hs, hs, VARSIZE(hs));
		funcctx->user_fctx = (void *) st;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		MemoryContextSwitchTo(oldcontext);
		PG_FREE_IF_COPY(hs, 0);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (AKStore *) funcctx->user_fctx;

	if (st->i < st->hs->size)
	{
		HEntry	   *ptr = &(ARRPTR(st->hs)[st->i]);
		Datum		res,
					dvalues[2];
		bool		nulls[2] = {false, false};
		text	   *item;
		HeapTuple	tuple;

		item = cstring_to_text_with_len(STRPTR(st->hs) + ptr->pos, ptr->keylen);
		dvalues[0] = PointerGetDatum(item);

		if (ptr->valisnull)
		{
			dvalues[1] = (Datum) 0;
			nulls[1] = true;
		}
		else
		{
			item = cstring_to_text_with_len(STRPTR(st->hs) + ptr->pos + ptr->keylen,
											ptr->vallen);
			dvalues[1] = PointerGetDatum(item);
		}
		st->i++;

		tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, dvalues, nulls);
		res = HeapTupleGetDatum(tuple);

		pfree(DatumGetPointer(dvalues[0]));
		if (!nulls[1])
			pfree(DatumGetPointer(dvalues[1]));

		SRF_RETURN_NEXT(funcctx, PointerGetDatum(res));
	}

	pfree(st->hs);
	pfree(st);

	SRF_RETURN_DONE(funcctx);
}
