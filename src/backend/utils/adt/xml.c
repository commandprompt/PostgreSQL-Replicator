/*-------------------------------------------------------------------------
 *
 * xml.c
 *	  XML data type support.
 *
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

/*
 * Generally, XML type support is only available when libxml use was
 * configured during the build.  But even if that is not done, the
 * type and all the functions are available, but most of them will
 * fail.  For one thing, this avoids having to manage variant catalog
 * installations.  But it also has nice effects such as that you can
 * dump a database containing XML type data even if the server is not
 * linked with libxml.  Thus, make sure xml_out() works even if nothing
 * else does.
 */

#include "postgres.h"

#ifdef USE_LIBXML
#include <libxml/chvalid.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/uri.h>
#include <libxml/xmlerror.h>
#endif /* USE_LIBXML */

#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/xml.h"


#ifdef USE_LIBXML

#define PG_XML_DEFAULT_URI "dummy.xml"

static StringInfo xml_err_buf = NULL;

static void 	xml_init(void);
static void    *xml_palloc(size_t size);
static void    *xml_repalloc(void *ptr, size_t size);
static void 	xml_pfree(void *ptr);
static char    *xml_pstrdup(const char *string);
static void 	xml_ereport(int level, int sqlcode,
							const char *msg, void *ctxt);
static void 	xml_errorHandler(void *ctxt, const char *msg, ...);
static void 	xml_ereport_by_code(int level, int sqlcode,
									const char *msg, int errcode);
static xmlChar *xml_text2xmlChar(text *in);
static xmlDocPtr xml_parse(text *data, int opts, bool is_document);

#endif /* USE_LIBXML */

#define NO_XML_SUPPORT() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("no XML support in this installation")))


Datum
xml_in(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	char		*s = PG_GETARG_CSTRING(0);
	size_t		len;
	xmltype		*vardata;

	len = strlen(s);
	vardata = palloc(len + VARHDRSZ);
	VARATT_SIZEP(vardata) = len + VARHDRSZ;
	memcpy(VARDATA(vardata), s, len);

	/*
	 * Parse the data to check if it is well-formed XML data.  Assume
	 * that ERROR occurred if parsing failed.  Do we need DTD
	 * validation (if DTD exists)?
	 */
	xml_parse(vardata, XML_PARSE_DTDATTR | XML_PARSE_DTDVALID, false);

	PG_RETURN_XML_P(vardata);
#else
	NO_XML_SUPPORT();
	return 0;
#endif
}


Datum
xml_out(PG_FUNCTION_ARGS)
{
	xmltype		*s = PG_GETARG_XML_P(0);
	char		*result;
	int32		len;

	len = VARSIZE(s) - VARHDRSZ;
	result = palloc(len + 1);
	memcpy(result, VARDATA(s), len);
	result[len] = '\0';

	PG_RETURN_CSTRING(result);
}


#ifdef USE_LIBXML
static void
appendStringInfoText(StringInfo str, const text *t)
{
	appendBinaryStringInfo(str, VARDATA(t), VARSIZE(t) - VARHDRSZ);
}


static xmltype *
stringinfo_to_xmltype(StringInfo buf)
{
	int32 len;
	xmltype *result;

	len = buf->len + VARHDRSZ;
	result = palloc(len);
	VARATT_SIZEP(result) = len;
	memcpy(VARDATA(result), buf->data, buf->len);

	return result;
}
#endif


Datum
xmlcomment(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	text *arg = PG_GETARG_TEXT_P(0);
	int len =  VARATT_SIZEP(arg) - VARHDRSZ;
	StringInfoData buf;
	int i;

	/* check for "--" in string or "-" at the end */
	for (i = 1; i < len; i++)
		if ((VARDATA(arg)[i] == '-' && VARDATA(arg)[i - 1] == '-')
			|| (VARDATA(arg)[i] == '-' && i == len - 1))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_XML_COMMENT),
							 errmsg("invalid XML comment")));

	initStringInfo(&buf);
	appendStringInfo(&buf, "<!--");
	appendStringInfoText(&buf, arg);
	appendStringInfo(&buf, "-->");

	PG_RETURN_XML_P(stringinfo_to_xmltype(&buf));
#else
	NO_XML_SUPPORT();
	return 0;
#endif
}


Datum
texttoxml(PG_FUNCTION_ARGS)
{
	text	   *data = PG_GETARG_TEXT_P(0);

	PG_RETURN_XML_P(xmlparse(data, false, true));
}


xmltype *
xmlparse(text *data, bool is_document, bool preserve_whitespace)
{
#ifdef USE_LIBXML
	if (!preserve_whitespace)
		ereport(WARNING,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("XMLPARSE with STRIP WHITESPACE is not implemented")));

	/*
	 * Note, that here we try to apply DTD defaults
	 * (XML_PARSE_DTDATTR) according to SQL/XML:10.16.7.d: 'Default
	 * valies defined by internal DTD are applied'.  As for external
	 * DTDs, we try to support them too, (see SQL/XML:10.16.7.e)
	 */
	xml_parse(data, XML_PARSE_DTDATTR, is_document);

	return (xmltype *) data;
#else
	NO_XML_SUPPORT();
	return NULL;
#endif
}


xmltype *
xmlpi(char *target, text *arg)
{
#ifdef USE_LIBXML
	xmltype *result;
	StringInfoData buf;

	if (pg_strncasecmp(target, "xml", 3) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION),
				 errmsg("invalid XML processing instruction"),
				 errdetail("XML processing instruction target name cannot start with \"xml\".")));

	initStringInfo(&buf);

	appendStringInfo(&buf, "<?%s", target);

	if (arg != NULL)
	{
		char *string;

		string = DatumGetCString(DirectFunctionCall1(textout,
													 PointerGetDatum(arg)));
		if (strstr(string, "?>") != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION),
				 errmsg("invalid XML processing instruction"),
				 errdetail("XML processing instruction cannot contain \"?>\".")));

		appendStringInfoChar(&buf, ' ');
		appendStringInfoString(&buf, string);
		pfree(string);
	}
	appendStringInfoString(&buf, "?>");

	result = stringinfo_to_xmltype(&buf);
	pfree(buf.data);
	return result;
#else
	NO_XML_SUPPORT();
	return NULL;
#endif
}


xmltype *
xmlroot(xmltype *data, text *version, int standalone)
{
#ifdef USE_LIBXML
	xmltype *result;
	StringInfoData buf;

	initStringInfo(&buf);

	/*
	 * FIXME: This is probably supposed to be cleverer if there
	 * already is an XML preamble.
	 */
	appendStringInfo(&buf,"<?xml");
	if (version)
	{
		appendStringInfo(&buf, " version=\"");
		appendStringInfoText(&buf, version);
		appendStringInfo(&buf, "\"");
	}
	if (standalone)
		appendStringInfo(&buf, " standalone=\"%s\"",
						 (standalone == 1 ? "yes" : "no"));
	appendStringInfo(&buf, "?>");
	appendStringInfoText(&buf, (text *) data);

	result = stringinfo_to_xmltype(&buf);
	pfree(buf.data);
	return result;
#else
	NO_XML_SUPPORT();
	return NULL;
#endif
}


/*
 * Validate document (given as string) against DTD (given as external link)
 * TODO !!! use text instead of cstring for second arg
 * TODO allow passing DTD as a string value (not only as an URI)
 * TODO redesign (see comment with '!!!' below)
 */
Datum
xmlvalidate(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	text				*data = PG_GETARG_TEXT_P(0);
	text				*dtdOrUri = PG_GETARG_TEXT_P(1);
	bool				result = false;
	xmlParserCtxtPtr	ctxt = NULL;
	xmlDocPtr 			doc = NULL;
	xmlDtdPtr			dtd = NULL;

	xml_init();

	/* We use a PG_TRY block to ensure libxml is cleaned up on error */
	PG_TRY();
	{
		ctxt = xmlNewParserCtxt();
		if (ctxt == NULL)
			xml_ereport(ERROR, ERRCODE_INTERNAL_ERROR,
						"could not allocate parser context", ctxt);

		doc = xmlCtxtReadMemory(ctxt, (char *) VARDATA(data),
								VARSIZE(data) - VARHDRSZ,
								PG_XML_DEFAULT_URI, NULL, 0);
		if (doc == NULL)
			xml_ereport(ERROR, ERRCODE_INVALID_XML_DOCUMENT,
						"could not parse XML data", ctxt);

#if 0
		uri = xmlCreateURI();
		elog(NOTICE, "dtd - %s", dtdOrUri);
		dtd = palloc(sizeof(xmlDtdPtr));
		uri = xmlParseURI(dtdOrUri);
		if (uri == NULL)
			xml_ereport(ERROR, ERRCODE_INTERNAL_ERROR,
						"not implemented yet... (TODO)", ctxt);
		else
#endif
			dtd = xmlParseDTD(NULL, xml_text2xmlChar(dtdOrUri));

		if (dtd == NULL)
			xml_ereport(ERROR, ERRCODE_INVALID_XML_DOCUMENT,
						"could not load DTD", ctxt);

		if (xmlValidateDtd(xmlNewValidCtxt(), doc, dtd) == 1)
			result = true;

		if (!result)
			xml_ereport(NOTICE, ERRCODE_INVALID_XML_DOCUMENT,
						"validation against DTD failed", ctxt);

#if 0
		if (uri)
			xmlFreeURI(uri);
#endif
		if (dtd)
			xmlFreeDtd(dtd);
		if (doc)
			xmlFreeDoc(doc);
		if (ctxt)
			xmlFreeParserCtxt(ctxt);
		xmlCleanupParser();
	}
	PG_CATCH();
	{
#if 0
		if (uri)
			xmlFreeURI(uri);
#endif
		if (dtd)
			xmlFreeDtd(dtd);
		if (doc)
			xmlFreeDoc(doc);
		if (ctxt)
			xmlFreeParserCtxt(ctxt);
		xmlCleanupParser();

		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_BOOL(result);
#else /* not USE_LIBXML */
	NO_XML_SUPPORT();
	return 0;
#endif /* not USE_LIBXML */
}


#ifdef USE_LIBXML

/*
 * Container for some init stuff (not good design!)
 * TODO xmlChar is utf8-char, make proper tuning (initdb with enc!=utf8 and check)
 */
static void
xml_init(void)
{
	/*
	 * Currently, we have no pure UTF-8 support for internals -- check
	 * if we can work.
	 */
	if (sizeof (char) != sizeof (xmlChar))
		ereport(ERROR,
				(errmsg("could not initialize XML library"),
				 errdetail("libxml2 has incompatible char type: sizeof(char)=%u, sizeof(xmlChar)=%u.",
						   (int) sizeof(char), (int) sizeof(xmlChar))));

	if (xml_err_buf == NULL)
	{
		/* First time through: create error buffer in permanent context */
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		xml_err_buf = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		/* Reset pre-existing buffer to empty */
		xml_err_buf->data[0] = '\0';
		xml_err_buf->len = 0;
	}
	/* Now that xml_err_buf exists, safe to call xml_errorHandler */
	xmlSetGenericErrorFunc(NULL, xml_errorHandler);

	xmlMemSetup(xml_pfree, xml_palloc, xml_repalloc, xml_pstrdup);
	xmlInitParser();
	LIBXML_TEST_VERSION;
}


/*
 * Convert a C string to XML internal representation
 * (same things as for TEXT, but with checking the data for well-formedness
 * and, moreover, validation against DTD, if needed).
 * NOTICE: We use TEXT type as internal storage type. In the future,
 * we plan to create own storage type (maybe several types/strategies)
 * TODO predefined DTDs / XSDs and validation
 * TODO validation against XML Schema
 * TODO maybe, libxml2's xmlreader is better? (do not construct DOM, yet do not use SAX - see xml_reader.c)
 * TODO what about internal URI for docs? (see PG_XML_DEFAULT_URI below)
 */
static xmlDocPtr
xml_parse(text *data, int opts, bool is_document)
{
	bool				validationFailed = false;
	int					res_code;
	int32				len;
	xmlChar				*string;
	xmlParserCtxtPtr 	ctxt = NULL;
	xmlDocPtr 			doc = NULL;
#ifdef XML_DEBUG_DTD_CONST
	xmlDtdPtr			dtd = NULL;
#endif

	len = VARSIZE(data) - VARHDRSZ; /* will be useful later */
	string = xml_text2xmlChar(data);

	xml_init();

	/* We use a PG_TRY block to ensure libxml is cleaned up on error */
	PG_TRY();
	{
		ctxt = xmlNewParserCtxt();
		if (ctxt == NULL)
			xml_ereport(ERROR, ERRCODE_INTERNAL_ERROR,
						"could not allocate parser context", ctxt);

		/* first, we try to parse the string as XML doc, then, as XML chunk */
		if (len >= 5 && strncmp((char *) string, "<?xml", 5) == 0)
		{
			/* consider it as DOCUMENT */
			doc = xmlCtxtReadMemory(ctxt, (char *) string, len,
									PG_XML_DEFAULT_URI, NULL, opts);
			if (doc == NULL)
				xml_ereport(ERROR, ERRCODE_INVALID_XML_DOCUMENT,
							"could not parse XML data", ctxt);
		}
		else
		{
			/* attempt to parse the string as if it is an XML fragment */
			doc = xmlNewDoc(NULL);
			/* TODO resolve: xmlParseBalancedChunkMemory assumes that string is UTF8 encoded! */
			res_code = xmlParseBalancedChunkMemory(doc, NULL, NULL, 0, string, NULL);
			if (res_code != 0)
				xml_ereport_by_code(ERROR, ERRCODE_INVALID_XML_DOCUMENT,
									"could not parse XML data", res_code);
		}

#ifdef XML_DEBUG_DTD_CONST
		dtd = xmlParseDTD(NULL, (xmlChar *) XML_DEBUG_DTD_CONST);
		if (dtd == NULL)
			xml_ereport(ERROR, ERRCODE_INVALID_XML_DOCUMENT,
						"could not parse DTD data", ctxt);
		if (xmlValidateDtd(xmlNewValidCtxt(), doc, dtd) != 1)
			validationFailed = true;
#else
		/* if dtd for our xml data is detected... */
		if ((doc->intSubset != NULL) || (doc->extSubset != NULL))
		{
			/* assume inline DTD exists - validation should be performed */
			if (ctxt->valid == 0)
			{
				/* DTD exists, but validator reported 'validation failed' */
				validationFailed = true;
			}
		}
#endif

		if (validationFailed)
			xml_ereport(WARNING, ERRCODE_INVALID_XML_DOCUMENT,
						"validation against DTD failed", ctxt);

		/* TODO encoding issues
		 * (thoughts:
		 * 		CASE:
		 *   		- XML data has explicit encoding attribute in its prolog
		 *   		- if not, assume that enc. of XML data is the same as client's one
		 *
		 * 		The common rule is to accept the XML data only if its encoding
		 * 		is the same as encoding of the storage (server's). The other possible
		 * 		option is to accept all the docs, but DO TRANSFORMATION and, if needed,
		 * 		change the prolog.
		 *
		 * 		I think I'd stick the first way (for the 1st version),
		 * 		it's much simplier (less errors...)
		 * ) */
		/* ... */

#ifdef XML_DEBUG_DTD_CONST
		if (dtd)
			xmlFreeDtd(dtd);
#endif
		if (doc)
			xmlFreeDoc(doc);
		if (ctxt)
			xmlFreeParserCtxt(ctxt);
		xmlCleanupParser();
	}
	PG_CATCH();
	{
#ifdef XML_DEBUG_DTD_CONST
		if (dtd)
			xmlFreeDtd(dtd);
#endif
		if (doc)
			xmlFreeDoc(doc);
		if (ctxt)
			xmlFreeParserCtxt(ctxt);
		xmlCleanupParser();

		PG_RE_THROW();
	}
	PG_END_TRY();

	return doc;
}


/*
 * xmlChar<->text convertions
 */
static xmlChar *
xml_text2xmlChar(text *in)
{
	int32 		len = VARSIZE(in) - VARHDRSZ;
	xmlChar		*res;

	res = palloc(len + 1);
	memcpy(res, VARDATA(in), len);
	res[len] = '\0';

	return(res);
}


/*
 * Wrappers for memory management functions
 */
static void *
xml_palloc(size_t size)
{
	return palloc(size);
}


static void *
xml_repalloc(void *ptr, size_t size)
{
	return repalloc(ptr, size);
}


static void
xml_pfree(void *ptr)
{
	pfree(ptr);
}


static char *
xml_pstrdup(const char *string)
{
	return pstrdup(string);
}


/*
 * Wrapper for "ereport" function.
 * Adds detail - libxml's native error message, if any.
 */
static void
xml_ereport(int level, int sqlcode,
			const char *msg, void *ctxt)
{
	xmlErrorPtr libxmlErr = NULL;

	if (xml_err_buf->len > 0)
	{
		ereport(DEBUG1,
				(errmsg("%s", xml_err_buf->data)));
		xml_err_buf->data[0] = '\0';
		xml_err_buf->len = 0;
	}

	if (ctxt != NULL)
		libxmlErr = xmlCtxtGetLastError(ctxt);

	if (libxmlErr == NULL)
	{
		ereport(level,
				(errcode(sqlcode),
				 errmsg("%s", msg)));
	}
	else
	{
		/* as usual, libxml error message contains '\n'; get rid of it */
		char *xmlErrDetail;
		int xmlErrLen, i;

		xmlErrDetail = pstrdup(libxmlErr->message);
		xmlErrLen = strlen(xmlErrDetail);
		for (i = 0; i < xmlErrLen; i++)
		{
			if (xmlErrDetail[i] == '\n')
				xmlErrDetail[i] = '.';
		}
		ereport(level,
				(errcode(sqlcode),
				 errmsg("%s", msg),
				 errdetail("%s", xmlErrDetail)));
	}
}


/*
 * Error handler for libxml error messages
 */
static void
xml_errorHandler(void *ctxt, const char *msg,...)
{
	/* Append the formatted text to xml_err_buf */
	for (;;)
	{
		va_list		args;
		bool		success;

		/* Try to format the data. */
		va_start(args, msg);
		success = appendStringInfoVA(xml_err_buf, msg, args);
		va_end(args);

		if (success)
			break;

		/* Double the buffer size and try again. */
		enlargeStringInfo(xml_err_buf, xml_err_buf->maxlen);
	}
}


/*
 * Return error message by libxml error code
 * TODO make them closer to recommendations from Postgres manual
 */
static void
xml_ereport_by_code(int level, int sqlcode,
					const char *msg, int code)
{
    const char *det;

	if (xml_err_buf->len > 0)
	{
		ereport(DEBUG1,
				(errmsg("%s", xml_err_buf->data)));
		xml_err_buf->data[0] = '\0';
		xml_err_buf->len = 0;
	}

    switch (code)
	{
        case XML_ERR_INTERNAL_ERROR:
            det = "libxml internal error";
            break;
        case XML_ERR_ENTITY_LOOP:
            det = "Detected an entity reference loop";
            break;
        case XML_ERR_ENTITY_NOT_STARTED:
            det = "EntityValue: \" or ' expected";
            break;
        case XML_ERR_ENTITY_NOT_FINISHED:
            det = "EntityValue: \" or ' expected";
            break;
        case XML_ERR_ATTRIBUTE_NOT_STARTED:
            det = "AttValue: \" or ' expected";
            break;
        case XML_ERR_LT_IN_ATTRIBUTE:
            det = "Unescaped '<' not allowed in attributes values";
            break;
        case XML_ERR_LITERAL_NOT_STARTED:
            det = "SystemLiteral \" or ' expected";
            break;
        case XML_ERR_LITERAL_NOT_FINISHED:
            det = "Unfinished System or Public ID \" or ' expected";
            break;
        case XML_ERR_MISPLACED_CDATA_END:
            det = "Sequence ']]>' not allowed in content";
            break;
        case XML_ERR_URI_REQUIRED:
            det = "SYSTEM or PUBLIC, the URI is missing";
            break;
        case XML_ERR_PUBID_REQUIRED:
            det = "PUBLIC, the Public Identifier is missing";
            break;
        case XML_ERR_HYPHEN_IN_COMMENT:
            det = "Comment must not contain '--' (double-hyphen)";
            break;
        case XML_ERR_PI_NOT_STARTED:
            det = "xmlParsePI : no target name";
            break;
        case XML_ERR_RESERVED_XML_NAME:
            det = "Invalid PI name";
            break;
        case XML_ERR_NOTATION_NOT_STARTED:
            det = "NOTATION: Name expected here";
            break;
        case XML_ERR_NOTATION_NOT_FINISHED:
            det = "'>' required to close NOTATION declaration";
            break;
        case XML_ERR_VALUE_REQUIRED:
            det = "Entity value required";
            break;
        case XML_ERR_URI_FRAGMENT:
            det = "Fragment not allowed";
            break;
        case XML_ERR_ATTLIST_NOT_STARTED:
            det = "'(' required to start ATTLIST enumeration";
            break;
        case XML_ERR_NMTOKEN_REQUIRED:
            det = "NmToken expected in ATTLIST enumeration";
            break;
        case XML_ERR_ATTLIST_NOT_FINISHED:
            det = "')' required to finish ATTLIST enumeration";
            break;
        case XML_ERR_MIXED_NOT_STARTED:
            det = "MixedContentDecl : '|' or ')*' expected";
            break;
        case XML_ERR_PCDATA_REQUIRED:
            det = "MixedContentDecl : '#PCDATA' expected";
            break;
        case XML_ERR_ELEMCONTENT_NOT_STARTED:
            det = "ContentDecl : Name or '(' expected";
            break;
        case XML_ERR_ELEMCONTENT_NOT_FINISHED:
            det = "ContentDecl : ',' '|' or ')' expected";
            break;
        case XML_ERR_PEREF_IN_INT_SUBSET:
            det = "PEReference: forbidden within markup decl in internal subset";
            break;
        case XML_ERR_GT_REQUIRED:
            det = "Expected '>'";
            break;
        case XML_ERR_CONDSEC_INVALID:
            det = "XML conditional section '[' expected";
            break;
        case XML_ERR_EXT_SUBSET_NOT_FINISHED:
            det = "Content error in the external subset";
            break;
        case XML_ERR_CONDSEC_INVALID_KEYWORD:
            det = "conditional section INCLUDE or IGNORE keyword expected";
            break;
        case XML_ERR_CONDSEC_NOT_FINISHED:
            det = "XML conditional section not closed";
            break;
        case XML_ERR_XMLDECL_NOT_STARTED:
            det = "Text declaration '<?xml' required";
            break;
        case XML_ERR_XMLDECL_NOT_FINISHED:
            det = "parsing XML declaration: '?>' expected";
            break;
        case XML_ERR_EXT_ENTITY_STANDALONE:
            det = "external parsed entities cannot be standalone";
            break;
        case XML_ERR_ENTITYREF_SEMICOL_MISSING:
            det = "EntityRef: expecting ';'";
            break;
        case XML_ERR_DOCTYPE_NOT_FINISHED:
            det = "DOCTYPE improperly terminated";
            break;
        case XML_ERR_LTSLASH_REQUIRED:
            det = "EndTag: '</' not found";
            break;
        case XML_ERR_EQUAL_REQUIRED:
            det = "Expected '='";
            break;
        case XML_ERR_STRING_NOT_CLOSED:
            det = "String not closed expecting \" or '";
            break;
        case XML_ERR_STRING_NOT_STARTED:
            det = "String not started expecting ' or \"";
            break;
        case XML_ERR_ENCODING_NAME:
            det = "Invalid XML encoding name";
            break;
        case XML_ERR_STANDALONE_VALUE:
            det = "Standalone accepts only 'yes' or 'no'";
            break;
        case XML_ERR_DOCUMENT_EMPTY:
            det = "Document is empty";
            break;
        case XML_ERR_DOCUMENT_END:
            det = "Extra content at the end of the document";
            break;
        case XML_ERR_NOT_WELL_BALANCED:
            det = "Chunk is not well balanced";
            break;
        case XML_ERR_EXTRA_CONTENT:
            det = "Extra content at the end of well balanced chunk";
            break;
        case XML_ERR_VERSION_MISSING:
            det = "Malformed declaration expecting version";
            break;
        /* more err codes... Please, keep the order! */
        case XML_ERR_ATTRIBUTE_WITHOUT_VALUE: /* 41 */
        	det ="Attribute without value";
        	break;
        case XML_ERR_ATTRIBUTE_REDEFINED:
        	det ="Attribute defined more than once in the same element";
        	break;
        case XML_ERR_COMMENT_NOT_FINISHED: /* 45 */
            det = "Comment is not finished";
            break;
        case XML_ERR_NAME_REQUIRED: /* 68 */
            det = "Element name not found";
            break;
        case XML_ERR_TAG_NOT_FINISHED: /* 77 */
            det = "Closing tag not found";
            break;
        default:
            det = "Unrecognized libxml error code: %d";
			break;
	}

	ereport(level,
			(errcode(sqlcode),
			 errmsg("%s", msg),
			 errdetail(det, code)));
}


/*
 * Convert one char in the current server encoding to a Unicode
 * codepoint.
 */
static pg_wchar
sqlchar_to_unicode(char *s)
{
	char *utf8string;
	pg_wchar ret[2];			/* need space for trailing zero */

	utf8string = (char *) pg_do_encoding_conversion((unsigned char *) s,
													pg_mblen(s),
													GetDatabaseEncoding(),
													PG_UTF8);

	pg_encoding_mb2wchar_with_len(PG_UTF8, utf8string, ret, pg_mblen(s));

	return ret[0];
}


static bool
is_valid_xml_namefirst(pg_wchar c)
{
	/* (Letter | '_' | ':') */
	return (xmlIsBaseCharQ(c) || xmlIsIdeographicQ(c)
			|| c == '_' || c == ':');
}


static bool
is_valid_xml_namechar(pg_wchar c)
{
	/* Letter | Digit | '.' | '-' | '_' | ':' | CombiningChar | Extender */
	return (xmlIsBaseCharQ(c) || xmlIsIdeographicQ(c)
			|| xmlIsDigitQ(c)
			|| c == '.' || c == '-' || c == '_' || c == ':'
			|| xmlIsCombiningQ(c)
			|| xmlIsExtenderQ(c));
}
#endif /* USE_LIBXML */


/*
 * Map SQL identifier to XML name; see SQL/XML:2003 section 9.1.
 */
char *
map_sql_identifier_to_xml_name(char *ident, bool fully_escaped)
{
#ifdef USE_LIBXML
	StringInfoData buf;
	char *p;

	initStringInfo(&buf);

	for (p = ident; *p; p += pg_mblen(p))
	{
		if (*p == ':' && (p == ident || fully_escaped))
			appendStringInfo(&buf, "_x003A_");
		else if (*p == '_' && *(p+1) == 'x')
			appendStringInfo(&buf, "_x005F_");
		else if (fully_escaped && p == ident &&
				 pg_strncasecmp(p, "xml", 3) == 0)
		{
			if (*p == 'x')
				appendStringInfo(&buf, "_x0078_");
			else
				appendStringInfo(&buf, "_x0058_");
		}
		else
		{
			pg_wchar u = sqlchar_to_unicode(p);

			if ((p == ident)
				? !is_valid_xml_namefirst(u)
				: !is_valid_xml_namechar(u))
				appendStringInfo(&buf, "_x%04X_", (unsigned int) u);
			else
				appendBinaryStringInfo(&buf, p, pg_mblen(p));
		}
	}

	return buf.data;
#else /* not USE_LIBXML */
	NO_XML_SUPPORT();
	return NULL;
#endif /* not USE_LIBXML */
}