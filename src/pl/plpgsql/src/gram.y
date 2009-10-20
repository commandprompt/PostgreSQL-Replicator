%{
/*-------------------------------------------------------------------------
 *
 * gram.y				- Parser for the PL/pgSQL
 *						  procedural language
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "plpgsql.h"

#include "catalog/pg_type.h"
#include "parser/parser.h"


/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree


static PLpgSQL_expr		*read_sql_construct(int until,
											int until2,
											int until3,
											const char *expected,
											const char *sqlstart,
											bool isexpression,
											bool valid_sql,
											int *endtoken);
static PLpgSQL_expr		*read_sql_expression2(int until, int until2,
											  const char *expected,
											  int *endtoken);
static	PLpgSQL_expr	*read_sql_stmt(const char *sqlstart);
static	PLpgSQL_type	*read_datatype(int tok);
static	PLpgSQL_stmt	*make_execsql_stmt(const char *sqlstart, int lineno);
static	PLpgSQL_stmt_fetch *read_fetch_direction(void);
static	PLpgSQL_stmt	*make_return_stmt(int lineno);
static	PLpgSQL_stmt	*make_return_next_stmt(int lineno);
static	PLpgSQL_stmt	*make_return_query_stmt(int lineno);
static  PLpgSQL_stmt 	*make_case(int lineno, PLpgSQL_expr *t_expr,
								   List *case_when_list, List *else_stmts);
static	void			 check_assignable(PLpgSQL_datum *datum);
static	void			 read_into_target(PLpgSQL_rec **rec, PLpgSQL_row **row,
										  bool *strict);
static	PLpgSQL_row		*read_into_scalar_list(const char *initial_name,
											   PLpgSQL_datum *initial_datum);
static PLpgSQL_row		*make_scalar_list1(const char *initial_name,
										   PLpgSQL_datum *initial_datum,
										   int lineno);
static	void			 check_sql_expr(const char *stmt);
static	void			 plpgsql_sql_error_callback(void *arg);
static	char			*parse_string_token(const char *token);
static	void			 plpgsql_string_error_callback(void *arg);
static	char			*check_label(const char *yytxt);
static	void			 check_labels(const char *start_label,
									  const char *end_label);
static PLpgSQL_expr 	*read_cursor_args(PLpgSQL_var *cursor,
										  int until, const char *expected);
static List				*read_raise_options(void);

%}

%expect 0
%name-prefix="plpgsql_yy"

%union {
		int32					ival;
		bool					boolean;
		char					*str;
		struct
		{
			char *name;
			int  lineno;
		}						varname;
		struct
		{
			char *name;
			int  lineno;
			PLpgSQL_datum   *scalar;
			PLpgSQL_rec     *rec;
			PLpgSQL_row     *row;
		}						forvariable;
		struct
		{
			char *label;
			int  n_initvars;
			int  *initvarnos;
		}						declhdr;
		struct
		{
			char *end_label;
			List *stmts;
		}						loop_body;
		List					*list;
		PLpgSQL_type			*dtype;
		PLpgSQL_datum			*scalar;	/* a VAR, RECFIELD, or TRIGARG */
		PLpgSQL_variable		*variable;	/* a VAR, REC, or ROW */
		PLpgSQL_var				*var;
		PLpgSQL_row				*row;
		PLpgSQL_rec				*rec;
		PLpgSQL_expr			*expr;
		PLpgSQL_stmt			*stmt;
		PLpgSQL_stmt_block		*program;
		PLpgSQL_condition		*condition;
		PLpgSQL_exception		*exception;
		PLpgSQL_exception_block	*exception_block;
		PLpgSQL_nsitem			*nsitem;
		PLpgSQL_diag_item		*diagitem;
		PLpgSQL_stmt_fetch		*fetch;
		PLpgSQL_case_when		*casewhen;
}

%type <declhdr> decl_sect
%type <varname> decl_varname
%type <str>		decl_renname
%type <boolean>	decl_const decl_notnull exit_type
%type <expr>	decl_defval decl_cursor_query
%type <dtype>	decl_datatype
%type <row>		decl_cursor_args
%type <list>	decl_cursor_arglist
%type <nsitem>	decl_aliasitem
%type <str>		decl_stmts decl_stmt

%type <expr>	expr_until_semi expr_until_rightbracket
%type <expr>	expr_until_then expr_until_loop opt_expr_until_when
%type <expr>	opt_exitcond

%type <ival>	assign_var
%type <var>		cursor_variable
%type <variable>	decl_cursor_arg
%type <forvariable>	for_variable
%type <stmt>	for_control

%type <str>		any_identifier any_name opt_block_label opt_label
%type <str>		execsql_start

%type <list>	proc_sect proc_stmts stmt_else
%type <loop_body>	loop_body
%type <stmt>	proc_stmt pl_block
%type <stmt>	stmt_assign stmt_if stmt_loop stmt_while stmt_exit
%type <stmt>	stmt_return stmt_raise stmt_execsql
%type <stmt>	stmt_dynexecute stmt_for stmt_perform stmt_getdiag
%type <stmt>	stmt_open stmt_fetch stmt_move stmt_close stmt_null
%type <stmt>	stmt_case

%type <list>	proc_exceptions
%type <exception_block> exception_sect
%type <exception>	proc_exception
%type <condition>	proc_conditions proc_condition

%type <casewhen>	case_when
%type <list>	case_when_list opt_case_else

%type <list>	getdiag_list
%type <diagitem> getdiag_list_item
%type <ival>	getdiag_kind getdiag_target

%type <ival>	opt_scrollable
%type <fetch>   opt_fetch_direction

%type <ival>	lno

		/*
		 * Keyword tokens
		 */
%token	K_ALIAS
%token	K_ASSIGN
%token	K_BEGIN
%token	K_BY
%token	K_CASE
%token	K_CLOSE
%token	K_CONSTANT
%token	K_CONTINUE
%token	K_CURSOR
%token	K_DECLARE
%token	K_DEFAULT
%token	K_DIAGNOSTICS
%token	K_DOTDOT
%token	K_ELSE
%token	K_ELSIF
%token	K_END
%token	K_EXCEPTION
%token	K_EXECUTE
%token	K_EXIT
%token	K_FOR
%token	K_FETCH
%token	K_FROM
%token	K_GET
%token	K_IF
%token	K_IN
%token	K_INSERT
%token	K_INTO
%token	K_IS
%token	K_LOOP
%token	K_MOVE
%token	K_NOSCROLL
%token	K_NOT
%token	K_NULL
%token	K_OPEN
%token	K_OR
%token	K_PERFORM
%token	K_ROW_COUNT
%token	K_RAISE
%token	K_RENAME
%token	K_RESULT_OID
%token	K_RETURN
%token	K_REVERSE
%token	K_SCROLL
%token	K_STRICT
%token	K_THEN
%token	K_TO
%token	K_TYPE
%token	K_USING
%token	K_WHEN
%token	K_WHILE

		/*
		 * Other tokens
		 */
%token	T_STRING
%token	T_NUMBER
%token	T_SCALAR				/* a VAR, RECFIELD, or TRIGARG */
%token	T_ROW
%token	T_RECORD
%token	T_DTYPE
%token	T_WORD
%token	T_ERROR

%token	O_OPTION
%token	O_DUMP

%%

pl_function		: comp_optsect pl_block opt_semi
					{
						yylval.program = (PLpgSQL_stmt_block *) $2;
					}
				;

comp_optsect	:
				| comp_options
				;

comp_options	: comp_options comp_option
				| comp_option
				;

comp_option		: O_OPTION O_DUMP
					{
						plpgsql_DumpExecTree = true;
					}
				;

opt_semi		:
				| ';'
				;

pl_block		: decl_sect K_BEGIN lno proc_sect exception_sect K_END opt_label
					{
						PLpgSQL_stmt_block *new;

						new = palloc0(sizeof(PLpgSQL_stmt_block));

						new->cmd_type	= PLPGSQL_STMT_BLOCK;
						new->lineno		= $3;
						new->label		= $1.label;
						new->n_initvars = $1.n_initvars;
						new->initvarnos = $1.initvarnos;
						new->body		= $4;
						new->exceptions	= $5;

						check_labels($1.label, $7);
						plpgsql_ns_pop();

						$$ = (PLpgSQL_stmt *)new;
					}
				;


decl_sect		: opt_block_label
					{
						plpgsql_ns_setlocal(false);
						$$.label	  = $1;
						$$.n_initvars = 0;
						$$.initvarnos = NULL;
					}
				| opt_block_label decl_start
					{
						plpgsql_ns_setlocal(false);
						$$.label	  = $1;
						$$.n_initvars = 0;
						$$.initvarnos = NULL;
					}
				| opt_block_label decl_start decl_stmts
					{
						plpgsql_ns_setlocal(false);
						if ($3 != NULL)
							$$.label = $3;
						else
							$$.label = $1;
						/* Remember variables declared in decl_stmts */
						$$.n_initvars = plpgsql_add_initdatums(&($$.initvarnos));
					}
				;

decl_start		: K_DECLARE
					{
						/* Forget any variables created before block */
						plpgsql_add_initdatums(NULL);
						/* Make variable names be local to block */
						plpgsql_ns_setlocal(true);
					}
				;

decl_stmts		: decl_stmts decl_stmt
					{	$$ = $2;	}
				| decl_stmt
					{	$$ = $1;	}
				;

decl_stmt		: '<' '<' any_name '>' '>'
					{	$$ = $3;	}
				| K_DECLARE
					{	$$ = NULL;	}
				| decl_statement
					{	$$ = NULL;	}
				;

decl_statement	: decl_varname decl_const decl_datatype decl_notnull decl_defval
					{
						PLpgSQL_variable	*var;

						var = plpgsql_build_variable($1.name, $1.lineno,
													 $3, true);
						if ($2)
						{
							if (var->dtype == PLPGSQL_DTYPE_VAR)
								((PLpgSQL_var *) var)->isconst = $2;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("row or record variable cannot be CONSTANT")));
						}
						if ($4)
						{
							if (var->dtype == PLPGSQL_DTYPE_VAR)
								((PLpgSQL_var *) var)->notnull = $4;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("row or record variable cannot be NOT NULL")));
						}
						if ($5 != NULL)
						{
							if (var->dtype == PLPGSQL_DTYPE_VAR)
								((PLpgSQL_var *) var)->default_val = $5;
							else
								ereport(ERROR,
										(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("default value for row or record variable is not supported")));
						}
					}
				| decl_varname K_ALIAS K_FOR decl_aliasitem ';'
					{
						plpgsql_ns_additem($4->itemtype,
										   $4->itemno, $1.name);
					}
				| K_RENAME decl_renname K_TO decl_renname ';'
					{
						plpgsql_ns_rename($2, $4);
					}
				| decl_varname opt_scrollable K_CURSOR
					{ plpgsql_ns_push($1.name); }
				  decl_cursor_args decl_is_for decl_cursor_query
					{
						PLpgSQL_var *new;
						PLpgSQL_expr *curname_def;
						char		buf[1024];
						char		*cp1;
						char		*cp2;

						/* pop local namespace for cursor args */
						plpgsql_ns_pop();

						new = (PLpgSQL_var *)
							plpgsql_build_variable($1.name, $1.lineno,
												   plpgsql_build_datatype(REFCURSOROID,
																		  -1),
												   true);

						curname_def = palloc0(sizeof(PLpgSQL_expr));

						curname_def->dtype = PLPGSQL_DTYPE_EXPR;
						strcpy(buf, "SELECT ");
						cp1 = new->refname;
						cp2 = buf + strlen(buf);
						/*
						 * Don't trust standard_conforming_strings here;
						 * it might change before we use the string.
						 */
						if (strchr(cp1, '\\') != NULL)
							*cp2++ = ESCAPE_STRING_SYNTAX;
						*cp2++ = '\'';
						while (*cp1)
						{
							if (SQL_STR_DOUBLE(*cp1, true))
								*cp2++ = *cp1;
							*cp2++ = *cp1++;
						}
						strcpy(cp2, "'::refcursor");
						curname_def->query = pstrdup(buf);
						new->default_val = curname_def;

						new->cursor_explicit_expr = $7;
						if ($5 == NULL)
							new->cursor_explicit_argrow = -1;
						else
							new->cursor_explicit_argrow = $5->dno;
						new->cursor_options = CURSOR_OPT_FAST_PLAN | $2;
					}
				;

opt_scrollable :
					{
						$$ = 0;
					}
				| K_NOSCROLL
					{
						$$ = CURSOR_OPT_NO_SCROLL;
					}
				| K_SCROLL
					{
						$$ = CURSOR_OPT_SCROLL;
					}
				;

decl_cursor_query :
					{
						PLpgSQL_expr *query;

						plpgsql_ns_setlocal(false);
						query = read_sql_stmt("");
						plpgsql_ns_setlocal(true);

						$$ = query;
					}
				;

decl_cursor_args :
					{
						$$ = NULL;
					}
				| '(' decl_cursor_arglist ')'
					{
						PLpgSQL_row *new;
						int i;
						ListCell *l;

						new = palloc0(sizeof(PLpgSQL_row));
						new->dtype = PLPGSQL_DTYPE_ROW;
						new->lineno = plpgsql_scanner_lineno();
						new->rowtupdesc = NULL;
						new->nfields = list_length($2);
						new->fieldnames = palloc(new->nfields * sizeof(char *));
						new->varnos = palloc(new->nfields * sizeof(int));

						i = 0;
						foreach (l, $2)
						{
							PLpgSQL_variable *arg = (PLpgSQL_variable *) lfirst(l);
							new->fieldnames[i] = arg->refname;
							new->varnos[i] = arg->dno;
							i++;
						}
						list_free($2);

						plpgsql_adddatum((PLpgSQL_datum *) new);
						$$ = new;
					}
				;

decl_cursor_arglist : decl_cursor_arg
					{
						$$ = list_make1($1);
					}
				| decl_cursor_arglist ',' decl_cursor_arg
					{
						$$ = lappend($1, $3);
					}
				;

decl_cursor_arg : decl_varname decl_datatype
					{
						$$ = plpgsql_build_variable($1.name, $1.lineno,
													$2, true);
					}
				;

decl_is_for		:	K_IS |		/* Oracle */
					K_FOR;		/* ANSI */

decl_aliasitem	: any_identifier
					{
						char	*name;
						PLpgSQL_nsitem *nsi;

						plpgsql_convert_ident($1, &name, 1);
						if (name[0] != '$')
							yyerror("only positional parameters can be aliased");

						plpgsql_ns_setlocal(false);

						nsi = plpgsql_ns_lookup(name, NULL, NULL, NULL);
						if (nsi == NULL)
						{
							plpgsql_error_lineno = plpgsql_scanner_lineno();
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_PARAMETER),
									 errmsg("function has no parameter \"%s\"",
											name)));
						}

						plpgsql_ns_setlocal(true);

						pfree(name);

						$$ = nsi;
					}
				;

decl_varname	: T_WORD
					{
						char	*name;

						plpgsql_convert_ident(yytext, &name, 1);
						$$.name = name;
						$$.lineno  = plpgsql_scanner_lineno();
					}
				| T_SCALAR
					{
						/*
						 * Since the scanner is only searching the topmost
						 * namestack entry, getting T_SCALAR etc can only
						 * happen if the name is already declared in this
						 * block.
						 */
						yyerror("duplicate declaration");
					}
				| T_ROW
					{
						yyerror("duplicate declaration");
					}
				| T_RECORD
					{
						yyerror("duplicate declaration");
					}
				;

/* XXX this is broken because it doesn't allow for T_SCALAR,T_ROW,T_RECORD */
decl_renname	: T_WORD
					{
						char	*name;

						plpgsql_convert_ident(yytext, &name, 1);
						/* the result must be palloc'd, see plpgsql_ns_rename */
						$$ = name;
					}
				;

decl_const		:
					{ $$ = false; }
				| K_CONSTANT
					{ $$ = true; }
				;

decl_datatype	:
					{
						/*
						 * If there's a lookahead token, read_datatype
						 * should consume it.
						 */
						$$ = read_datatype(yychar);
						yyclearin;
					}
				;

decl_notnull	:
					{ $$ = false; }
				| K_NOT K_NULL
					{ $$ = true; }
				;

decl_defval		: ';'
					{ $$ = NULL; }
				| decl_defkey
					{
						plpgsql_ns_setlocal(false);
						$$ = plpgsql_read_expression(';', ";");
						plpgsql_ns_setlocal(true);
					}
				;

decl_defkey		: K_ASSIGN
				| K_DEFAULT
				;

proc_sect		:
					{ $$ = NIL; }
				| proc_stmts
					{ $$ = $1; }
				;

proc_stmts		: proc_stmts proc_stmt
						{
							if ($2 == NULL)
								$$ = $1;
							else
								$$ = lappend($1, $2);
						}
				| proc_stmt
						{
							if ($1 == NULL)
								$$ = NIL;
							else
								$$ = list_make1($1);
						}
				;

proc_stmt		: pl_block ';'
						{ $$ = $1; }
				| stmt_assign
						{ $$ = $1; }
				| stmt_if
						{ $$ = $1; }
				| stmt_case
						{ $$ = $1; }
				| stmt_loop
						{ $$ = $1; }
				| stmt_while
						{ $$ = $1; }
				| stmt_for
						{ $$ = $1; }
				| stmt_exit
						{ $$ = $1; }
				| stmt_return
						{ $$ = $1; }
				| stmt_raise
						{ $$ = $1; }
				| stmt_execsql
						{ $$ = $1; }
				| stmt_dynexecute
						{ $$ = $1; }
				| stmt_perform
						{ $$ = $1; }
				| stmt_getdiag
						{ $$ = $1; }
				| stmt_open
						{ $$ = $1; }
				| stmt_fetch
						{ $$ = $1; }
				| stmt_move
						{ $$ = $1; }
				| stmt_close
						{ $$ = $1; }
				| stmt_null
						{ $$ = $1; }
				;

stmt_perform	: K_PERFORM lno expr_until_semi
					{
						PLpgSQL_stmt_perform *new;

						new = palloc0(sizeof(PLpgSQL_stmt_perform));
						new->cmd_type = PLPGSQL_STMT_PERFORM;
						new->lineno   = $2;
						new->expr  = $3;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_assign		: assign_var lno K_ASSIGN expr_until_semi
					{
						PLpgSQL_stmt_assign *new;

						new = palloc0(sizeof(PLpgSQL_stmt_assign));
						new->cmd_type = PLPGSQL_STMT_ASSIGN;
						new->lineno   = $2;
						new->varno = $1;
						new->expr  = $4;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_getdiag	: K_GET K_DIAGNOSTICS lno getdiag_list ';'
					{
						PLpgSQL_stmt_getdiag	 *new;

						new = palloc0(sizeof(PLpgSQL_stmt_getdiag));
						new->cmd_type = PLPGSQL_STMT_GETDIAG;
						new->lineno   = $3;
						new->diag_items  = $4;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

getdiag_list : getdiag_list ',' getdiag_list_item
					{
						$$ = lappend($1, $3);
					}
				| getdiag_list_item
					{
						$$ = list_make1($1);
					}
				;

getdiag_list_item : getdiag_target K_ASSIGN getdiag_kind
					{
						PLpgSQL_diag_item *new;

						new = palloc(sizeof(PLpgSQL_diag_item));
						new->target = $1;
						new->kind = $3;

						$$ = new;
					}
				;

getdiag_kind : K_ROW_COUNT
					{
						$$ = PLPGSQL_GETDIAG_ROW_COUNT;
					}
				| K_RESULT_OID
					{
						$$ = PLPGSQL_GETDIAG_RESULT_OID;
					}
				;

getdiag_target	: T_SCALAR
					{
						check_assignable(yylval.scalar);
						$$ = yylval.scalar->dno;
					}
				| T_ROW
					{
						yyerror("expected an integer variable");
					}
				| T_RECORD
					{
						yyerror("expected an integer variable");
					}
				| T_WORD
					{
						yyerror("expected an integer variable");
					}
				;


assign_var		: T_SCALAR
					{
						check_assignable(yylval.scalar);
						$$ = yylval.scalar->dno;
					}
				| T_ROW
					{
						check_assignable((PLpgSQL_datum *) yylval.row);
						$$ = yylval.row->dno;
					}
				| T_RECORD
					{
						check_assignable((PLpgSQL_datum *) yylval.rec);
						$$ = yylval.rec->dno;
					}
				| assign_var '[' expr_until_rightbracket
					{
						PLpgSQL_arrayelem	*new;

						new = palloc0(sizeof(PLpgSQL_arrayelem));
						new->dtype		= PLPGSQL_DTYPE_ARRAYELEM;
						new->subscript	= $3;
						new->arrayparentno = $1;

						plpgsql_adddatum((PLpgSQL_datum *)new);

						$$ = new->dno;
					}
				;

stmt_if			: K_IF lno expr_until_then proc_sect stmt_else K_END K_IF ';'
					{
						PLpgSQL_stmt_if *new;

						new = palloc0(sizeof(PLpgSQL_stmt_if));
						new->cmd_type	= PLPGSQL_STMT_IF;
						new->lineno		= $2;
						new->cond		= $3;
						new->true_body	= $4;
						new->false_body = $5;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_else		:
					{
						$$ = NIL;
					}
				| K_ELSIF lno expr_until_then proc_sect stmt_else
					{
						/*
						 * Translate the structure:	   into:
						 *
						 * IF c1 THEN				   IF c1 THEN
						 *	 ...						   ...
						 * ELSIF c2 THEN			   ELSE
						 *								   IF c2 THEN
						 *	 ...							   ...
						 * ELSE							   ELSE
						 *	 ...							   ...
						 * END IF						   END IF
						 *							   END IF
						 */
						PLpgSQL_stmt_if *new_if;

						/* first create a new if-statement */
						new_if = palloc0(sizeof(PLpgSQL_stmt_if));
						new_if->cmd_type	= PLPGSQL_STMT_IF;
						new_if->lineno		= $2;
						new_if->cond		= $3;
						new_if->true_body	= $4;
						new_if->false_body	= $5;

						/* wrap the if-statement in a "container" list */
						$$ = list_make1(new_if);
					}

				| K_ELSE proc_sect
					{
						$$ = $2;
					}
				;

stmt_case		: K_CASE lno opt_expr_until_when case_when_list opt_case_else K_END K_CASE ';'
					{
						$$ = make_case($2, $3, $4, $5);
					}
				;

opt_expr_until_when	:
					{
						PLpgSQL_expr *expr = NULL;
						int	tok = yylex();

						if (tok != K_WHEN)
						{
							plpgsql_push_back_token(tok);
							expr = plpgsql_read_expression(K_WHEN, "WHEN");
						}
						plpgsql_push_back_token(K_WHEN);
						$$ = expr;
					}
			    ;

case_when_list	: case_when_list case_when
					{
						$$ = lappend($1, $2);
					}
				| case_when
					{
						$$ = list_make1($1);
					}
				;

case_when		: K_WHEN lno expr_until_then proc_sect
					{
						PLpgSQL_case_when *new = palloc(sizeof(PLpgSQL_case_when));

						new->lineno	= $2;
						new->expr	= $3;
						new->stmts	= $4;
						$$ = new;
					}
				;

opt_case_else	:
					{
						$$ = NIL;
					}
				| K_ELSE proc_sect
					{
						/*
						 * proc_sect could return an empty list, but we
						 * must distinguish that from not having ELSE at all.
						 * Simplest fix is to return a list with one NULL
						 * pointer, which make_case() must take care of.
						 */
						if ($2 != NIL)
							$$ = $2;
						else
							$$ = list_make1(NULL);
					}
				;

stmt_loop		: opt_block_label K_LOOP lno loop_body
					{
						PLpgSQL_stmt_loop *new;

						new = palloc0(sizeof(PLpgSQL_stmt_loop));
						new->cmd_type = PLPGSQL_STMT_LOOP;
						new->lineno   = $3;
						new->label	  = $1;
						new->body	  = $4.stmts;

						check_labels($1, $4.end_label);
						plpgsql_ns_pop();

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_while		: opt_block_label K_WHILE lno expr_until_loop loop_body
					{
						PLpgSQL_stmt_while *new;

						new = palloc0(sizeof(PLpgSQL_stmt_while));
						new->cmd_type = PLPGSQL_STMT_WHILE;
						new->lineno   = $3;
						new->label	  = $1;
						new->cond	  = $4;
						new->body	  = $5.stmts;

						check_labels($1, $5.end_label);
						plpgsql_ns_pop();

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_for		: opt_block_label K_FOR for_control loop_body
					{
						/* This runs after we've scanned the loop body */
						if ($3->cmd_type == PLPGSQL_STMT_FORI)
						{
							PLpgSQL_stmt_fori		*new;

							new = (PLpgSQL_stmt_fori *) $3;
							new->label	  = $1;
							new->body	  = $4.stmts;
							$$ = (PLpgSQL_stmt *) new;
						}
						else
						{
							PLpgSQL_stmt_forq		*new;

							Assert($3->cmd_type == PLPGSQL_STMT_FORS ||
								   $3->cmd_type == PLPGSQL_STMT_FORC ||
								   $3->cmd_type == PLPGSQL_STMT_DYNFORS);
							/* forq is the common supertype of all three */
							new = (PLpgSQL_stmt_forq *) $3;
							new->label	  = $1;
							new->body	  = $4.stmts;
							$$ = (PLpgSQL_stmt *) new;
						}

						check_labels($1, $4.end_label);
						/* close namespace started in opt_block_label */
						plpgsql_ns_pop();
					}
				;

for_control		:
				lno for_variable K_IN
					{
						int			tok = yylex();

						if (tok == K_EXECUTE)
						{
							/* EXECUTE means it's a dynamic FOR loop */
							PLpgSQL_stmt_dynfors	*new;
							PLpgSQL_expr			*expr;
							int						term;

							expr = read_sql_expression2(K_LOOP, K_USING,
														"LOOP or USING",
														&term);

							new = palloc0(sizeof(PLpgSQL_stmt_dynfors));
							new->cmd_type = PLPGSQL_STMT_DYNFORS;
							new->lineno   = $1;
							if ($2.rec)
							{
								new->rec = $2.rec;
								check_assignable((PLpgSQL_datum *) new->rec);
							}
							else if ($2.row)
							{
								new->row = $2.row;
								check_assignable((PLpgSQL_datum *) new->row);
							}
							else if ($2.scalar)
							{
								/* convert single scalar to list */
								new->row = make_scalar_list1($2.name, $2.scalar, $2.lineno);
								/* no need for check_assignable */
							}
							else
							{
								plpgsql_error_lineno = $2.lineno;
								yyerror("loop variable of loop over rows must be a record or row variable or list of scalar variables");
							}
							new->query = expr;

							if (term == K_USING)
							{
								do
								{
									expr = read_sql_expression2(',', K_LOOP,
																", or LOOP",
																&term);
									new->params = lappend(new->params, expr);
								} while (term == ',');
							}

							$$ = (PLpgSQL_stmt *) new;
						}
						else if (tok == T_SCALAR &&
								 yylval.scalar->dtype == PLPGSQL_DTYPE_VAR &&
								 ((PLpgSQL_var *) yylval.scalar)->datatype->typoid == REFCURSOROID)
						{
							/* It's FOR var IN cursor */
							PLpgSQL_stmt_forc	*new;
							PLpgSQL_var			*cursor = (PLpgSQL_var *) yylval.scalar;
							char				*varname;

							new = (PLpgSQL_stmt_forc *) palloc0(sizeof(PLpgSQL_stmt_forc));
							new->cmd_type = PLPGSQL_STMT_FORC;
							new->lineno = $1;

							new->curvar = cursor->dno;

							/* Should have had a single variable name */
							plpgsql_error_lineno = $2.lineno;
							if ($2.scalar && $2.row)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("cursor FOR loop must have only one target variable")));

							/* create loop's private RECORD variable */
							plpgsql_convert_ident($2.name, &varname, 1);
							new->rec = plpgsql_build_record(varname,
															$2.lineno,
															true);

							/* can't use an unbound cursor this way */
							if (cursor->cursor_explicit_expr == NULL)
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("cursor FOR loop must use a bound cursor variable")));

							/* collect cursor's parameters if any */
							new->argquery = read_cursor_args(cursor,
															 K_LOOP,
															 "LOOP");

							$$ = (PLpgSQL_stmt *) new;
						}
						else
						{
							PLpgSQL_expr	*expr1;
							bool			 reverse = false;

							/*
							 * We have to distinguish between two
							 * alternatives: FOR var IN a .. b and FOR
							 * var IN query. Unfortunately this is
							 * tricky, since the query in the second
							 * form needn't start with a SELECT
							 * keyword.  We use the ugly hack of
							 * looking for two periods after the first
							 * token. We also check for the REVERSE
							 * keyword, which means it must be an
							 * integer loop.
							 */
							if (tok == K_REVERSE)
								reverse = true;
							else
								plpgsql_push_back_token(tok);

							/*
							 * Read tokens until we see either a ".."
							 * or a LOOP. The text we read may not
							 * necessarily be a well-formed SQL
							 * statement, so we need to invoke
							 * read_sql_construct directly.
							 */
							expr1 = read_sql_construct(K_DOTDOT,
													   K_LOOP,
													   0,
													   "LOOP",
													   "SELECT ",
													   true,
													   false,
													   &tok);

							if (tok == K_DOTDOT)
							{
								/* Saw "..", so it must be an integer loop */
								PLpgSQL_expr		*expr2;
								PLpgSQL_expr		*expr_by;
								PLpgSQL_var			*fvar;
								PLpgSQL_stmt_fori	*new;
								char				*varname;

								/* Check first expression is well-formed */
								check_sql_expr(expr1->query);

								/* Read and check the second one */
								expr2 = read_sql_expression2(K_LOOP, K_BY,
															 "LOOP",
															 &tok);

								/* Get the BY clause if any */
								if (tok == K_BY)
									expr_by = plpgsql_read_expression(K_LOOP,
																	  "LOOP");
								else
									expr_by = NULL;

								/* Should have had a single variable name */
								plpgsql_error_lineno = $2.lineno;
								if ($2.scalar && $2.row)
									ereport(ERROR,
											(errcode(ERRCODE_SYNTAX_ERROR),
											 errmsg("integer FOR loop must have only one target variable")));

								/* create loop's private variable */
								plpgsql_convert_ident($2.name, &varname, 1);
								fvar = (PLpgSQL_var *)
									plpgsql_build_variable(varname,
														   $2.lineno,
														   plpgsql_build_datatype(INT4OID,
																				  -1),
														   true);

								new = palloc0(sizeof(PLpgSQL_stmt_fori));
								new->cmd_type = PLPGSQL_STMT_FORI;
								new->lineno   = $1;
								new->var	  = fvar;
								new->reverse  = reverse;
								new->lower	  = expr1;
								new->upper	  = expr2;
								new->step	  = expr_by;

								$$ = (PLpgSQL_stmt *) new;
							}
							else
							{
								/*
								 * No "..", so it must be a query loop. We've prefixed an
								 * extra SELECT to the query text, so we need to remove that
								 * before performing syntax checking.
								 */
								char				*tmp_query;
								PLpgSQL_stmt_fors	*new;

								if (reverse)
									yyerror("cannot specify REVERSE in query FOR loop");

								Assert(strncmp(expr1->query, "SELECT ", 7) == 0);
								tmp_query = pstrdup(expr1->query + 7);
								pfree(expr1->query);
								expr1->query = tmp_query;

								check_sql_expr(expr1->query);

								new = palloc0(sizeof(PLpgSQL_stmt_fors));
								new->cmd_type = PLPGSQL_STMT_FORS;
								new->lineno   = $1;
								if ($2.rec)
								{
									new->rec = $2.rec;
									check_assignable((PLpgSQL_datum *) new->rec);
								}
								else if ($2.row)
								{
									new->row = $2.row;
									check_assignable((PLpgSQL_datum *) new->row);
								}
								else if ($2.scalar)
								{
									/* convert single scalar to list */
									new->row = make_scalar_list1($2.name, $2.scalar, $2.lineno);
									/* no need for check_assignable */
								}
								else
								{
									plpgsql_error_lineno = $2.lineno;
									yyerror("loop variable of loop over rows must be a record or row variable or list of scalar variables");
								}

								new->query = expr1;
								$$ = (PLpgSQL_stmt *) new;
							}
						}
					}
				;

/*
 * Processing the for_variable is tricky because we don't yet know if the
 * FOR is an integer FOR loop or a loop over query results.  In the former
 * case, the variable is just a name that we must instantiate as a loop
 * local variable, regardless of any other definition it might have.
 * Therefore, we always save the actual identifier into $$.name where it
 * can be used for that case.  We also save the outer-variable definition,
 * if any, because that's what we need for the loop-over-query case.  Note
 * that we must NOT apply check_assignable() or any other semantic check
 * until we know what's what.
 *
 * However, if we see a comma-separated list of names, we know that it
 * can't be an integer FOR loop and so it's OK to check the variables
 * immediately.  In particular, for T_WORD followed by comma, we should
 * complain that the name is not known rather than say it's a syntax error.
 * Note that the non-error result of this case sets *both* $$.scalar and
 * $$.row; see the for_control production.
 */
for_variable	: T_SCALAR
					{
						int			tok;

						$$.name = pstrdup(yytext);
						$$.lineno  = plpgsql_scanner_lineno();
						$$.scalar = yylval.scalar;
						$$.rec = NULL;
						$$.row = NULL;
						/* check for comma-separated list */
						tok = yylex();
						plpgsql_push_back_token(tok);
						if (tok == ',')
							$$.row = read_into_scalar_list($$.name, $$.scalar);
					}
				| T_WORD
					{
						int			tok;

						$$.name = pstrdup(yytext);
						$$.lineno  = plpgsql_scanner_lineno();
						$$.scalar = NULL;
						$$.rec = NULL;
						$$.row = NULL;
						/* check for comma-separated list */
						tok = yylex();
						plpgsql_push_back_token(tok);
						if (tok == ',')
						{
							plpgsql_error_lineno = $$.lineno;
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("\"%s\" is not a scalar variable",
											$$.name)));
						}
					}
				| T_RECORD
					{
						$$.name = pstrdup(yytext);
						$$.lineno  = plpgsql_scanner_lineno();
						$$.scalar = NULL;
						$$.rec = yylval.rec;
						$$.row = NULL;
					}
				| T_ROW
					{
						$$.name = pstrdup(yytext);
						$$.lineno  = plpgsql_scanner_lineno();
						$$.scalar = NULL;
						$$.row = yylval.row;
						$$.rec = NULL;
					}
				;

stmt_exit		: exit_type lno opt_label opt_exitcond
					{
						PLpgSQL_stmt_exit *new;

						new = palloc0(sizeof(PLpgSQL_stmt_exit));
						new->cmd_type = PLPGSQL_STMT_EXIT;
						new->is_exit  = $1;
						new->lineno	  = $2;
						new->label	  = $3;
						new->cond	  = $4;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

exit_type		: K_EXIT
					{
						$$ = true;
					}
				| K_CONTINUE
					{
						$$ = false;
					}
				;

stmt_return		: K_RETURN lno
					{
						int	tok;

						tok = yylex();
						if (tok == 0)
							yyerror("unexpected end of function definition");

						/*
						 * To avoid making NEXT and QUERY effectively be
						 * reserved words within plpgsql, recognize them
						 * via yytext.
						 */
						if (pg_strcasecmp(yytext, "next") == 0)
						{
							$$ = make_return_next_stmt($2);
						}
						else if (pg_strcasecmp(yytext, "query") == 0)
						{
							$$ = make_return_query_stmt($2);
						}
						else
						{
							plpgsql_push_back_token(tok);
							$$ = make_return_stmt($2);
						}
					}
				;

stmt_raise		: K_RAISE lno
					{
						PLpgSQL_stmt_raise		*new;
						int	tok;

						new = palloc(sizeof(PLpgSQL_stmt_raise));

						new->cmd_type	= PLPGSQL_STMT_RAISE;
						new->lineno		= $2;
						new->elog_level = ERROR;	/* default */
						new->condname	= NULL;
						new->message	= NULL;
						new->params		= NIL;
						new->options	= NIL;

						tok = yylex();
						if (tok == 0)
							yyerror("unexpected end of function definition");

						/*
						 * We could have just RAISE, meaning to re-throw
						 * the current error.
						 */
						if (tok != ';')
						{
							/*
							 * First is an optional elog severity level.
							 * Most of these are not plpgsql keywords,
							 * so we rely on examining yytext.
							 */
							if (pg_strcasecmp(yytext, "exception") == 0)
							{
								new->elog_level = ERROR;
								tok = yylex();
							}
							else if (pg_strcasecmp(yytext, "warning") == 0)
							{
								new->elog_level = WARNING;
								tok = yylex();
							}
							else if (pg_strcasecmp(yytext, "notice") == 0)
							{
								new->elog_level = NOTICE;
								tok = yylex();
							}
							else if (pg_strcasecmp(yytext, "info") == 0)
							{
								new->elog_level = INFO;
								tok = yylex();
							}
							else if (pg_strcasecmp(yytext, "log") == 0)
							{
								new->elog_level = LOG;
								tok = yylex();
							}
							else if (pg_strcasecmp(yytext, "debug") == 0)
							{
								new->elog_level = DEBUG1;
								tok = yylex();
							}
							if (tok == 0)
								yyerror("unexpected end of function definition");

							/*
							 * Next we can have a condition name, or
							 * equivalently SQLSTATE 'xxxxx', or a string
							 * literal that is the old-style message format,
							 * or USING to start the option list immediately.
							 */
							if (tok == T_STRING)
							{
								/* old style message and parameters */
								new->message = parse_string_token(yytext);
								/*
								 * We expect either a semi-colon, which
								 * indicates no parameters, or a comma that
								 * begins the list of parameter expressions,
								 * or USING to begin the options list.
								 */
								tok = yylex();
								if (tok != ',' && tok != ';' && tok != K_USING)
									yyerror("syntax error");

								while (tok == ',')
								{
									PLpgSQL_expr *expr;

									expr = read_sql_construct(',', ';', K_USING,
															  ", or ; or USING",
															  "SELECT ",
															  true, true, &tok);
									new->params = lappend(new->params, expr);
								}
							}
							else if (tok != K_USING)
							{
								/* must be condition name or SQLSTATE */
								if (pg_strcasecmp(yytext, "sqlstate") == 0)
								{
									/* next token should be a string literal */
									char   *sqlstatestr;

									if (yylex() != T_STRING)
										yyerror("syntax error");
									sqlstatestr = parse_string_token(yytext);

									if (strlen(sqlstatestr) != 5)
										yyerror("invalid SQLSTATE code");
									if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
										yyerror("invalid SQLSTATE code");
									new->condname = sqlstatestr;
								}
								else
								{
									char   *cname;

									if (tok != T_WORD)
										yyerror("syntax error");
									plpgsql_convert_ident(yytext, &cname, 1);
									plpgsql_recognize_err_condition(cname,
																	false);
									new->condname = cname;
								}
								tok = yylex();
								if (tok != ';' && tok != K_USING)
									yyerror("syntax error");
							}

							if (tok == K_USING)
								new->options = read_raise_options();
						}

						$$ = (PLpgSQL_stmt *)new;
					}
				;

loop_body		: proc_sect K_END K_LOOP opt_label ';'
					{
						$$.stmts = $1;
						$$.end_label = $4;
					}
				;

stmt_execsql	: execsql_start lno
					{
						$$ = make_execsql_stmt($1, $2);
					}
				;

/* T_WORD+T_ERROR match any otherwise-unrecognized starting keyword */
execsql_start	: K_INSERT
					{ $$ = pstrdup(yytext); }
				| T_WORD
					{ $$ = pstrdup(yytext); }
				| T_ERROR
					{ $$ = pstrdup(yytext); }
				;

stmt_dynexecute : K_EXECUTE lno
					{
						PLpgSQL_stmt_dynexecute *new;
						PLpgSQL_expr *expr;
						int endtoken;

						expr = read_sql_construct(K_INTO, K_USING, ';',
												  "INTO or USING or ;",
												  "SELECT ",
												  true, true, &endtoken);

						new = palloc(sizeof(PLpgSQL_stmt_dynexecute));
						new->cmd_type = PLPGSQL_STMT_DYNEXECUTE;
						new->lineno = $2;
						new->query = expr;
						new->into = false;
						new->strict = false;
						new->rec = NULL;
						new->row = NULL;
						new->params = NIL;

						/* If we found "INTO", collect the argument */
						if (endtoken == K_INTO)
						{
							new->into = true;
							read_into_target(&new->rec, &new->row, &new->strict);
							endtoken = yylex();
							if (endtoken != ';' && endtoken != K_USING)
								yyerror("syntax error");
						}

						/* If we found "USING", collect the argument(s) */
						if (endtoken == K_USING)
						{
							do
							{
								expr = read_sql_expression2(',', ';',
															", or ;",
															&endtoken);
								new->params = lappend(new->params, expr);
							} while (endtoken == ',');
						}

						$$ = (PLpgSQL_stmt *)new;
					}
				;


stmt_open		: K_OPEN lno cursor_variable
					{
						PLpgSQL_stmt_open *new;
						int				  tok;

						new = palloc0(sizeof(PLpgSQL_stmt_open));
						new->cmd_type = PLPGSQL_STMT_OPEN;
						new->lineno = $2;
						new->curvar = $3->dno;
						new->cursor_options = CURSOR_OPT_FAST_PLAN;

						if ($3->cursor_explicit_expr == NULL)
						{
							/* be nice if we could use opt_scrollable here */
						    tok = yylex();
							if (tok == K_NOSCROLL)
							{
								new->cursor_options |= CURSOR_OPT_NO_SCROLL;
								tok = yylex();
							}
							else if (tok == K_SCROLL)
							{
								new->cursor_options |= CURSOR_OPT_SCROLL;
								tok = yylex();
							}

							if (tok != K_FOR)
							{
								plpgsql_error_lineno = $2;
								ereport(ERROR,
										(errcode(ERRCODE_SYNTAX_ERROR),
										 errmsg("syntax error at \"%s\"",
												yytext),
										 errdetail("Expected \"FOR\", to open a cursor for an unbound cursor variable.")));
							}

							tok = yylex();
							if (tok == K_EXECUTE)
							{
								new->dynquery = read_sql_stmt("SELECT ");
							}
							else
							{
								plpgsql_push_back_token(tok);
								new->query = read_sql_stmt("");
							}
						}
						else
						{
							/* predefined cursor query, so read args */
							new->argquery = read_cursor_args($3, ';', ";");
						}

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_fetch		: K_FETCH lno opt_fetch_direction cursor_variable K_INTO
					{
						PLpgSQL_stmt_fetch *fetch = $3;
						PLpgSQL_rec	   *rec;
						PLpgSQL_row	   *row;

						/* We have already parsed everything through the INTO keyword */
						read_into_target(&rec, &row, NULL);

						if (yylex() != ';')
							yyerror("syntax error");

						fetch->lineno = $2;
						fetch->rec		= rec;
						fetch->row		= row;
						fetch->curvar	= $4->dno;
						fetch->is_move	= false;

						$$ = (PLpgSQL_stmt *)fetch;
					}
				;

stmt_move		: K_MOVE lno opt_fetch_direction cursor_variable ';'
					{
						PLpgSQL_stmt_fetch *fetch = $3;

						fetch->lineno = $2;
						fetch->curvar	= $4->dno;
						fetch->is_move	= true;

						$$ = (PLpgSQL_stmt *)fetch;
					}
				;

opt_fetch_direction	:
					{
						$$ = read_fetch_direction();
					}
				;

stmt_close		: K_CLOSE lno cursor_variable ';'
					{
						PLpgSQL_stmt_close *new;

						new = palloc(sizeof(PLpgSQL_stmt_close));
						new->cmd_type = PLPGSQL_STMT_CLOSE;
						new->lineno = $2;
						new->curvar = $3->dno;

						$$ = (PLpgSQL_stmt *)new;
					}
				;

stmt_null		: K_NULL ';'
					{
						/* We do not bother building a node for NULL */
						$$ = NULL;
					}
				;

cursor_variable	: T_SCALAR
					{
						if (yylval.scalar->dtype != PLPGSQL_DTYPE_VAR)
							yyerror("cursor variable must be a simple variable");

						if (((PLpgSQL_var *) yylval.scalar)->datatype->typoid != REFCURSOROID)
						{
							plpgsql_error_lineno = plpgsql_scanner_lineno();
							ereport(ERROR,
									(errcode(ERRCODE_DATATYPE_MISMATCH),
									 errmsg("variable \"%s\" must be of type cursor or refcursor",
											((PLpgSQL_var *) yylval.scalar)->refname)));
						}
						$$ = (PLpgSQL_var *) yylval.scalar;
					}
				| T_ROW
					{
						yyerror("expected a cursor or refcursor variable");
					}
				| T_RECORD
					{
						yyerror("expected a cursor or refcursor variable");
					}
				| T_WORD
					{
						yyerror("expected a cursor or refcursor variable");
					}
				;

exception_sect	:
					{ $$ = NULL; }
				| K_EXCEPTION lno
					{
						/*
						 * We use a mid-rule action to add these
						 * special variables to the namespace before
						 * parsing the WHEN clauses themselves.
						 */
						PLpgSQL_exception_block *new = palloc(sizeof(PLpgSQL_exception_block));
						PLpgSQL_variable *var;

						var = plpgsql_build_variable("sqlstate", $2,
													 plpgsql_build_datatype(TEXTOID, -1),
													 true);
						((PLpgSQL_var *) var)->isconst = true;
						new->sqlstate_varno = var->dno;

						var = plpgsql_build_variable("sqlerrm", $2,
													 plpgsql_build_datatype(TEXTOID, -1),
													 true);
						((PLpgSQL_var *) var)->isconst = true;
						new->sqlerrm_varno = var->dno;

						$<exception_block>$ = new;
					}
					proc_exceptions
					{
						PLpgSQL_exception_block *new = $<exception_block>3;
						new->exc_list = $4;

						$$ = new;
					}
				;

proc_exceptions	: proc_exceptions proc_exception
						{
							$$ = lappend($1, $2);
						}
				| proc_exception
						{
							$$ = list_make1($1);
						}
				;

proc_exception	: K_WHEN lno proc_conditions K_THEN proc_sect
					{
						PLpgSQL_exception *new;

						new = palloc0(sizeof(PLpgSQL_exception));
						new->lineno     = $2;
						new->conditions = $3;
						new->action	    = $5;

						$$ = new;
					}
				;

proc_conditions	: proc_conditions K_OR proc_condition
						{
							PLpgSQL_condition	*old;

							for (old = $1; old->next != NULL; old = old->next)
								/* skip */ ;
							old->next = $3;
							$$ = $1;
						}
				| proc_condition
						{
							$$ = $1;
						}
				;

proc_condition	: any_name
						{
							if (strcmp($1, "sqlstate") != 0)
							{
								$$ = plpgsql_parse_err_condition($1);
							}
							else
							{
								PLpgSQL_condition *new;
								char   *sqlstatestr;

								/* next token should be a string literal */
								if (yylex() != T_STRING)
									yyerror("syntax error");
								sqlstatestr = parse_string_token(yytext);

								if (strlen(sqlstatestr) != 5)
									yyerror("invalid SQLSTATE code");
								if (strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") != 5)
									yyerror("invalid SQLSTATE code");

								new = palloc(sizeof(PLpgSQL_condition));
								new->sqlerrstate =
									MAKE_SQLSTATE(sqlstatestr[0],
												  sqlstatestr[1],
												  sqlstatestr[2],
												  sqlstatestr[3],
												  sqlstatestr[4]);
								new->condname = sqlstatestr;
								new->next = NULL;

								$$ = new;
							}
						}
				;

expr_until_semi :
					{ $$ = plpgsql_read_expression(';', ";"); }
				;

expr_until_rightbracket :
					{ $$ = plpgsql_read_expression(']', "]"); }
				;

expr_until_then :
					{ $$ = plpgsql_read_expression(K_THEN, "THEN"); }
				;

expr_until_loop :
					{ $$ = plpgsql_read_expression(K_LOOP, "LOOP"); }
				;

opt_block_label	:
					{
						plpgsql_ns_push(NULL);
						$$ = NULL;
					}
				| '<' '<' any_name '>' '>'
					{
						plpgsql_ns_push($3);
						$$ = $3;
					}
				;

opt_label	:
					{
						$$ = NULL;
					}
				| any_identifier
					{
						$$ = check_label($1);
					}
				;

opt_exitcond	: ';'
					{ $$ = NULL; }
				| K_WHEN expr_until_semi
					{ $$ = $2; }
				;

/*
 * need all the options because scanner will have tried to resolve as variable
 */
any_identifier	: T_WORD
					{
						$$ = yytext;
					}
				| T_SCALAR
					{
						$$ = yytext;
					}
				| T_RECORD
					{
						$$ = yytext;
					}
				| T_ROW
					{
						$$ = yytext;
					}
				;

any_name		: any_identifier
					{
						char	*name;

						plpgsql_convert_ident($1, &name, 1);
						$$ = name;
					}
				;

lno				:
					{
						$$ = plpgsql_error_lineno = plpgsql_scanner_lineno();
					}
				;

%%


#define MAX_EXPR_PARAMS  1024

/*
 * determine the expression parameter position to use for a plpgsql datum
 *
 * It is important that any given plpgsql datum map to just one parameter.
 * We used to be sloppy and assign a separate parameter for each occurrence
 * of a datum reference, but that fails for situations such as "select DATUM
 * from ... group by DATUM".
 *
 * The params[] array must be of size MAX_EXPR_PARAMS.
 */
static int
assign_expr_param(int dno, int *params, int *nparams)
{
	int		i;

	/* already have an instance of this dno? */
	for (i = 0; i < *nparams; i++)
	{
		if (params[i] == dno)
			return i+1;
	}
	/* check for array overflow */
	if (*nparams >= MAX_EXPR_PARAMS)
	{
		plpgsql_error_lineno = plpgsql_scanner_lineno();
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("too many variables specified in SQL statement")));
	}
	/* add new parameter dno to array */
	params[*nparams] = dno;
	(*nparams)++;
	return *nparams;
}


/* Convenience routine to read an expression with one possible terminator */
PLpgSQL_expr *
plpgsql_read_expression(int until, const char *expected)
{
	return read_sql_construct(until, 0, 0, expected,
							  "SELECT ", true, true, NULL);
}

/* Convenience routine to read an expression with two possible terminators */
static PLpgSQL_expr *
read_sql_expression2(int until, int until2, const char *expected,
					 int *endtoken)
{
	return read_sql_construct(until, until2, 0, expected,
							  "SELECT ", true, true, endtoken);
}

/* Convenience routine to read a SQL statement that must end with ';' */
static PLpgSQL_expr *
read_sql_stmt(const char *sqlstart)
{
	return read_sql_construct(';', 0, 0, ";",
							  sqlstart, false, true, NULL);
}

/*
 * Read a SQL construct and build a PLpgSQL_expr for it.
 *
 * until:		token code for expected terminator
 * until2:		token code for alternate terminator (pass 0 if none)
 * until3:		token code for another alternate terminator (pass 0 if none)
 * expected:	text to use in complaining that terminator was not found
 * sqlstart:	text to prefix to the accumulated SQL text
 * isexpression: whether to say we're reading an "expression" or a "statement"
 * valid_sql:   whether to check the syntax of the expr (prefixed with sqlstart)
 * endtoken:	if not NULL, ending token is stored at *endtoken
 *				(this is only interesting if until2 or until3 isn't zero)
 */
static PLpgSQL_expr *
read_sql_construct(int until,
				   int until2,
				   int until3,
				   const char *expected,
				   const char *sqlstart,
				   bool isexpression,
				   bool valid_sql,
				   int *endtoken)
{
	int					tok;
	int					lno;
	PLpgSQL_dstring		ds;
	int					parenlevel = 0;
	int					nparams = 0;
	int					params[MAX_EXPR_PARAMS];
	char				buf[32];
	PLpgSQL_expr		*expr;

	lno = plpgsql_scanner_lineno();
	plpgsql_dstring_init(&ds);
	plpgsql_dstring_append(&ds, sqlstart);

	for (;;)
	{
		tok = yylex();
		if (tok == until && parenlevel == 0)
			break;
		if (tok == until2 && parenlevel == 0)
			break;
		if (tok == until3 && parenlevel == 0)
			break;
		if (tok == '(' || tok == '[')
			parenlevel++;
		else if (tok == ')' || tok == ']')
		{
			parenlevel--;
			if (parenlevel < 0)
				yyerror("mismatched parentheses");
		}
		/*
		 * End of function definition is an error, and we don't expect to
		 * hit a semicolon either (unless it's the until symbol, in which
		 * case we should have fallen out above).
		 */
		if (tok == 0 || tok == ';')
		{
			if (parenlevel != 0)
				yyerror("mismatched parentheses");
			plpgsql_error_lineno = lno;
			if (isexpression)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("missing \"%s\" at end of SQL expression",
								expected)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("missing \"%s\" at end of SQL statement",
								expected)));
		}

		if (plpgsql_SpaceScanned)
			plpgsql_dstring_append(&ds, " ");

		switch (tok)
		{
			case T_SCALAR:
				snprintf(buf, sizeof(buf), " $%d ",
						 assign_expr_param(yylval.scalar->dno,
										   params, &nparams));
				plpgsql_dstring_append(&ds, buf);
				break;

			case T_ROW:
				snprintf(buf, sizeof(buf), " $%d ",
						 assign_expr_param(yylval.row->dno,
										   params, &nparams));
				plpgsql_dstring_append(&ds, buf);
				break;

			case T_RECORD:
				snprintf(buf, sizeof(buf), " $%d ",
						 assign_expr_param(yylval.rec->dno,
										   params, &nparams));
				plpgsql_dstring_append(&ds, buf);
				break;

			default:
				plpgsql_dstring_append(&ds, yytext);
				break;
		}
	}

	if (endtoken)
		*endtoken = tok;

	expr = palloc(sizeof(PLpgSQL_expr) + sizeof(int) * nparams - sizeof(int));
	expr->dtype			= PLPGSQL_DTYPE_EXPR;
	expr->query			= pstrdup(plpgsql_dstring_get(&ds));
	expr->plan			= NULL;
	expr->nparams		= nparams;
	while(nparams-- > 0)
		expr->params[nparams] = params[nparams];
	plpgsql_dstring_free(&ds);

	if (valid_sql)
		check_sql_expr(expr->query);

	return expr;
}

static PLpgSQL_type *
read_datatype(int tok)
{
	int					lno;
	PLpgSQL_dstring		ds;
	char			   *type_name;
	PLpgSQL_type		*result;
	bool				needspace = false;
	int					parenlevel = 0;

	lno = plpgsql_scanner_lineno();

	/* Often there will be a lookahead token, but if not, get one */
	if (tok == YYEMPTY)
		tok = yylex();

	if (tok == T_DTYPE)
	{
		/* lexer found word%TYPE and did its thing already */
		return yylval.dtype;
	}

	plpgsql_dstring_init(&ds);

	while (tok != ';')
	{
		if (tok == 0)
		{
			if (parenlevel != 0)
				yyerror("mismatched parentheses");
			else
				yyerror("incomplete data type declaration");
		}
		/* Possible followers for datatype in a declaration */
		if (tok == K_NOT || tok == K_ASSIGN || tok == K_DEFAULT)
			break;
		/* Possible followers for datatype in a cursor_arg list */
		if ((tok == ',' || tok == ')') && parenlevel == 0)
			break;
		if (tok == '(')
			parenlevel++;
		else if (tok == ')')
			parenlevel--;
		if (needspace)
			plpgsql_dstring_append(&ds, " ");
		needspace = true;
		plpgsql_dstring_append(&ds, yytext);

		tok = yylex();
	}

	plpgsql_push_back_token(tok);

	type_name = plpgsql_dstring_get(&ds);

	if (type_name[0] == '\0')
		yyerror("missing data type declaration");

	plpgsql_error_lineno = lno;	/* in case of error in parse_datatype */

	result = plpgsql_parse_datatype(type_name);

	plpgsql_dstring_free(&ds);

	return result;
}

static PLpgSQL_stmt *
make_execsql_stmt(const char *sqlstart, int lineno)
{
	PLpgSQL_dstring		ds;
	int					nparams = 0;
	int					params[MAX_EXPR_PARAMS];
	char				buf[32];
	PLpgSQL_stmt_execsql *execsql;
	PLpgSQL_expr		*expr;
	PLpgSQL_row			*row = NULL;
	PLpgSQL_rec			*rec = NULL;
	int					tok;
	int					prev_tok;
	bool				have_into = false;
	bool				have_strict = false;

	plpgsql_dstring_init(&ds);
	plpgsql_dstring_append(&ds, sqlstart);

	/*
	 * We have to special-case the sequence INSERT INTO, because we don't want
	 * that to be taken as an INTO-variables clause.  Fortunately, this is the
	 * only valid use of INTO in a pl/pgsql SQL command, and INTO is already a
	 * fully reserved word in the main grammar.  We have to treat it that way
	 * anywhere in the string, not only at the start; consider CREATE RULE
	 * containing an INSERT statement.
	 */
	if (pg_strcasecmp(sqlstart, "insert") == 0)
		tok = K_INSERT;
	else
		tok = 0;

	for (;;)
	{
		prev_tok = tok;
		tok = yylex();
		if (tok == ';')
			break;
		if (tok == 0)
			yyerror("unexpected end of function definition");

		if (tok == K_INTO && prev_tok != K_INSERT)
		{
			if (have_into)
				yyerror("INTO specified more than once");
			have_into = true;
			read_into_target(&rec, &row, &have_strict);
			continue;
		}

		if (plpgsql_SpaceScanned)
			plpgsql_dstring_append(&ds, " ");

		switch (tok)
		{
			case T_SCALAR:
				snprintf(buf, sizeof(buf), " $%d ",
						 assign_expr_param(yylval.scalar->dno,
										   params, &nparams));
				plpgsql_dstring_append(&ds, buf);
				break;

			case T_ROW:
				snprintf(buf, sizeof(buf), " $%d ",
						 assign_expr_param(yylval.row->dno,
										   params, &nparams));
				plpgsql_dstring_append(&ds, buf);
				break;

			case T_RECORD:
				snprintf(buf, sizeof(buf), " $%d ",
						 assign_expr_param(yylval.rec->dno,
										   params, &nparams));
				plpgsql_dstring_append(&ds, buf);
				break;

			default:
				plpgsql_dstring_append(&ds, yytext);
				break;
		}
	}

	expr = palloc(sizeof(PLpgSQL_expr) + sizeof(int) * nparams - sizeof(int));
	expr->dtype			= PLPGSQL_DTYPE_EXPR;
	expr->query			= pstrdup(plpgsql_dstring_get(&ds));
	expr->plan			= NULL;
	expr->nparams		= nparams;
	while(nparams-- > 0)
		expr->params[nparams] = params[nparams];
	plpgsql_dstring_free(&ds);

	check_sql_expr(expr->query);

	execsql = palloc(sizeof(PLpgSQL_stmt_execsql));
	execsql->cmd_type = PLPGSQL_STMT_EXECSQL;
	execsql->lineno  = lineno;
	execsql->sqlstmt = expr;
	execsql->into	 = have_into;
	execsql->strict	 = have_strict;
	execsql->rec	 = rec;
	execsql->row	 = row;

	return (PLpgSQL_stmt *) execsql;
}


static PLpgSQL_stmt_fetch *
read_fetch_direction(void)
{
	PLpgSQL_stmt_fetch *fetch;
	int			tok;
	bool		check_FROM = true;

	/*
	 * We create the PLpgSQL_stmt_fetch struct here, but only fill in
	 * the fields arising from the optional direction clause
	 */
	fetch = (PLpgSQL_stmt_fetch *) palloc0(sizeof(PLpgSQL_stmt_fetch));
	fetch->cmd_type = PLPGSQL_STMT_FETCH;
	/* set direction defaults: */
	fetch->direction = FETCH_FORWARD;
	fetch->how_many  = 1;
	fetch->expr      = NULL;

	/*
	 * Most of the direction keywords are not plpgsql keywords, so we
	 * rely on examining yytext ...
	 */
	tok = yylex();
	if (tok == 0)
		yyerror("unexpected end of function definition");

	if (pg_strcasecmp(yytext, "next") == 0)
	{
		/* use defaults */
	}
	else if (pg_strcasecmp(yytext, "prior") == 0)
	{
		fetch->direction = FETCH_BACKWARD;
	}
	else if (pg_strcasecmp(yytext, "first") == 0)
	{
		fetch->direction = FETCH_ABSOLUTE;
	}
	else if (pg_strcasecmp(yytext, "last") == 0)
	{
		fetch->direction = FETCH_ABSOLUTE;
		fetch->how_many  = -1;
	}
	else if (pg_strcasecmp(yytext, "absolute") == 0)
	{
		fetch->direction = FETCH_ABSOLUTE;
		fetch->expr = read_sql_expression2(K_FROM, K_IN,
										   "FROM or IN",
										   NULL);
		check_FROM = false;
	}
	else if (pg_strcasecmp(yytext, "relative") == 0)
	{
		fetch->direction = FETCH_RELATIVE;
		fetch->expr = read_sql_expression2(K_FROM, K_IN,
										   "FROM or IN",
										   NULL);
		check_FROM = false;
	}
	else if (pg_strcasecmp(yytext, "forward") == 0)
	{
		/* use defaults */
	}
	else if (pg_strcasecmp(yytext, "backward") == 0)
	{
		fetch->direction = FETCH_BACKWARD;
	}
	else if (tok != T_SCALAR)
	{
		plpgsql_push_back_token(tok);
		fetch->expr = read_sql_expression2(K_FROM, K_IN,
										   "FROM or IN",
										   NULL);
		check_FROM = false;
	}
	else
	{
		/* Assume there's no direction clause */
		plpgsql_push_back_token(tok);
		check_FROM = false;
	}

	/* check FROM or IN keyword after direction's specification */
	if (check_FROM)
	{
		tok = yylex();
		if (tok != K_FROM && tok != K_IN)
			yyerror("expected FROM or IN");
	}

	return fetch;
}


static PLpgSQL_stmt *
make_return_stmt(int lineno)
{
	PLpgSQL_stmt_return *new;

	new = palloc0(sizeof(PLpgSQL_stmt_return));
	new->cmd_type = PLPGSQL_STMT_RETURN;
	new->lineno   = lineno;
	new->expr	  = NULL;
	new->retvarno = -1;

	if (plpgsql_curr_compile->fn_retset)
	{
		if (yylex() != ';')
			yyerror("RETURN cannot have a parameter in function "
					"returning set; use RETURN NEXT or RETURN QUERY");
	}
	else if (plpgsql_curr_compile->out_param_varno >= 0)
	{
		if (yylex() != ';')
			yyerror("RETURN cannot have a parameter in function with OUT parameters");
		new->retvarno = plpgsql_curr_compile->out_param_varno;
	}
	else if (plpgsql_curr_compile->fn_rettype == VOIDOID)
	{
		if (yylex() != ';')
			yyerror("RETURN cannot have a parameter in function returning void");
	}
	else if (plpgsql_curr_compile->fn_retistuple)
	{
		switch (yylex())
		{
			case K_NULL:
				/* we allow this to support RETURN NULL in triggers */
				break;

			case T_ROW:
				new->retvarno = yylval.row->dno;
				break;

			case T_RECORD:
				new->retvarno = yylval.rec->dno;
				break;

			default:
				yyerror("RETURN must specify a record or row variable in function returning row");
				break;
		}
		if (yylex() != ';')
			yyerror("RETURN must specify a record or row variable in function returning row");
	}
	else
	{
		/*
		 * Note that a well-formed expression is
		 * _required_ here; anything else is a
		 * compile-time error.
		 */
		new->expr = plpgsql_read_expression(';', ";");
	}

	return (PLpgSQL_stmt *) new;
}


static PLpgSQL_stmt *
make_return_next_stmt(int lineno)
{
	PLpgSQL_stmt_return_next *new;

	if (!plpgsql_curr_compile->fn_retset)
		yyerror("cannot use RETURN NEXT in a non-SETOF function");

	new = palloc0(sizeof(PLpgSQL_stmt_return_next));
	new->cmd_type	= PLPGSQL_STMT_RETURN_NEXT;
	new->lineno		= lineno;
	new->expr		= NULL;
	new->retvarno	= -1;

	if (plpgsql_curr_compile->out_param_varno >= 0)
	{
		if (yylex() != ';')
			yyerror("RETURN NEXT cannot have a parameter in function with OUT parameters");
		new->retvarno = plpgsql_curr_compile->out_param_varno;
	}
	else if (plpgsql_curr_compile->fn_retistuple)
	{
		switch (yylex())
		{
			case T_ROW:
				new->retvarno = yylval.row->dno;
				break;

			case T_RECORD:
				new->retvarno = yylval.rec->dno;
				break;

			default:
				yyerror("RETURN NEXT must specify a record or row variable in function returning row");
				break;
		}
		if (yylex() != ';')
			yyerror("RETURN NEXT must specify a record or row variable in function returning row");
	}
	else
		new->expr = plpgsql_read_expression(';', ";");

	return (PLpgSQL_stmt *) new;
}


static PLpgSQL_stmt *
make_return_query_stmt(int lineno)
{
	PLpgSQL_stmt_return_query *new;
	int			tok;

	if (!plpgsql_curr_compile->fn_retset)
		yyerror("cannot use RETURN QUERY in a non-SETOF function");

	new = palloc0(sizeof(PLpgSQL_stmt_return_query));
	new->cmd_type = PLPGSQL_STMT_RETURN_QUERY;
	new->lineno = lineno;

	/* check for RETURN QUERY EXECUTE */
	if ((tok = yylex()) != K_EXECUTE)
	{
		/* ordinary static query */
		plpgsql_push_back_token(tok);
		new->query = read_sql_stmt("");
	}
	else
	{
		/* dynamic SQL */
		int		term;

		new->dynquery = read_sql_expression2(';', K_USING, "; or USING",
											 &term);
		if (term == K_USING)
		{
			do
			{
				PLpgSQL_expr *expr;

				expr = read_sql_expression2(',', ';', ", or ;", &term);
				new->params = lappend(new->params, expr);
			} while (term == ',');
		}
	}

	return (PLpgSQL_stmt *) new;
}


static void
check_assignable(PLpgSQL_datum *datum)
{
	switch (datum->dtype)
	{
		case PLPGSQL_DTYPE_VAR:
			if (((PLpgSQL_var *) datum)->isconst)
			{
				plpgsql_error_lineno = plpgsql_scanner_lineno();
				ereport(ERROR,
						(errcode(ERRCODE_ERROR_IN_ASSIGNMENT),
						 errmsg("\"%s\" is declared CONSTANT",
								((PLpgSQL_var *) datum)->refname)));
			}
			break;
		case PLPGSQL_DTYPE_ROW:
			/* always assignable? */
			break;
		case PLPGSQL_DTYPE_REC:
			/* always assignable?  What about NEW/OLD? */
			break;
		case PLPGSQL_DTYPE_RECFIELD:
			/* always assignable? */
			break;
		case PLPGSQL_DTYPE_ARRAYELEM:
			/* always assignable? */
			break;
		case PLPGSQL_DTYPE_TRIGARG:
			yyerror("cannot assign to tg_argv");
			break;
		default:
			elog(ERROR, "unrecognized dtype: %d", datum->dtype);
			break;
	}
}

/*
 * Read the argument of an INTO clause.  On entry, we have just read the
 * INTO keyword.
 */
static void
read_into_target(PLpgSQL_rec **rec, PLpgSQL_row **row, bool *strict)
{
	int			tok;

	/* Set default results */
	*rec = NULL;
	*row = NULL;
	if (strict)
		*strict = false;

	tok = yylex();
	if (strict && tok == K_STRICT)
	{
		*strict = true;
		tok = yylex();
	}

	switch (tok)
	{
		case T_ROW:
			*row = yylval.row;
			check_assignable((PLpgSQL_datum *) *row);
			break;

		case T_RECORD:
			*rec = yylval.rec;
			check_assignable((PLpgSQL_datum *) *rec);
			break;

		case T_SCALAR:
			*row = read_into_scalar_list(yytext, yylval.scalar);
			break;

		default:
			plpgsql_error_lineno = plpgsql_scanner_lineno();
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error at \"%s\"", yytext),
					 errdetail("Expected record variable, row variable, "
							   "or list of scalar variables following INTO.")));
	}
}

/*
 * Given the first datum and name in the INTO list, continue to read
 * comma-separated scalar variables until we run out. Then construct
 * and return a fake "row" variable that represents the list of
 * scalars.
 */
static PLpgSQL_row *
read_into_scalar_list(const char *initial_name,
					  PLpgSQL_datum *initial_datum)
{
	int				 nfields;
	char			*fieldnames[1024];
	int				 varnos[1024];
	PLpgSQL_row		*row;
	int				 tok;

	check_assignable(initial_datum);
	fieldnames[0] = pstrdup(initial_name);
	varnos[0]	  = initial_datum->dno;
	nfields		  = 1;

	while ((tok = yylex()) == ',')
	{
		/* Check for array overflow */
		if (nfields >= 1024)
		{
			plpgsql_error_lineno = plpgsql_scanner_lineno();
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("too many INTO variables specified")));
		}

		tok = yylex();
		switch(tok)
		{
			case T_SCALAR:
				check_assignable(yylval.scalar);
				fieldnames[nfields] = pstrdup(yytext);
				varnos[nfields++]	= yylval.scalar->dno;
				break;

			default:
				plpgsql_error_lineno = plpgsql_scanner_lineno();
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("\"%s\" is not a scalar variable",
								yytext)));
		}
	}

	/*
	 * We read an extra, non-comma token from yylex(), so push it
	 * back onto the input stream
	 */
	plpgsql_push_back_token(tok);

	row = palloc(sizeof(PLpgSQL_row));
	row->dtype = PLPGSQL_DTYPE_ROW;
	row->refname = pstrdup("*internal*");
	row->lineno = plpgsql_scanner_lineno();
	row->rowtupdesc = NULL;
	row->nfields = nfields;
	row->fieldnames = palloc(sizeof(char *) * nfields);
	row->varnos = palloc(sizeof(int) * nfields);
	while (--nfields >= 0)
	{
		row->fieldnames[nfields] = fieldnames[nfields];
		row->varnos[nfields] = varnos[nfields];
	}

	plpgsql_adddatum((PLpgSQL_datum *)row);

	return row;
}

/*
 * Convert a single scalar into a "row" list.  This is exactly
 * like read_into_scalar_list except we never consume any input.
 * In fact, since this can be invoked long after the source
 * input was actually read, the lineno has to be passed in.
 */
static PLpgSQL_row *
make_scalar_list1(const char *initial_name,
				  PLpgSQL_datum *initial_datum,
				  int lineno)
{
	PLpgSQL_row		*row;

	check_assignable(initial_datum);

	row = palloc(sizeof(PLpgSQL_row));
	row->dtype = PLPGSQL_DTYPE_ROW;
	row->refname = pstrdup("*internal*");
	row->lineno = lineno;
	row->rowtupdesc = NULL;
	row->nfields = 1;
	row->fieldnames = palloc(sizeof(char *));
	row->varnos = palloc(sizeof(int));
	row->fieldnames[0] = pstrdup(initial_name);
	row->varnos[0] = initial_datum->dno;

	plpgsql_adddatum((PLpgSQL_datum *)row);

	return row;
}

/*
 * When the PL/PgSQL parser expects to see a SQL statement, it is very
 * liberal in what it accepts; for example, we often assume an
 * unrecognized keyword is the beginning of a SQL statement. This
 * avoids the need to duplicate parts of the SQL grammar in the
 * PL/PgSQL grammar, but it means we can accept wildly malformed
 * input. To try and catch some of the more obviously invalid input,
 * we run the strings we expect to be SQL statements through the main
 * SQL parser.
 *
 * We only invoke the raw parser (not the analyzer); this doesn't do
 * any database access and does not check any semantic rules, it just
 * checks for basic syntactic correctness. We do this here, rather
 * than after parsing has finished, because a malformed SQL statement
 * may cause the PL/PgSQL parser to become confused about statement
 * borders. So it is best to bail out as early as we can.
 */
static void
check_sql_expr(const char *stmt)
{
	ErrorContextCallback  syntax_errcontext;
	ErrorContextCallback *previous_errcontext;
	MemoryContext oldCxt;

	if (!plpgsql_check_syntax)
		return;

	/*
	 * Setup error traceback support for ereport(). The previous
	 * ereport callback is installed by pl_comp.c, but we don't want
	 * that to be invoked (since it will try to transpose the syntax
	 * error to be relative to the CREATE FUNCTION), so temporarily
	 * remove it from the list of callbacks.
	 */
	Assert(error_context_stack->callback == plpgsql_compile_error_callback);

	previous_errcontext = error_context_stack;
	syntax_errcontext.callback = plpgsql_sql_error_callback;
	syntax_errcontext.arg = (char *) stmt;
	syntax_errcontext.previous = error_context_stack->previous;
	error_context_stack = &syntax_errcontext;

	oldCxt = MemoryContextSwitchTo(compile_tmp_cxt);
	(void) raw_parser(stmt);
	MemoryContextSwitchTo(oldCxt);

	/* Restore former ereport callback */
	error_context_stack = previous_errcontext;
}

static void
plpgsql_sql_error_callback(void *arg)
{
	char *sql_stmt = (char *) arg;

	Assert(plpgsql_error_funcname);

	errcontext("SQL statement in PL/PgSQL function \"%s\" near line %d",
			   plpgsql_error_funcname, plpgsql_error_lineno);
	internalerrquery(sql_stmt);
	internalerrposition(geterrposition());
	errposition(0);
}

/*
 * Convert a string-literal token to the represented string value.
 *
 * To do this, we need to invoke the core lexer.  Here we are only concerned
 * with setting up the right errcontext state, which is handled the same as
 * in check_sql_expr().
 */
static char *
parse_string_token(const char *token)
{
	char	   *result;
	ErrorContextCallback  syntax_errcontext;
	ErrorContextCallback *previous_errcontext;

	/* See comments in check_sql_expr() */
	Assert(error_context_stack->callback == plpgsql_compile_error_callback);

	previous_errcontext = error_context_stack;
	syntax_errcontext.callback = plpgsql_string_error_callback;
	syntax_errcontext.arg = (char *) token;
	syntax_errcontext.previous = error_context_stack->previous;
	error_context_stack = &syntax_errcontext;

	result = pg_parse_string_token(token);

	/* Restore former ereport callback */
	error_context_stack = previous_errcontext;

	return result;
}

static void
plpgsql_string_error_callback(void *arg)
{
	Assert(plpgsql_error_funcname);

	errcontext("string literal in PL/PgSQL function \"%s\" near line %d",
			   plpgsql_error_funcname, plpgsql_error_lineno);
	/* representing the string literal as internalquery seems overkill */
	errposition(0);
}

static char *
check_label(const char *yytxt)
{
	char	   *label_name;

	plpgsql_convert_ident(yytxt, &label_name, 1);
	if (plpgsql_ns_lookup_label(label_name) == NULL)
		yyerror("label does not exist");
	return label_name;
}

static void
check_labels(const char *start_label, const char *end_label)
{
	if (end_label)
	{
		if (!start_label)
		{
			plpgsql_error_lineno = plpgsql_scanner_lineno();
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("end label \"%s\" specified for unlabelled block",
							end_label)));
		}

		if (strcmp(start_label, end_label) != 0)
		{
			plpgsql_error_lineno = plpgsql_scanner_lineno();
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("end label \"%s\" differs from block's label \"%s\"",
							end_label, start_label)));
		}
	}
}

/*
 * Read the arguments (if any) for a cursor, followed by the until token
 *
 * If cursor has no args, just swallow the until token and return NULL.
 * If it does have args, we expect to see "( expr [, expr ...] )" followed
 * by the until token.  Consume all that and return a SELECT query that
 * evaluates the expression(s) (without the outer parens).
 */
static PLpgSQL_expr *
read_cursor_args(PLpgSQL_var *cursor, int until, const char *expected)
{
	PLpgSQL_expr *expr;
	int			tok;
	char	   *cp;

	tok = yylex();
	if (cursor->cursor_explicit_argrow < 0)
	{
		/* No arguments expected */
		if (tok == '(')
		{
			plpgsql_error_lineno = plpgsql_scanner_lineno();
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cursor \"%s\" has no arguments",
							cursor->refname)));
		}

		if (tok != until)
		{
			plpgsql_error_lineno = plpgsql_scanner_lineno();
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("syntax error at \"%s\"",
							yytext)));
		}

		return NULL;
	}

	/* Else better provide arguments */
	if (tok != '(')
	{
		plpgsql_error_lineno = plpgsql_scanner_lineno();
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cursor \"%s\" has arguments",
						cursor->refname)));
	}

	/*
	 * Push back the '(', else plpgsql_read_expression
	 * will complain about unbalanced parens.
	 */
	plpgsql_push_back_token(tok);

	expr = plpgsql_read_expression(until, expected);

	/*
	 * Now remove the leading and trailing parens,
	 * because we want "SELECT 1, 2", not "SELECT (1, 2)".
	 */
	cp = expr->query;

	if (strncmp(cp, "SELECT", 6) != 0)
	{
		plpgsql_error_lineno = plpgsql_scanner_lineno();
		/* internal error */
		elog(ERROR, "expected \"SELECT (\", got \"%s\"", expr->query);
	}
	cp += 6;
	while (*cp == ' ')			/* could be more than 1 space here */
		cp++;
	if (*cp != '(')
	{
		plpgsql_error_lineno = plpgsql_scanner_lineno();
		/* internal error */
		elog(ERROR, "expected \"SELECT (\", got \"%s\"", expr->query);
	}
	*cp = ' ';

	cp += strlen(cp) - 1;

	if (*cp != ')')
		yyerror("expected \")\"");
	*cp = '\0';

	return expr;
}

/*
 * Parse RAISE ... USING options
 */
static List *
read_raise_options(void)
{
	List	   *result = NIL;

	for (;;)
	{
		PLpgSQL_raise_option *opt;
		int		tok;

		if ((tok = yylex()) == 0)
			yyerror("unexpected end of function definition");

		opt = (PLpgSQL_raise_option *) palloc(sizeof(PLpgSQL_raise_option));

		if (pg_strcasecmp(yytext, "errcode") == 0)
			opt->opt_type = PLPGSQL_RAISEOPTION_ERRCODE;
		else if (pg_strcasecmp(yytext, "message") == 0)
			opt->opt_type = PLPGSQL_RAISEOPTION_MESSAGE;
		else if (pg_strcasecmp(yytext, "detail") == 0)
			opt->opt_type = PLPGSQL_RAISEOPTION_DETAIL;
		else if (pg_strcasecmp(yytext, "hint") == 0)
			opt->opt_type = PLPGSQL_RAISEOPTION_HINT;
		else
		{
			plpgsql_error_lineno = plpgsql_scanner_lineno();
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized RAISE statement option \"%s\"",
							yytext)));
		}

		if (yylex() != K_ASSIGN)
			yyerror("syntax error, expected \"=\"");

		opt->expr = read_sql_expression2(',', ';', ", or ;", &tok);

		result = lappend(result, opt);

		if (tok == ';')
			break;
	}

	return result;
}

/*
 * Fix up CASE statement
 */
static PLpgSQL_stmt *
make_case(int lineno, PLpgSQL_expr *t_expr,
		  List *case_when_list, List *else_stmts)
{
	PLpgSQL_stmt_case 	*new;

	new = palloc(sizeof(PLpgSQL_stmt_case));
	new->cmd_type = PLPGSQL_STMT_CASE;
	new->lineno = lineno;
	new->t_expr = t_expr;
	new->t_varno = 0;
	new->case_when_list = case_when_list;
	new->have_else = (else_stmts != NIL);
	/* Get rid of list-with-NULL hack */
	if (list_length(else_stmts) == 1 && linitial(else_stmts) == NULL)
		new->else_stmts = NIL;
	else
		new->else_stmts = else_stmts;

	/*
	 * When test expression is present, we create a var for it and then
	 * convert all the WHEN expressions to "VAR IN (original_expression)".
	 * This is a bit klugy, but okay since we haven't yet done more than
	 * read the expressions as text.  (Note that previous parsing won't
	 * have complained if the WHEN ... THEN expression contained multiple
	 * comma-separated values.)
	 */
	if (t_expr)
	{
		ListCell *l;
		PLpgSQL_var *t_var;
		int		t_varno;

		/*
		 * We don't yet know the result datatype of t_expr.  Build the
		 * variable as if it were INT4; we'll fix this at runtime if needed.
		 */
		t_var = (PLpgSQL_var *)
			plpgsql_build_variable("*case*", lineno,
								   plpgsql_build_datatype(INT4OID, -1),
								   false);
		t_varno = t_var->dno;
		new->t_varno = t_varno;

		foreach(l, case_when_list)
		{
			PLpgSQL_case_when *cwt = (PLpgSQL_case_when *) lfirst(l);
			PLpgSQL_expr *expr = cwt->expr;
			int		nparams = expr->nparams;
			PLpgSQL_expr *new_expr;
			PLpgSQL_dstring ds;
			char	buff[32];

			/* Must add the CASE variable as an extra param to expression */
			if (nparams >= MAX_EXPR_PARAMS)
			{
				plpgsql_error_lineno = cwt->lineno;
				ereport(ERROR,
					    (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					     errmsg("too many variables specified in SQL statement")));
			}

			new_expr = palloc(sizeof(PLpgSQL_expr) + sizeof(int) * (nparams + 1) - sizeof(int));
			memcpy(new_expr, expr,
				   sizeof(PLpgSQL_expr) + sizeof(int) * nparams - sizeof(int));
			new_expr->nparams = nparams + 1;
			new_expr->params[nparams] = t_varno;

			/* And do the string hacking */
			plpgsql_dstring_init(&ds);

			plpgsql_dstring_append(&ds, "SELECT $");
			snprintf(buff, sizeof(buff), "%d", nparams + 1);
			plpgsql_dstring_append(&ds, buff);
			plpgsql_dstring_append(&ds, " IN (");

			/* copy expression query without SELECT keyword */
			Assert(strncmp(expr->query, "SELECT ", 7) == 0);
			plpgsql_dstring_append(&ds, expr->query + 7);
			plpgsql_dstring_append_char(&ds, ')');

			new_expr->query = pstrdup(plpgsql_dstring_get(&ds));

			plpgsql_dstring_free(&ds);
			pfree(expr->query);
			pfree(expr);

			cwt->expr = new_expr;
		}
	}

	return (PLpgSQL_stmt *) new;
}


/* Needed to avoid conflict between different prefix settings: */
#undef yylex

#include "pl_scan.c"
