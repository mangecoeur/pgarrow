/*
 * query.c - interaction to PostgreSQL server
 *
 * Copyright 2018-2019 (C) KaiGai Kohei <kaigai@heterodb.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the PostgreSQL License. See the LICENSE file.
 */
#include "pg2arrow.h"

#define atooid(x)		((Oid) strtoul((x), NULL, 10))
#define InvalidOid		((Oid) 0)

/* Dictionary Batch */
SQLdictionary  *pgsql_dictionary_list = NULL;
static int		pgsql_dictionary_count = 0;

/* forward declarations */
static SQLtable *
pgsql_create_composite_type(PGconn *conn, Oid comptype_relid);
static SQLattribute *
pgsql_create_array_element(PGconn *conn, Oid array_elemid,
						   int *p_numFieldNode,
						   int *p_numBuffers);
static inline bool
pg_strtobool(const char *v)
{
	if (strcasecmp(v, "t") == 0 ||
		strcasecmp(v, "true") == 0 ||
		strcmp(v, "1") == 0)
		return true;
	else if (strcasecmp(v, "f") == 0 ||
			 strcasecmp(v, "false") == 0 ||
			 strcmp(v, "0") == 0)
		return false;
	Elog("unexpected boolean type literal: %s", v);
}

static inline char
pg_strtochar(const char *v)
{
	if (strlen(v) == 0)
		Elog("unexpected empty string");
	if (strlen(v) > 1)
		Elog("unexpected character string");
	return *v;
}

static SQLdictionary *
pgsql_create_dictionary(PGconn *conn, Oid enum_typeid)
{
	SQLdictionary *dict;
	PGresult   *res;
	char		query[4096];
	int			i, j, nitems;
	int			nslots;

	for (dict = pgsql_dictionary_list; dict != NULL; dict = dict->next)
	{
		if (dict->enum_typeid == enum_typeid)
			return dict;
	}

	snprintf(query, sizeof(query),
			 "SELECT enumlabel"
			 "  FROM pg_catalog.pg_enum"
			 " WHERE enumtypid = %u", enum_typeid);
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		Elog("failed on pg_enum system catalog query: %s",
			 PQresultErrorMessage(res));

	nitems = PQntuples(res);
	nslots = Min(Max(nitems, 1<<10), 1<<18);
	dict = palloc0(offsetof(SQLdictionary, hslots[nslots]));
	dict->enum_typeid = enum_typeid;
	dict->dict_id = pgsql_dictionary_count++;
	sql_buffer_init(&dict->values);
	sql_buffer_init(&dict->extra);
	dict->nitems = nitems;
	dict->nslots = nslots;
	sql_buffer_append_zero(&dict->values, sizeof(int32));
	for (i=0; i < nitems; i++)
	{
		const char *enumlabel = PQgetvalue(res, i, 0);
		hashItem   *hitem;
		uint32		hash;
		size_t		len;

		if (PQgetisnull(res, i, 0) != 0)
			Elog("Unexpected result from pg_enum system catalog");

		len = strlen(enumlabel);
		hash = hash_any((const unsigned char *)enumlabel, len);
		j = hash % nslots;
		hitem = palloc0(offsetof(hashItem, label[len + 1]));
		strcpy(hitem->label, enumlabel);
		hitem->label_len = len;
		hitem->index = i;
		hitem->hash = hash;
		hitem->next = dict->hslots[j];
		dict->hslots[j] = hitem;

		sql_buffer_append(&dict->extra, enumlabel, len);
		sql_buffer_append(&dict->values, &dict->extra.usage, sizeof(int32));
	}
	dict->nitems = nitems;
	dict->next = pgsql_dictionary_list;
	pgsql_dictionary_list = dict;
	PQclear(res);

	return dict;
}

/*
 * pgsql_setup_attribute
 */
static void
pgsql_setup_attribute(PGconn *conn,
					  SQLattribute *attr,
					  const char *attname,
					  Oid atttypid,
					  int atttypmod,
					  int attlen,
					  char attbyval,
					  char attalign,
					  char typtype,
					  Oid comp_typrelid,
					  Oid array_elemid,
					  const char *nspname,
					  const char *typname,
					  int *p_numFieldNodes,
					  int *p_numBuffers)
{
	attr->attname   = pstrdup(attname);
	attr->atttypid  = atttypid;
	attr->atttypmod = atttypmod;
	attr->attlen    = attlen;
	attr->attbyval  = attbyval;

	if (attalign == 'c')
		attr->attalign = sizeof(char);
	else if (attalign == 's')
		attr->attalign = sizeof(short);
	else if (attalign == 'i')
		attr->attalign = sizeof(int);
	else if (attalign == 'd')
		attr->attalign = sizeof(double);
	else
		Elog("unknown state of attalign: %c", attalign);

	attr->typnamespace = pstrdup(nspname);
	attr->typname = pstrdup(typname);
	attr->typtype = typtype;
	if (typtype == 'b')
	{
		if (array_elemid != InvalidOid)
			attr->element = pgsql_create_array_element(conn, array_elemid,
													   p_numFieldNodes,
													   p_numBuffers);
	}
	else if (typtype == 'c')
	{
		/* composite data type */
		SQLtable   *subtypes;

		assert(comp_typrelid != 0);
		subtypes = pgsql_create_composite_type(conn, comp_typrelid);
		*p_numFieldNodes += subtypes->numFieldNodes;
		*p_numBuffers += subtypes->numBuffers;

		attr->subtypes = subtypes;
	}
	else if (typtype == 'e')
	{
		attr->enumdict = pgsql_create_dictionary(conn, atttypid);
	}
	else
		Elog("unknown state pf typtype: %c", typtype);

	/* init statistics */
	attr->min_isnull = true;
	attr->max_isnull = true;
	attr->min_value  = 0UL;
	attr->max_value  = 0UL;
	/* assign properties of Apache Arrow Type */
	assignArrowType(attr, p_numBuffers);
	*p_numFieldNodes += 1;
}

/*
 * pgsql_create_composite_type
 */
static SQLtable *
pgsql_create_composite_type(PGconn *conn, Oid comptype_relid)
{
	PGresult   *res;
	SQLtable   *table;
	char		query[4096];
	int			j, nfields;

	snprintf(query, sizeof(query),
			 "SELECT attname, attnum, atttypid, atttypmod, attlen,"
			 "       attbyval, attalign, typtype, typrelid, typelem,"
			 "       nspname, typname"
			 "  FROM pg_catalog.pg_attribute a,"
			 "       pg_catalog.pg_type t,"
			 "       pg_catalog.pg_namespace n"
			 " WHERE t.typnamespace = n.oid"
			 "   AND a.atttypid = t.oid"
			 "   AND a.attrelid = %u", comptype_relid);
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		Elog("failed on pg_type system catalog query: %s",
			 PQresultErrorMessage(res));

	nfields = PQntuples(res);
	table = palloc0(offsetof(SQLtable, attrs[nfields]));
	table->nfields = nfields;
	for (j=0; j < nfields; j++)
	{
		const char *attname   = PQgetvalue(res, j, 0);
		const char *attnum    = PQgetvalue(res, j, 1);
		const char *atttypid  = PQgetvalue(res, j, 2);
		const char *atttypmod = PQgetvalue(res, j, 3);
		const char *attlen    = PQgetvalue(res, j, 4);
		const char *attbyval  = PQgetvalue(res, j, 5);
		const char *attalign  = PQgetvalue(res, j, 6);
		const char *typtype   = PQgetvalue(res, j, 7);
		const char *typrelid  = PQgetvalue(res, j, 8);
		const char *typelem   = PQgetvalue(res, j, 9);
		const char *nspname   = PQgetvalue(res, j, 10);
		const char *typname   = PQgetvalue(res, j, 11);
		int			index     = atoi(attnum);

		if (index < 1 || index > nfields)
			Elog("attribute number is out of range");
		pgsql_setup_attribute(conn,
							  &table->attrs[index-1],
							  attname,
							  atooid(atttypid),
							  atoi(atttypmod),
							  atoi(attlen),
							  pg_strtobool(attbyval),
							  pg_strtochar(attalign),
							  pg_strtochar(typtype),
							  atooid(typrelid),
							  atooid(typelem),
							  nspname, typname,
							  &table->numFieldNodes,
							  &table->numBuffers);
	}
	return table;
}

static SQLattribute *
pgsql_create_array_element(PGconn *conn, Oid array_elemid,
						   int *p_numFieldNode,
						   int *p_numBuffers)
{
	SQLattribute   *attr = palloc0(sizeof(SQLattribute));
	PGresult	   *res;
	char			query[4096];
	const char     *nspname;
	const char	   *typname;
	const char	   *typlen;
	const char	   *typbyval;
	const char	   *typalign;
	const char	   *typtype;
	const char	   *typrelid;
	const char	   *typelem;

	snprintf(query, sizeof(query),
			 "SELECT nspname, typname,"
			 "       typlen, typbyval, typalign, typtype,"
			 "       typrelid, typelem"
			 "  FROM pg_catalog.pg_type t,"
			 "       pg_catalog.pg_namespace n"
			 " WHERE t.typnamespace = n.oid"
			 "   AND t.oid = %u", array_elemid);
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		Elog("failed on pg_type system catalog query: %s",
			 PQresultErrorMessage(res));
	if (PQntuples(res) != 1)
		Elog("unexpected number of result rows: %d", PQntuples(res));
	nspname  = PQgetvalue(res, 0, 0);
	typname  = PQgetvalue(res, 0, 1);
	typlen   = PQgetvalue(res, 0, 2);
	typbyval = PQgetvalue(res, 0, 3);
	typalign = PQgetvalue(res, 0, 4);
	typtype  = PQgetvalue(res, 0, 5);
	typrelid = PQgetvalue(res, 0, 6);
	typelem  = PQgetvalue(res, 0, 7);

	pgsql_setup_attribute(conn,
						  attr,
						  typname,
						  array_elemid,
						  -1,
						  atoi(typlen),
						  pg_strtobool(typbyval),
						  pg_strtochar(typalign),
						  pg_strtochar(typtype),
						  atooid(typrelid),
						  atooid(typelem),
						  nspname,
						  typname,
						  p_numFieldNode,
						  p_numBuffers);
	return attr;
}

/*
 * pgsql_create_buffer
 */
SQLtable *
pgsql_create_buffer(PGconn *conn, PGresult *res, size_t segment_sz)
{
	int			j, nfields = PQnfields(res);
	SQLtable   *table;

	table = palloc0(offsetof(SQLtable, attrs[nfields]));
	table->segment_sz = segment_sz;
	table->nitems = 0;
	table->nfields = nfields;
	for (j=0; j < nfields; j++)
	{
		const char *attname = PQfname(res, j);
		Oid			atttypid = PQftype(res, j);
		int			atttypmod = PQfmod(res, j);
		PGresult   *__res;
		char		query[4096];
		const char *typlen;
		const char *typbyval;
		const char *typalign;
		const char *typtype;
		const char *typrelid;
		const char *typelem;
		const char *nspname;
		const char *typname;

		snprintf(query, sizeof(query),
				 "SELECT typlen, typbyval, typalign, typtype,"
				 "       typrelid, typelem, nspname, typname"
				 "  FROM pg_catalog.pg_type t,"
				 "       pg_catalog.pg_namespace n"
				 " WHERE t.typnamespace = n.oid"
				 "   AND t.oid = %u", atttypid);
		__res = PQexec(conn, query);
		if (PQresultStatus(__res) != PGRES_TUPLES_OK)
			Elog("failed on pg_type system catalog query: %s",
				 PQresultErrorMessage(res));
		if (PQntuples(__res) != 1)
			Elog("unexpected number of result rows: %d", PQntuples(__res));
		typlen   = PQgetvalue(__res, 0, 0);
		typbyval = PQgetvalue(__res, 0, 1);
		typalign = PQgetvalue(__res, 0, 2);
		typtype  = PQgetvalue(__res, 0, 3);
		typrelid = PQgetvalue(__res, 0, 4);
		typelem  = PQgetvalue(__res, 0, 5);
		nspname  = PQgetvalue(__res, 0, 6);
		typname  = PQgetvalue(__res, 0, 7);
		pgsql_setup_attribute(conn,
							  &table->attrs[j],
                              attname,
							  atttypid,
							  atttypmod,
							  atoi(typlen),
							  *typbyval,
							  *typalign,
							  *typtype,
							  atoi(typrelid),
							  atoi(typelem),
							  nspname, typname,
							  &table->numFieldNodes,
							  &table->numBuffers);
		PQclear(__res);
	}
	return table;
}

/*
 * pgsql_clear_attribute
 */
static void
pgsql_clear_attribute(SQLattribute *attr)
{
	attr->nitems = 0;
	attr->nullcount = 0;
	sql_buffer_clear(&attr->nullmap);
	sql_buffer_clear(&attr->values);
	sql_buffer_clear(&attr->extra);

	if (attr->subtypes)
	{
		SQLtable   *subtypes = attr->subtypes;
		int			j;

		for (j=0; j < subtypes->nfields; j++)
			pgsql_clear_attribute(&subtypes->attrs[j]);
	}
	if (attr->element)
		pgsql_clear_attribute(attr->element);
	/* clear statistics */
	attr->min_isnull = true;
	attr->max_isnull = true;
	attr->min_value  = 0UL;
	attr->max_value  = 0UL;
}

/*
 * pgsql_writeout_buffer
 */
void
pgsql_writeout_buffer(SQLtable *table)
{
	off_t		currPos;
	size_t		metaSize;
	size_t		bodySize;
	int			j, index;
	ArrowBlock *b;

	/* write a new record batch */
	currPos = lseek(table->fdesc, 0, SEEK_CUR);
	if (currPos < 0)
		Elog("unable to get current position of the file");
	writeArrowRecordBatch(table, &metaSize, &bodySize);

	index = table->numRecordBatches++;
	if (index == 0)
		table->recordBatches = palloc(sizeof(ArrowBlock));
	else
		table->recordBatches = repalloc(table->recordBatches,
										sizeof(ArrowBlock) * (index+1));
	b = &table->recordBatches[index];
	b->tag = ArrowNodeTag__Block;
	b->offset = currPos;
	b->metaDataLength = metaSize;
	b->bodyLength = bodySize;

	/* shows progress (optional) */
	if (shows_progress)
	{
		printf("RecordBatch %d: offset=%lu length=%lu (meta=%zu, body=%zu)\n",
			   index, currPos, metaSize + bodySize, metaSize, bodySize);
	}

	/* makes table/attributes empty again */
	table->nitems = 0;
	for (j=0; j < table->nfields; j++)
		pgsql_clear_attribute(&table->attrs[j]);
}

/*
 * pgsql_append_results
 */
void
pgsql_append_results(SQLtable *table, PGresult *res)
{
	int		i, ntuples = PQntuples(res);
	int		j, nfields = PQnfields(res);
	size_t	usage;

	assert(nfields == table->nfields);
	for (i=0; i < ntuples; i++)
	{
		usage = 0;

		for (j=0; j < nfields; j++)
		{
			SQLattribute   *attr = &table->attrs[j];
			const char	   *addr;
			size_t			sz;
			/* data must be binary format */
			assert(PQfformat(res, j) == 1);
			if (PQgetisnull(res, i, j))
			{
				addr = NULL;
				sz = 0;
			}
			else
			{
				addr = PQgetvalue(res, i, j);
				sz = PQgetlength(res, i, j);
			}
			assert(attr->nitems == table->nitems);
			attr->put_value(attr, addr, sz);
			if (attr->stat_update)
				attr->stat_update(attr, addr, sz);
			usage += attr->buffer_usage(attr);
		}
		table->nitems++;
		/* exceeds the threshold to write? */
		if (usage > table->segment_sz)
		{
			if (table->nitems == 0)
				Elog("A result row is larger than size of record batch!!");
			pgsql_writeout_buffer(table);
		}
	}
}

/*
 * pgsql_dump_attribute
 */
static void
pgsql_dump_attribute(SQLattribute *attr, const char *label, int indent)
{
	int		j;

	for (j=0; j < indent; j++)
		putchar(' ');
	printf("%s {attname='%s', atttypid=%u, atttypmod=%d, attlen=%d,"
		   " attbyval=%s, attalign=%d, typtype=%c, arrow_type=",
		   label,
		   attr->attname, attr->atttypid, attr->atttypmod, attr->attlen,
		   attr->attbyval ? "true" : "false", attr->attalign, attr->typtype);
	dumpArrowNode((ArrowNode *)&attr->arrow_type, stdout);
	printf("}\n");

	if (attr->typtype == 'b')
	{
		if (attr->element)
			pgsql_dump_attribute(attr->element, "element", indent+2);
	}
	else if (attr->typtype == 'c')
	{
		SQLtable   *subtypes = attr->subtypes;
		char		label[64];

		for (j=0; j < subtypes->nfields; j++)
		{
			snprintf(label, sizeof(label), "subtype[%d]", j);
			pgsql_dump_attribute(&subtypes->attrs[j], label, indent+2);
		}
	}
}

/*
 * pgsql_dump_buffer
 */
void
pgsql_dump_buffer(SQLtable *table)
{
	int		j;
	char	label[64];

	printf("Dump of SQL buffer:\n"
		   "nfields: %d\n"
		   "nitems: %zu\n",
		   table->nfields,
		   table->nitems);
	for (j=0; j < table->nfields; j++)
	{
		snprintf(label, sizeof(label), "attr[%d]", j);
		pgsql_dump_attribute(&table->attrs[j], label, 0);
	}
}
