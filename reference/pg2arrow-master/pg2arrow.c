/*
 * pg2arrow.c - main logic of the command
 *
 * Copyright 2018-2019 (C) KaiGai Kohei <kaigai@heterodb.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the PostgreSQL License. See the LICENSE file.
 */
#include "pg2arrow.h"

/* static functions */
#define CURSOR_NAME		"curr_pg2arrow"
static PGresult *pgsql_begin_query(PGconn *conn, const char *query);
static PGresult *pgsql_next_result(PGconn *conn);
static void      pgsql_end_query(PGconn *conn);

/* command options */
static char	   *sql_command = NULL;
static char	   *output_filename = NULL;
static size_t	batch_segment_sz = 0;
static char	   *pgsql_hostname = NULL;
static char	   *pgsql_portno = NULL;
static char	   *pgsql_username = NULL;
static int		pgsql_password_prompt = 0;
static char	   *pgsql_database = NULL;
static char	   *dump_arrow_filename = NULL;
int				shows_progress = 0;

static void
usage(void)
{
	fputs("Usage:\n"
		  "  pg2arrow [OPTION]... [DBNAME [USERNAME]]\n"
		  "\n"
		  "General options:\n"
		  "  -d, --dbname=DBNAME     database name to connect to\n"
		  "  -c, --command=COMMAND   SQL command to run\n"
		  "  -f, --file=FILENAME     SQL command from file\n"
		  "      (-c and -f are exclusive, either of them must be specified)\n"
		  "  -o, --output=FILENAME   result file in Apache Arrow format\n"
		  "      (default creates a temporary file)\n"
		  "\n"
		  "Arrow format options:\n"
		  "  -s, --segment-size=SIZE size of record batch for each\n"
		  "      (default: 256MB)\n"
		  "\n"
		  "Connection options:\n"
		  "  -h, --host=HOSTNAME     database server host\n"
		  "  -p, --port=PORT         database server port\n"
		  "  -U, --username=USERNAME database user name\n"
		  "  -w, --no-password       never prompt for password\n"
		  "  -W, --password          force password prompt\n"
		  "\n"
		  "Debug options:\n"
		  "      --dump=FILENAME     dump information of arrow file\n"
		  "      --progress          shows progress of the job.\n"
		  "\n"
		  "Report bugs to <pgstrom@heterodb.com>.\n",
		  stderr);
	exit(1);
}

static void
parse_options(int argc, char * const argv[])
{
	static struct option long_options[] = {
		{"dbname",       required_argument,  NULL,  'd' },
		{"command",      required_argument,  NULL,  'c' },
		{"file",         required_argument,  NULL,  'f' },
		{"output",       required_argument,  NULL,  'o' },
		{"segment-size", required_argument,  NULL,  's' },
		{"host",         required_argument,  NULL,  'h' },
		{"port",         required_argument,  NULL,  'p' },
		{"username",     required_argument,  NULL,  'U' },
		{"no-password",  no_argument,        NULL,  'w' },
		{"password",     no_argument,        NULL,  'W' },
		{"dump",         required_argument,  NULL, 1000 },
		{"progress",     no_argument,        NULL, 1001 },
		{"help",         no_argument,        NULL, 9999 },
		{NULL, 0, NULL, 0},
	};
	int			c;
	char	   *pos;
	char	   *sql_file = NULL;

	while ((c = getopt_long(argc, argv, "d:c:f:o:s:n:dh:p:U:wW",
							long_options, NULL)) >= 0)
	{
		switch (c)
		{
			case 'd':
				if (pgsql_database)
					Elog("-d option specified twice");
				pgsql_database = optarg;
				break;
			case 'c':
				if (sql_command)
					Elog("-c option specified twice");
				if (sql_file)
					Elog("-c and -f options are exclusive");
				sql_command = optarg;
				break;
			case 'f':
				if (sql_file)
					Elog("-f option specified twice");
				if (sql_command)
					Elog("-c and -f options are exclusive");
				sql_file = optarg;
				break;
			case 'o':
				if (output_filename)
					Elog("-o option specified twice");
				output_filename = optarg;
				break;
			case 's':
				if (batch_segment_sz != 0)
					Elog("-s option specified twice");
				pos = optarg;
				while (isdigit(*pos))
					pos++;
				if (*pos == '\0')
					batch_segment_sz = atol(optarg);
				else if (strcasecmp(pos, "k") == 0 ||
						 strcasecmp(pos, "kb") == 0)
					batch_segment_sz = atol(optarg) * (1UL << 10);
				else if (strcasecmp(pos, "m") == 0 ||
						 strcasecmp(pos, "mb") == 0)
					batch_segment_sz = atol(optarg) * (1UL << 20);
				else if (strcasecmp(pos, "g") == 0 ||
						 strcasecmp(pos, "gb") == 0)
					batch_segment_sz = atol(optarg) * (1UL << 30);
				else
					Elog("segment size is not valid: %s", optarg);
				break;
			case 'h':
				if (pgsql_hostname)
					Elog("-h option specified twice");
				pgsql_hostname = optarg;
				break;
			case 'p':
				if (pgsql_portno)
					Elog("-p option specified twice");
				pgsql_portno = optarg;
				break;
			case 'U':
				if (pgsql_username)
					Elog("-U option specified twice");
				pgsql_username = optarg;
				break;
			case 'w':
				if (pgsql_password_prompt > 0)
					Elog("-w and -W options are exclusive");
				pgsql_password_prompt = -1;
				break;
			case 'W':
				if (pgsql_password_prompt < 0)
					Elog("-w and -W options are exclusive");
				pgsql_password_prompt = 1;
				break;
			case 1000:		/* --dump */
				if (dump_arrow_filename)
					Elog("--dump option specified twice");
				dump_arrow_filename = optarg;
				break;
			case 1001:		/* --progress */
				shows_progress = 1;
				break;
			case 9999:		/* --help */
			default:
				usage();
				break;
		}
	}
	if (optind + 1 == argc)
	{
		if (pgsql_database)
			Elog("database name was specified twice");
		pgsql_database = argv[optind];
	}
	else if (optind + 2 == argc)
	{
		if (pgsql_database)
			Elog("database name was specified twice");
		if (pgsql_username)
			Elog("database user was specified twice");
		pgsql_database = argv[optind];
		pgsql_username = argv[optind + 1];
	}
	else if (optind != argc)
		Elog("Too much command line arguments");
	/*
	 * special code path if '--dump' option is specified.
	 */
	if (dump_arrow_filename)
	{
		readArrowFile(dump_arrow_filename);
		exit(0);
	}
	if (batch_segment_sz == 0)
		batch_segment_sz = (1UL << 28);		/* 256MB in default */
	if (sql_file)
	{
		int			fdesc;
		char	   *buffer;
		struct stat	st_buf;
		ssize_t		nbytes, offset = 0;

		assert(!sql_command);
		fdesc = open(sql_file, O_RDONLY);
		if (fdesc < 0)
			Elog("failed on open '%s': %m", sql_file);
		if (fstat(fdesc, &st_buf) != 0)
			Elog("failed on fstat(2) on '%s': %m", sql_file);
		buffer = palloc(st_buf.st_size + 1);
		while (offset < st_buf.st_size)
		{
			nbytes = read(fdesc, buffer + offset, st_buf.st_size - offset);
			if (nbytes < 0)
			{
				if (errno != EINTR)
					Elog("failed on read('%s'): %m", sql_file);
			}
			else if (nbytes == 0)
				break;
		}
		buffer[offset] = '\0';

		sql_command = buffer;
	}
	else if (!sql_command)
		Elog("Neither -c nor -f options are specified");
}

static PGconn *
pgsql_server_connect(void)
{
	PGconn	   *conn;
	const char *keys[20];
	const char *values[20];
	int			index = 0;
	int			status;

	if (pgsql_hostname)
	{
		keys[index] = "host";
		values[index] = pgsql_hostname;
		index++;
	}
	if (pgsql_portno)
	{
		keys[index] = "port";
		values[index] = pgsql_portno;
		index++;
	}
	if (pgsql_database)
	{
		keys[index] = "dbname";
		values[index] = pgsql_database;
		index++;
	}
	if (pgsql_username)
	{
		keys[index] = "user";
		values[index] = pgsql_username;
		index++;
	}
	if (pgsql_password_prompt > 0)
	{
		keys[index] = "password";
		values[index] = getpass("Password: ");
		index++;
	}
	keys[index] = "application_name";
	values[index] = "pg2arrow";
	index++;
	/* terminal */
	keys[index] = NULL;
	values[index] = NULL;

	conn = PQconnectdbParams(keys, values, 0);
	if (!conn)
		Elog("out of memory");
	status = PQstatus(conn);
	if (status != CONNECTION_OK)
		Elog("failed on PostgreSQL connection: %s",
			 PQerrorMessage(conn));
	return conn;
}

/*
 * pgsql_begin_query
 */
static PGresult *
pgsql_begin_query(PGconn *conn, const char *query)
{
	PGresult   *res;
	char	   *buffer;

	/* set transaction read-only */
	res = PQexec(conn, "BEGIN READ ONLY");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		Elog("unable to begin transaction: %s", PQresultErrorMessage(res));
	PQclear(res);

	/* declare cursor */
	buffer = palloc(strlen(query) + 2048);
	sprintf(buffer, "DECLARE %s BINARY CURSOR FOR %s", CURSOR_NAME, query);
	res = PQexec(conn, buffer);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		Elog("unable to declare a SQL cursor: %s", PQresultErrorMessage(res));
	PQclear(res);

	return pgsql_next_result(conn);
}

/*
 * pgsql_next_result
 */
static PGresult *
pgsql_next_result(PGconn *conn)
{
	PGresult   *res;
	/* fetch results per half million rows */
	res = PQexecParams(conn,
					   "FETCH FORWARD 500000 FROM " CURSOR_NAME,
					   0, NULL, NULL, NULL, NULL,
					   1);	/* results in binary mode */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		Elog("SQL execution failed: %s", PQresultErrorMessage(res));
	if (PQntuples(res) == 0)
	{
		PQclear(res);
		return NULL;
	}
	return res;
}

/*
 * pgsql_end_query
 */
static void
pgsql_end_query(PGconn *conn)
{
	PGresult   *res;
	/* close the cursor */
	res = PQexec(conn, "CLOSE " CURSOR_NAME);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		Elog("failed on close cursor '%s': %s", CURSOR_NAME,
			 PQresultErrorMessage(res));
	PQclear(res);

	/* close the connection */
	PQfinish(conn);
}

/*
 * setupArrowFieldNode
 */
static int
setupArrowFieldNode(ArrowFieldNode *node, SQLattribute *attr)
{
	SQLtable   *subtypes = attr->subtypes;
	SQLattribute *element = attr->element;
	int			i, count = 1;

	memset(node, 0, sizeof(ArrowFieldNode));
	node->tag = ArrowNodeTag__FieldNode;
	node->length = attr->nitems;
	node->null_count = attr->nullcount;
	/* array types */
	if (element)
		count += setupArrowFieldNode(node + count, element);
	/* composite types */
	if (subtypes)
	{
		for (i=0; i < subtypes->nfields; i++)
			count += setupArrowFieldNode(node + count, &subtypes->attrs[i]);
	}
	return count;
}

void
writeArrowRecordBatch(SQLtable *table,
					  size_t *p_metaLength,
					  size_t *p_bodyLength)
{
	ArrowMessage	message;
	ArrowRecordBatch *rbatch;
	ArrowFieldNode *nodes;
	ArrowBuffer	   *buffers;
	int32			i, j;
	size_t			metaLength;
	size_t			bodyLength = 0;

	/* fill up [nodes] vector */
	nodes = alloca(sizeof(ArrowFieldNode) * table->numFieldNodes);
	for (i=0, j=0; i < table->nfields; i++)
	{
		SQLattribute   *attr = &table->attrs[i];
		assert(table->nitems == attr->nitems);
		j += setupArrowFieldNode(nodes + j, attr);
	}
	assert(j == table->numFieldNodes);

	/* fill up [buffers] vector */
	buffers = alloca(sizeof(ArrowBuffer) * table->numBuffers);
	for (i=0, j=0; i < table->nfields; i++)
	{
		SQLattribute   *attr = &table->attrs[i];
		j += attr->setup_buffer(attr, buffers+j, &bodyLength);
	}
	assert(j == table->numBuffers);

	/* setup Message of Schema */
	memset(&message, 0, sizeof(ArrowMessage));
	message.tag = ArrowNodeTag__Message;
	message.version = ArrowMetadataVersion__V4;
	message.bodyLength = bodyLength;

	rbatch = &message.body.recordBatch;
	rbatch->tag = ArrowNodeTag__RecordBatch;
	rbatch->length = table->nitems;
	rbatch->nodes = nodes;
	rbatch->_num_nodes = table->numFieldNodes;
	rbatch->buffers = buffers;
	rbatch->_num_buffers = table->numBuffers;
	/* serialization */
	metaLength = writeFlatBufferMessage(table->fdesc, &message);
	for (i=0; i < table->nfields; i++)
	{
		SQLattribute   *attr = &table->attrs[i];
		attr->write_buffer(attr, table->fdesc);
	}
	*p_metaLength = metaLength;
	*p_bodyLength = bodyLength;
}

static void
setupArrowDictionaryEncoding(ArrowDictionaryEncoding *dict,
							 SQLattribute *attr)
{
	dict->tag = ArrowNodeTag__DictionaryEncoding;
	if (attr->enumdict)
	{
		dict->id = attr->enumdict->dict_id;
		dict->indexType.tag = ArrowNodeTag__Int;
		dict->indexType.bitWidth = 32;	/* OID in PostgreSQL */
		dict->indexType.is_signed = true;
		dict->isOrdered = false;
	}
}

static void
setupArrowField(ArrowField *field, SQLattribute *attr)
{
	memset(field, 0, sizeof(ArrowField));
	field->tag = ArrowNodeTag__Field;
	field->name = attr->attname;
	field->_name_len = strlen(attr->attname);
	field->nullable = true;
	field->type = attr->arrow_type;
	setupArrowDictionaryEncoding(&field->dictionary, attr);
	/* array type */
	if (attr->element)
	{
		field->children = palloc0(sizeof(ArrowField));
		field->_num_children = 1;
		setupArrowField(field->children, attr->element);
	}
	/* composite type */
	if (attr->subtypes)
	{
		SQLtable   *sub = attr->subtypes;
		int			i;

		field->children = palloc0(sizeof(ArrowField) * sub->nfields);
		field->_num_children = sub->nfields;
		for (i=0; i < sub->nfields; i++)
			setupArrowField(&field->children[i], &sub->attrs[i]);
	}
	//custom_metadata here?
	//min_values,max_values
}

static ssize_t
writeArrowSchema(SQLtable *table)
{
	ArrowMessage	message;
	ArrowSchema	   *schema;
	int32			i;

	/* setup Message of Schema */
	memset(&message, 0, sizeof(ArrowMessage));
	message.tag = ArrowNodeTag__Message;
	message.version = ArrowMetadataVersion__V4;
	schema = &message.body.schema;
	schema->tag = ArrowNodeTag__Schema;
	schema->endianness = ArrowEndianness__Little;
	schema->fields = alloca(sizeof(ArrowField) * table->nfields);
	schema->_num_fields = table->nfields;
	for (i=0; i < table->nfields; i++)
		setupArrowField(&schema->fields[i], &table->attrs[i]);
	/* serialization */
	return writeFlatBufferMessage(table->fdesc, &message);
}

static void
__writeArrowDictionaryBatch(int fdesc, ArrowBlock *block, SQLdictionary *dict)
{
	ArrowMessage	message;
	ArrowDictionaryBatch *dbatch;
	ArrowRecordBatch *rbatch;
	ArrowBuffer	   *buffer;
	loff_t			currPos;
	size_t			metaLength = 0;
	size_t			bodyLength = 0;

	/* setup Message of DictionaryBatch */
	memset(&message, 0, sizeof(ArrowMessage));
    message.tag = ArrowNodeTag__Message;
    message.version = ArrowMetadataVersion__V4;

	/* DictionaryBatch portion */
	dbatch = &message.body.dictionaryBatch;
	dbatch->tag = ArrowNodeTag__DictionaryBatch;
	dbatch->id = dict->dict_id;
	dbatch->isDelta = false;

	/* RecordBatch portion */
	rbatch = &dbatch->data;
	rbatch->tag = ArrowNodeTag__RecordBatch;
	rbatch->length = dict->nitems;
	rbatch->_num_nodes = 1;
    rbatch->nodes = alloca(sizeof(ArrowFieldNode));
	rbatch->nodes[0].tag = ArrowNodeTag__FieldNode;
	rbatch->nodes[0].length = dict->nitems;
	rbatch->nodes[0].null_count = 0;
	rbatch->_num_buffers = 3;	/* empty nullmap + offset + extra buffer */
	rbatch->buffers = alloca(sizeof(ArrowBuffer) * 3);
	/* buffer:0 - nullmap */
	buffer = &rbatch->buffers[0];
	buffer->tag = ArrowNodeTag__Buffer;
	buffer->offset = bodyLength;
	buffer->length = 0;
	/* buffer:1 - offset to extra buffer */
	buffer = &rbatch->buffers[1];
    buffer->tag = ArrowNodeTag__Buffer;
    buffer->offset = bodyLength;
    buffer->length = ARROWALIGN(dict->values.usage);
	bodyLength += buffer->length;
	/* buffer:2 - extra buffer */
	buffer = &rbatch->buffers[2];
	buffer->tag = ArrowNodeTag__Buffer;
    buffer->offset = bodyLength;
	buffer->length = ARROWALIGN(dict->extra.usage);
	bodyLength += buffer->length;

	/* serialization */
	message.bodyLength = bodyLength;
	currPos = lseek(fdesc, 0, SEEK_CUR);
	if (currPos < 0)
		Elog("unable to get current position of the file");
	metaLength = writeFlatBufferMessage(fdesc, &message);
	__write_buffer_common(fdesc, dict->values.ptr, dict->values.usage);
	__write_buffer_common(fdesc, dict->extra.ptr,  dict->extra.usage);

	/* setup Block of Footer */
	block->tag = ArrowNodeTag__Block;
	block->offset = currPos;
	block->metaDataLength = metaLength;
	block->bodyLength = bodyLength;
}

static void
writeArrowDictionaryBatches(SQLtable *table)
{
	SQLdictionary  *dict;
	int				index, count;

	if (!pgsql_dictionary_list)
		return;

	for (dict = pgsql_dictionary_list, count=0;
		 dict != NULL;
		 dict = dict->next, count++);
	table->numDictionaries = count;
	table->dictionaries = palloc0(sizeof(ArrowBlock) * count);

	for (dict = pgsql_dictionary_list, index=0;
		 dict != NULL;
		 dict = dict->next, index++)
	{
		__writeArrowDictionaryBatch(table->fdesc,
									table->dictionaries + index,
									dict);
	}
}

static ssize_t
writeArrowFooter(SQLtable *table)
{
	ArrowFooter		footer;
	ArrowSchema	   *schema;
	int				i;

	/* setup Footer */
	memset(&footer, 0, sizeof(ArrowFooter));
	footer.tag = ArrowNodeTag__Footer;
	footer.version = ArrowMetadataVersion__V4;
	/* setup Schema of Footer */
	schema = &footer.schema;
	schema->tag = ArrowNodeTag__Schema;
	schema->endianness = ArrowEndianness__Little;
	schema->fields = alloca(sizeof(ArrowField) * table->nfields);
	schema->_num_fields = table->nfields;
	for (i=0; i < table->nfields; i++)
		setupArrowField(&schema->fields[i], &table->attrs[i]);
	/* [dictionaries] */
	footer.dictionaries = table->dictionaries;
	footer._num_dictionaries = table->numDictionaries;

	/* [recordBatches] */
	footer.recordBatches = table->recordBatches;
	footer._num_recordBatches = table->numRecordBatches;

	/* serialization */
	return writeFlatBufferFooter(table->fdesc, &footer);
}

/*
 * Entrypoint of pg2arrow
 */
int main(int argc, char * const argv[])
{
	PGconn	   *conn;
	PGresult   *res;
	SQLtable   *table = NULL;
	ssize_t		nbytes;

	parse_options(argc, argv);
	/* open PostgreSQL connection */
	conn = pgsql_server_connect();
	/* run SQL command */
	res = pgsql_begin_query(conn, sql_command);
	if (!res)
		Elog("SQL command returned an empty result");
	table = pgsql_create_buffer(conn, res, batch_segment_sz);
	/* open the output file */
	if (output_filename)
	{
		table->fdesc = open(output_filename,
							O_RDWR | O_CREAT | O_TRUNC, 0644);
		if (table->fdesc < 0)
			Elog("failed to open '%s'", output_filename);
		table->filename = output_filename;
	}
	else
	{
		char	temp_filename[128];

		strcpy(temp_filename, "/tmp/XXXXXX.arrow");
		table->fdesc = mkostemps(temp_filename, 6,
								 O_RDWR | O_CREAT | O_TRUNC);
		if (table->fdesc < 0)
			Elog("failed to open '%s' : %m", temp_filename);
		table->filename = pstrdup(temp_filename);
		fprintf(stderr,
				"notice: -o, --output=FILENAME options was not specified,\n"
				"        so, a temporary file '%s' was built instead.\n",
				temp_filename);
	}
	//pgsql_dump_buffer(table);
	/* write header portion */
	nbytes = write(table->fdesc, "ARROW1\0\0", 8);
	if (nbytes != 8)
		Elog("failed on write(2): %m");
	nbytes = writeArrowSchema(table);
	writeArrowDictionaryBatches(table);
	do {
		pgsql_append_results(table, res);
		PQclear(res);
		res = pgsql_next_result(conn);
	} while (res != NULL);
	pgsql_end_query(conn);
	if (table->nitems > 0)
		pgsql_writeout_buffer(table);
	nbytes = writeArrowFooter(table);

	return 0;
}


