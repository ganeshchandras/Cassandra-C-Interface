#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "config.h"
#include <cassandra.h>

char *readline(char *prompt);
void nextarg(char *ln, int *pos, char *sep, char *arg);

static int tty = 0;

static void
cli_about()
{
	printf("You executed a command!\n");
}

static void
cli_help()
{
	return;
}

void insert(CassSession* session,char* key,char* val,char* table){
	char q_start[BUFSIZE] = "";
	int i=0;
	int j=0;
	strcpy(q_start,"insert into ");
	for (j = 0; q_start[j] != '\0'; j++);
	for (i = 0; table[i] != '\0'; i++,j++){
		q_start[j] = table[i];
	}
	strcat(q_start,"(");
	for (j = 0; q_start[j] != '\0'; j++);
	for (i = 0; key[i] != '\0'; i++,j++){
		q_start[j] = key[i];
	}
	strcat(q_start,") values('");
	for (j = 0; q_start[j] != '\0'; j++);
	for (i = 0; val[i] != '\0'; i++,j++){
		q_start[j] = val[i];
	}
	strcat(q_start,"')");
	CassStatement* statement = cass_statement_new(q_start, 0);
	CassFuture* query_future = cass_session_execute(session, statement);
	if (cass_future_error_code(query_future) == CASS_OK) {
		printf("Value has been inserted\n");
	}
	else{
		printf("Unable to run query\n");
	}	
}

void projection(CassSession* session,char* key,char* table){
	
	char q_start[BUFSIZE] = "";
	int i=0;
	int j=0;
	printf("%s",q_start);
	strcpy(q_start,"select ");
	for (j = 0; q_start[j] != '\0'; j++);
	for (i = 0; key[i] != '\0'; i++,j++){
		q_start[j] = key[i];
	}
	strcat(q_start," from ");
	for (j = 0; q_start[j] != '\0'; j++);
	for (i = 0; table[i] != '\0'; i++,j++){
		q_start[j] = table[i];
	}

	CassStatement* statement = cass_statement_new(q_start, 0);
	CassFuture* query_future = cass_session_execute(session, statement);
	if (cass_future_error_code(query_future) == CASS_OK) {
		const CassResult* result = cass_future_get_result(query_future);
		cass_future_free(query_future);
		CassIterator* rows = cass_iterator_from_result(result);
		while(cass_iterator_next(rows)) {
      			char id_str[CASS_UUID_STRING_LENGTH];
      			const CassRow* row = cass_iterator_get_row(rows);
      			const CassValue* value = cass_row_get_column_by_name(row, key);
     			if (cass_value_type(value) == CASS_VALUE_TYPE_VARCHAR) {
            			const char* text;
            			size_t text_length;
            			cass_value_get_string(value, &text, &text_length);
            			printf("\"%.*s\" \n", (int)text_length, text);
          		} else if (cass_value_type(value) == CASS_VALUE_TYPE_INT) {
            			cass_int32_t i;
            			cass_value_get_int32(value, &i);
            			printf("%d \n", (int)i);
			}      			
		}
			
		cass_result_free(result);
	}
	else{
      		const char* message;
      		size_t message_length;
      		cass_future_error_message(query_future, &message, &message_length);
      		fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
		printf("Unable to run query");
	}
	cass_statement_free(statement);	

	
}

void show_current_keyspaces(CassSession* session,char* keyspace){

	char q_start[BUFSIZE] = "";
	int i,j;
	strcpy(q_start,"select * from system_schema.tables Where keyspace_name='");
	for (j = 0; q_start[j] != '\0'; j++);
	for (i = 0; keyspace[i] != '\0'; i++,j++){
		q_start[j] = keyspace[i];
	}
	strcat(q_start,"'");
	CassStatement* statement = cass_statement_new(q_start, 0);
	CassFuture* query_future = cass_session_execute(session, statement);

	if (cass_future_error_code(query_future) == CASS_OK) {
		const CassResult* result = cass_future_get_result(query_future);
		cass_future_free(query_future);
		CassIterator* rows = cass_iterator_from_result(result);

		while(cass_iterator_next(rows)) {
        		const CassRow* row = cass_iterator_get_row(rows);
        		const CassValue* value = cass_row_get_column_by_name(row, "table_name");
			const char* table;
        		size_t table_length;
        		cass_value_get_string(value, &table, &table_length);
        		printf("table_name: '%.*s'\n",
               		(int)table_length, table);
      		}
		cass_result_free(result);
	}
	else{
      		const char* message;
      		size_t message_length;
      		cass_future_error_message(query_future, &message, &message_length);
      		fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
		printf("Unable to run query");
	}
	cass_statement_free(statement);	
}

void show_keyspaces(CassSession* session){
	
	const char* query = "select keyspace_name from system_schema.keyspaces";
	CassStatement* statement = cass_statement_new(query, 0);
	CassFuture* query_future = cass_session_execute(session, statement);

	if (cass_future_error_code(query_future) == CASS_OK) {
		const CassResult* result = cass_future_get_result(query_future);
		cass_future_free(query_future);
		CassIterator* rows = cass_iterator_from_result(result);

		while(cass_iterator_next(rows)) {
        		const CassRow* row = cass_iterator_get_row(rows);
        		const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");
			const char* keyspace;
        		size_t keyspace_length;
        		cass_value_get_string(value, &keyspace, &keyspace_length);
        		printf("keyspace_name: '%.*s'\n",(int)keyspace_length, keyspace);
      		}
		cass_result_free(result);
	}
	else{
      		const char* message;
      		size_t message_length;
      		cass_future_error_message(query_future, &message, &message_length);
      		fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
		printf("Unable to run query");
	}
	cass_statement_free(statement);
}

void cli(CassSession* session)
{
	char *cmdline = NULL;
	char cmd[BUFSIZE], prompt[BUFSIZE];
	char keyTableName[BUFSIZE] = {'\0'};
	char keyspace_name[BUFSIZE];
	int pos;
	int pos2;

	tty = isatty(STDIN_FILENO);
	if (tty)
		cli_about();

	/* Main command line loop */
	for (;;) {
		if (cmdline != NULL) {
			free(cmdline);
			cmdline = NULL;
		}
		memset(prompt, 0, BUFSIZE);
		sprintf(prompt, "cassandra> ");

		if (tty){
			cmdline = readline(prompt);
		}
		else
			cmdline = readline("");

		if (cmdline == NULL)
			continue;

		if (strlen(cmdline) == 0)
			continue;

		if (!tty)
			printf("%s\n", cmdline);

		if (strcmp(cmdline, "?") == 0) {
			cli_help();
			continue;
		}
		if (strcmp(cmdline, "quit") == 0 ||
		    strcmp(cmdline, "q") == 0)
			break;

		if (strcmp(cmdline,"show") == 0){
			show_keyspaces(session);
		}

		if (strcmp(cmdline,"list") == 0){
			if(keyTableName[0] == '\0'){
				printf("Keyspace and table not set\n");
				continue;
			}
			show_current_keyspaces(session,keyspace_name);
		}

		memset(cmd, 0, BUFSIZE);
		pos = 0;
		nextarg(cmdline, &pos, " ", cmd);
		if (strcmp(cmd,"use") == 0){
			nextarg(cmdline, &pos, " ", cmd);
			strcpy(keyTableName,cmd);
			pos2 = 0;
			nextarg(keyTableName,&pos2,".",keyspace_name);
		}
		if (strcmp(cmd,"get") == 0){
			if(keyTableName == '\0'){
				printf("Keyspace and table not set");
				continue;
			}
			nextarg(cmdline, &pos, " ", cmd);
			projection(session,cmd,keyTableName);
		}
		if (strcmp(cmd,"insert") == 0){
			if(keyTableName == '\0'){
				printf("Keyspace and table not set");
				continue;
			}
			nextarg(cmdline, &pos, " ", cmd);
			char key[BUFSIZE];
			strcpy(key,cmd);
			nextarg(cmdline, &pos, " ", cmd);
			insert(session,key,cmd,keyTableName);
		}
		if (strcmp(cmd, "about") == 0 || strcmp(cmd, "a") == 0) {
			cli_about();
			continue;

		}
	}
}

int main(int argc, char**argv)
{
	CassCluster* cluster = cass_cluster_new();
  	CassSession* session = cass_session_new();
  	cass_cluster_set_contact_points(cluster, "127.0.0.1");
	CassFuture* connect_future  = cass_session_connect_keyspace(session, cluster, "test");
  	CassError rc = cass_future_error_code(connect_future);
	cli(session);	
	cass_future_free(connect_future);
  	cass_session_free(session);
  	cass_cluster_free(cluster);
	exit(0);
}
