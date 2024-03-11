// #include "fio_cli.h"
#include "main.h"

// typedef struct {
//     char* description;
//     char* type;
//     unsigned int value;
//     unsigned int user_id;
// } Transaction;

typedef struct {
    unsigned int limit;
    unsigned int opening_balance;
    unsigned int user_id;
} User;

void handleTransaction_1(http_s *h) {

  json_error_t error;

  json_t* data = json_loads(fiobj_obj2cstr(h->body).data, 0, &error);

  int saldo;

  int value = json_integer_value(json_object_get(data, "valor"));
  
  PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

  if (PQstatus(conn) != CONNECTION_OK) {
      // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
      PQfinish(conn);
      http_send_error(h, 500);
      return;
  }

  PGresult *res = PQexec(conn, "BEGIN");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
      PQclear(res);
      PQfinish(conn);
      http_send_error(h, 500);
      return;
  }
  PQclear(res);

  res = PQexec(conn, "SELECT saldo FROM transacoes_1 order by id_transacao desc limit 1");
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
      // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
      PQclear(res);
      http_send_error(h, 500);
      return;
  }

  if (PQntuples(res) == 0) {
    saldo = 0;
  } else {
    sscanf(PQgetvalue(res, 0, 0), "%d", &saldo);
  }

  PQclear(res);

  char query[300];

  int resultado;

  if (strncmp(json_string_value(json_object_get(data, "tipo")), "d", 1) == 0) {
    if (saldo - value + 100000 >= 0) {
      resultado = saldo - value;
      sprintf(query, "insert into transacoes_1 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_1'), %u, 'd', '%s', %d, 100000);", value, json_string_value(json_object_get(data, "descricao")), saldo - value);
    } else {
      // printf("Erro ao validar valores\n");
      PQfinish(conn);
      http_send_error(h, 422);
      return;
    }
  } else {
    resultado = saldo + value;
    sprintf(query, "insert into transacoes_1 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_1'), %u, 'c', '%s', %d, 100000);", value, json_string_value(json_object_get(data, "descricao")), saldo + value);
  }
  res = PQexec(conn, query);

  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      // printf("Error while executing the insert query: %s\n", PQerrorMessage(conn));
      PQclear(res);
      http_send_error(h, 500);
      return;
  }
  PQclear(res);

  res = PQexec(conn, "COMMIT");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      // printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
      PQclear(res);
      http_send_error(h, 500);
      return;
  }
  PQclear(res);
  PQfinish(conn);

  json_t *json_obj = json_pack("{s:i,s:i}", "limite", 100000, "saldo", resultado);
  char *json_str = json_dumps(json_obj, 0);

  http_send_body(h, json_str, strlen(json_str));
}
void handleTransaction_2(http_s *h) {

    json_error_t error;

    json_t* data = json_loads(fiobj_obj2cstr(h->body).data, 0, &error);

    int saldo;

    int value = json_integer_value(json_object_get(data, "valor"));
    
    PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo FROM transacoes_2 order by id_transacao desc limit 1");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      saldo = 0;
    } else {
      sscanf(PQgetvalue(res, 0, 0), "%d", &saldo);
    }

    PQclear(res);

    int resultado;

    char query[300];

    if (strncmp(json_string_value(json_object_get(data, "tipo")), "d", 1) == 0) {
      if (saldo - value + 80000 >= 0) {
        resultado = saldo - value;
        sprintf(query, "insert into transacoes_2 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_2'), %u, 'd', '%s', %d, 80000);", value, json_string_value(json_object_get(data, "descricao")), saldo - value);
      } else {
        printf("Erro ao validar valores\n");
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 422);
        return;
      }
    } else {
      resultado = saldo + value;
      sprintf(query, "insert into transacoes_2 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_2'), %u, 'c', '%s', %d, 80000);", value, json_string_value(json_object_get(data, "descricao")), saldo + value);
    }
    res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Error while executing the insert query: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);
    PQfinish(conn);

  json_t *json_obj = json_pack("{s:i,s:i}", "limite", 80000, "saldo", resultado);
  char *json_str = json_dumps(json_obj, 0);

  http_send_body(h, json_str, strlen(json_str));
}
void handleTransaction_3(http_s *h) {

    json_error_t error;

    json_t* data = json_loads(fiobj_obj2cstr(h->body).data, 0, &error);

    int saldo;

    int value = json_integer_value(json_object_get(data, "valor"));
    
    PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo FROM transacoes_3 order by id_transacao desc limit 1");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      saldo = 0;
    } else {
      sscanf(PQgetvalue(res, 0, 0), "%d", &saldo);
    }

    PQclear(res);

    int resultado;

    char query[300];

    if (strncmp(json_string_value(json_object_get(data, "tipo")), "d", 1) == 0) {
      if (saldo - value + 1000000 >= 0) {
        resultado = saldo - value;
        sprintf(query, "insert into transacoes_3 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_3'), %u, 'd', '%s', %d, 1000000);", value, json_string_value(json_object_get(data, "descricao")), saldo - value);
      } else {
        printf("Erro ao validar valores\n");
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 422);
        return;
      }
    } else {
      resultado = saldo + value;
      sprintf(query, "insert into transacoes_3 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_3'), %u, 'c', '%s', %d, 1000000 );", value, json_string_value(json_object_get(data, "descricao")), saldo + value);
    }
    res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Error while executing the insert query: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);
    PQfinish(conn);

    json_t *json_obj = json_pack("{s:i,s:i}", "limite", 1000000, "saldo", resultado);
    char *json_str = json_dumps(json_obj, 0);

    http_send_body(h, json_str, strlen(json_str));
}
void handleTransaction_4(http_s *h) {

    json_error_t error;

    json_t* data = json_loads(fiobj_obj2cstr(h->body).data, 0, &error);

    int saldo;

    int value = json_integer_value(json_object_get(data, "valor"));
    
    PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo FROM transacoes_4 order by id_transacao desc limit 1");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      saldo = 0;
    } else {
      sscanf(PQgetvalue(res, 0, 0), "%d", &saldo);
    }

    PQclear(res);

    int resultado;

    char query[300];

    if (strncmp(json_string_value(json_object_get(data, "tipo")), "d", 1) == 0) {
      if (saldo - value + 10000000 >= 0) {
        resultado = saldo - value;
        sprintf(query, "insert into transacoes_4 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_4'), %u, 'd', '%s', %d, 10000000);", value, json_string_value(json_object_get(data, "descricao")), saldo - value);
      } else {
        printf("Erro ao validar valores\n");
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 422);
        http_send_error(h, 500);
        return;
      }
    } else {
      resultado = saldo + value;
      sprintf(query, "insert into transacoes_4 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_4'), %u, 'c', '%s', %d, 10000000);", value, json_string_value(json_object_get(data, "descricao")), saldo + value);
    }
    res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Error while executing the insert query: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);
    PQfinish(conn);

    json_t *json_obj = json_pack("{s:i,s:i}", "limite", 10000000, "saldo", resultado);
    char *json_str = json_dumps(json_obj, 0);

    http_send_body(h, json_str, strlen(json_str));
}
void handleTransaction_5(http_s *h) {

    json_error_t error;

    json_t* data = json_loads(fiobj_obj2cstr(h->body).data, 0, &error);

    int saldo;

    int value = json_integer_value(json_object_get(data, "valor"));
    
    PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo FROM transacoes_5 order by id_transacao desc limit 1");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      saldo = 0;
    } else {
      sscanf(PQgetvalue(res, 0, 0), "%d", &saldo);
    }

    PQclear(res);

    char query[300];

    int resultado;

    if (strncmp(json_string_value(json_object_get(data, "tipo")), "d", 1) == 0) {
      if (saldo - value + 500000 >= 0) {
        resultado = saldo - value;
        sprintf(query, "insert into transacoes_5 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_5'), %u, 'd', '%s', %d, 500000);", value, json_string_value(json_object_get(data, "descricao")), saldo - value);
      } else {
        printf("Erro ao validar valores\n");
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 422);
        return;
      }
    } else {
      resultado = saldo + value;
      sprintf(query, "insert into transacoes_5 (id_transacao, valor, tipo, descricao, saldo, limite) values (nextval('sq_transacoes_5'), %u, 'c', '%s', %d, 500000);", value, json_string_value(json_object_get(data, "descricao")), saldo + value);
    }
    res = PQexec(conn, query);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Error while executing the insert query: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "COMMIT");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);
    PQfinish(conn);

    json_t *json_obj = json_pack("{s:i,s:i}", "limite", 500000, "saldo", resultado);
    char *json_str = json_dumps(json_obj, 0);

    http_send_body(h, json_str, strlen(json_str));
}


void handleExtract_1(http_s *h) {

  time_t t = time(NULL);
  struct tm *tm = localtime(&t);

  char timestamp[100];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm);

  PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo, tipo, descricao, valor, data_transacao FROM transacoes_1 order by id_transacao desc limit 10");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(0));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(100000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));
      
      http_send_body(h, json_str, strlen(json_str));

      free(json_str);
      json_decref(raiz);
    } else {

      int total;

      int num_rows = PQntuples(res);
      sscanf(PQgetvalue(res, 0, 0), "%d", &total);

      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(total));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(100000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      for (int i = 0; i < num_rows; i++) {

        int valor;
        sscanf(PQgetvalue(res, i, 3), "%d", &valor);

        json_t *transacao = json_object();
        json_object_set_new(transacao, "valor", json_integer(valor));
        json_object_set_new(transacao, "tipo", json_string(PQgetvalue(res, i, 1)));
        json_object_set_new(transacao, "descricao", json_string(PQgetvalue(res, i, 2)));
        json_object_set_new(transacao, "realizada_em", json_string(PQgetvalue(res, i, 4)));
        json_array_append_new(ultimas_transacoes, transacao);

      }

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));

      http_send_body(h, json_str, strlen(json_str));
    }

  PQclear(res);

  res = PQexec(conn, "COMMIT");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      // printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
      PQclear(res);
      http_send_error(h, 500);
      return;
  }
  PQclear(res);
  PQfinish(conn);     
}
void handleExtract_2(http_s *h) {

  time_t t = time(NULL);
  struct tm *tm = localtime(&t);

  char timestamp[100];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm);

  PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo, tipo, descricao, valor, data_transacao FROM transacoes_2 order by id_transacao desc limit 10");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(0));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(100000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));
      
      http_send_body(h, json_str, strlen(json_str));

      free(json_str);
      json_decref(raiz);
    } else {

      int total;

      int num_rows = PQntuples(res);
      sscanf(PQgetvalue(res, 0, 0), "%d", &total);

      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(total));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(80000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      for (int i = 0; i < num_rows; i++) {

        int valor;
        sscanf(PQgetvalue(res, i, 3), "%d", &valor);

        json_t *transacao = json_object();
        json_object_set_new(transacao, "valor", json_integer(valor));
        json_object_set_new(transacao, "tipo", json_string(PQgetvalue(res, i, 1)));
        json_object_set_new(transacao, "descricao", json_string(PQgetvalue(res, i, 2)));
        json_object_set_new(transacao, "realizada_em", json_string(PQgetvalue(res, i, 4)));
        json_array_append_new(ultimas_transacoes, transacao);

      }

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));

      http_send_body(h, json_str, strlen(json_str));
    }
  PQclear(res);

  res = PQexec(conn, "COMMIT");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      // printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
      PQclear(res);
      http_send_error(h, 500);
      return;
  }
  PQclear(res);
  PQfinish(conn);  
}
void handleExtract_3(http_s *h) {

  time_t t = time(NULL);
  struct tm *tm = localtime(&t);

  char timestamp[100];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm);

  PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo, tipo, descricao, valor, data_transacao FROM transacoes_3 order by id_transacao desc limit 10");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(0));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(100000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));
      
      http_send_body(h, json_str, strlen(json_str));

      free(json_str);
      json_decref(raiz);
    } else {

      int total;

      int num_rows = PQntuples(res);
      sscanf(PQgetvalue(res, 0, 0), "%d", &total);

      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(total));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(1000000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      for (int i = 0; i < num_rows; i++) {

        int valor;
        sscanf(PQgetvalue(res, i, 3), "%d", &valor);

        json_t *transacao = json_object();
        json_object_set_new(transacao, "valor", json_integer(valor));
        json_object_set_new(transacao, "tipo", json_string(PQgetvalue(res, i, 1)));
        json_object_set_new(transacao, "descricao", json_string(PQgetvalue(res, i, 2)));
        json_object_set_new(transacao, "realizada_em", json_string(PQgetvalue(res, i, 4)));
        json_array_append_new(ultimas_transacoes, transacao);

      }

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));

      http_send_body(h, json_str, strlen(json_str));
    }
  PQclear(res);

  res = PQexec(conn, "COMMIT");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      // printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
      PQclear(res);
      http_send_error(h, 500);
      return;
  }
  PQclear(res);
  PQfinish(conn); 
}
void handleExtract_4(http_s *h) {

  time_t t = time(NULL);
  struct tm *tm = localtime(&t);

  char timestamp[100];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm);

  PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo, tipo, descricao, valor, data_transacao FROM transacoes_4 order by id_transacao desc limit 10");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(0));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(100000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));
      
      http_send_body(h, json_str, strlen(json_str));

      free(json_str);
      json_decref(raiz);
    } else {

      int total;

      int num_rows = PQntuples(res);
      sscanf(PQgetvalue(res, 0, 0), "%d", &total);

      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(total));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(10000000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      for (int i = 0; i < num_rows; i++) {

        int valor;
        sscanf(PQgetvalue(res, i, 3), "%d", &valor);

        json_t *transacao = json_object();
        json_object_set_new(transacao, "valor", json_integer(valor));
        json_object_set_new(transacao, "tipo", json_string(PQgetvalue(res, i, 1)));
        json_object_set_new(transacao, "descricao", json_string(PQgetvalue(res, i, 2)));
        json_object_set_new(transacao, "realizada_em", json_string(PQgetvalue(res, i, 4)));
        json_array_append_new(ultimas_transacoes, transacao);

      }

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));

      http_send_body(h, json_str, strlen(json_str));
    }
  PQclear(res);

  res = PQexec(conn, "COMMIT");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      // printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
      PQclear(res);
      http_send_error(h, 500);
      return;
  }
  PQclear(res);
  PQfinish(conn); 
}
void handleExtract_5(http_s *h) {

  time_t t = time(NULL);
  struct tm *tm = localtime(&t);

  char timestamp[100];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm);

  PGconn *conn = PQconnectdb("dbname=postgres user=postgres password=1234 host=localhost port=14000");

    if (PQstatus(conn) != CONNECTION_OK) {
        // printf("Error while connecting to the database server: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }

    PGresult *res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // printf("Erro ao iniciar transação: %s\n", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        http_send_error(h, 500);
        return;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT saldo, tipo, descricao, valor, data_transacao FROM transacoes_5 order by id_transacao desc limit 10");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // printf("Erro ao executar SELECT: %s\n", PQerrorMessage(conn));
        PQclear(res);
        http_send_error(h, 500);
        return;
    }

    if (PQntuples(res) == 0) {
      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(0));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(100000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));
      
      http_send_body(h, json_str, strlen(json_str));

      free(json_str);
      json_decref(raiz);
    } else {

      int total;

      int num_rows = PQntuples(res);
      sscanf(PQgetvalue(res, 0, 0), "%d", &total);

      json_t *raiz = json_object();

      json_t *saldo = json_object();
      json_object_set_new(saldo, "total", json_integer(total));
      json_object_set_new(saldo, "data_extrato", json_string(timestamp));
      json_object_set_new(saldo, "limite", json_integer(500000));

      json_object_set_new(raiz, "saldo", saldo);

      json_t *ultimas_transacoes = json_array();

      for (int i = 0; i < num_rows; i++) {

        int valor;
        sscanf(PQgetvalue(res, i, 3), "%d", &valor);

        json_t *transacao = json_object();
        json_object_set_new(transacao, "valor", json_integer(valor));
        json_object_set_new(transacao, "tipo", json_string(PQgetvalue(res, i, 1)));
        json_object_set_new(transacao, "descricao", json_string(PQgetvalue(res, i, 2)));
        json_object_set_new(transacao, "realizada_em", json_string(""));
        json_array_append_new(ultimas_transacoes, transacao);

      }
      json_object_set_new(raiz, "ultimas_transacoes", ultimas_transacoes);

      char *json_str = json_dumps(raiz, JSON_INDENT(2));

      http_send_body(h, json_str, strlen(json_str));
    }

  res = PQexec(conn, "COMMIT");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      printf("Erro ao confirmar transação: %s\n", PQerrorMessage(conn));
      PQclear(res);
      http_send_error(h, 500);
      return;
  }
  PQclear(res);
  PQfinish(conn); 
}

static void on_http_request(http_s *h) {


  /* set a response and send it (finnish vs. destroy). */
  // const char *tipo = fiobj_type_name(h->method);
  http_parse_query(h);
  http_parse_body(h);

  /* h->path = rota*/
  /* h->method = metodo */
  /* h->query = propriedade e valor & prop... */
  char* url_route = fiobj_obj2cstr(h->path).data;
  if (strstr(url_route, "transacoes") == NULL && strstr(url_route, "extrato") == NULL) {
    http_send_error(h, 404);
    return;
  }

  char user_id[2];

  strncpy(user_id, url_route + 10, 1);

  if (strncmp(user_id, "1", 1) == 0) {
    if (strncmp(fiobj_obj2cstr(h->method).data, "POST", 4) == 0) {
      handleTransaction_1(h);
      return;
    } else if (strncmp(fiobj_obj2cstr(h->method).data, "GET", 3) == 0) {
      handleExtract_1(h);
      return;
    }
  }
  if (strncmp(user_id, "2", 1) == 0) {
    if (strncmp(fiobj_obj2cstr(h->method).data, "POST", 4) == 0) {
      handleTransaction_2(h);
      return;
    } else if (strncmp(fiobj_obj2cstr(h->method).data, "GET", 3) == 0) {
      handleExtract_2(h);
      return;
    }
  }
  if (strncmp(user_id, "3", 1) == 0) {
    if (strncmp(fiobj_obj2cstr(h->method).data, "POST", 4) == 0) {
      handleTransaction_3(h);
      return;
    } else if (strncmp(fiobj_obj2cstr(h->method).data, "GET", 3) == 0) {
      handleExtract_3(h);
      return;
    }
  }
  if (strncmp(user_id, "4", 1) == 0) {
    if (strncmp(fiobj_obj2cstr(h->method).data, "POST", 4) == 0) {
      handleTransaction_4(h);
      return;
    } else if (strncmp(fiobj_obj2cstr(h->method).data, "GET", 3) == 0) {
      handleExtract_4(h);
      return;
    }
  }

  if (strncmp(user_id, "5", 1) == 0) {
    if (strncmp(fiobj_obj2cstr(h->method).data, "POST", 4) == 0) {
      handleTransaction_5(h);
      return;
    } else if (strncmp(fiobj_obj2cstr(h->method).data, "GET", 3) == 0) {
      handleExtract_5(h);
      return;
    }
  }

  http_send_error(h, 404);
}

/* starts a listeninng socket for HTTP connections. */
void initialize_http_service() {
  /* listen for inncoming connections */
  if (http_listen(
                  fio_cli_get("-p"), 
                  fio_cli_get("-b"),
                  .on_request = on_http_request,
                  .max_body_size = fio_cli_get_i("-maxbd") * 1024 * 1024,
                  .ws_max_msg_size = fio_cli_get_i("-max-msg") * 1024,
                  .public_folder = fio_cli_get("-public"),
                  .log = fio_cli_get_bool("-log"),
                  .timeout = fio_cli_get_i("-keep-alive"),
                  .ws_timeout = fio_cli_get_i("-ping")) == -1) {
    /* listen failed ?*/
    perror("ERROR: facil couldn't initialize HTTP service (already running?)");
    exit(1);
  }
}
