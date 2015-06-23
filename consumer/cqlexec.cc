#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <string>
#include <vector>
#include <list>
#include <utility>
#include <algorithm>

#include <jsoncpp/json/json.h>
#include <cassandra.h>

// -lcassandra

struct CaCtx {
  CassCluster        *cluster;
  CassSession        *session;
  CassFuture         *connect;
};

bool initCaCtx(const char *db, CaCtx *ctx);
bool execCql(CaCtx *ctx, const char *query, std::string *result);
void destroyCaCtx(CaCtx *ctx);

int main(int argc, char *argv[])
{
  if (argc != 3) {
    fprintf(stderr, "usage: %s ca cql", argv[0]);
    return EXIT_FAILURE;
  }

  const char *db  = argv[1];
  const char *cql = argv[2];

  CaCtx ctx;
  if (!initCaCtx(db, &ctx)) return EXIT_FAILURE;

  std::string json;
  bool r = execCql(&ctx, cql, &json);
  if (r) {
    printf("%s", json.c_str());
  }

  destroyCaCtx(&ctx);
  return EXIT_SUCCESS;
}

bool initCaCtx(const char *db, CaCtx *ctx)
{
  ctx->cluster = cass_cluster_new();
  ctx->session = cass_session_new();

  cass_cluster_set_contact_points(ctx->cluster, db);
  cass_cluster_set_max_connections_per_host(ctx->cluster, 1024);
  cass_cluster_set_max_concurrent_creation(ctx->cluster, 100);

  ctx->connect = cass_session_connect(ctx->session, ctx->cluster);
  if (cass_future_error_code(ctx->connect) != CASS_OK) {
    CassString msg = cass_future_error_message(ctx->connect);
    fprintf(stderr, "connect %s error %.*s\n", db, (int) msg.length, msg.data);
    return false;
  }

  return true;
}

void destroyCaCtx(CaCtx *ctx)
{
  if (ctx->connect) {
    CassFuture *closeFuture = cass_session_close(ctx->session);
    cass_future_wait(closeFuture);
    cass_future_free(closeFuture);
    cass_future_free(ctx->connect);
  }

  if (ctx->session) cass_session_free(ctx->session);
  if (ctx->cluster) cass_cluster_free(ctx->cluster);
}

Json::Value col2json(const CassValue *col)
{
  CassValueType type = cass_value_type(col);
  if (type == CASS_VALUE_TYPE_INT) {
    int i;
    cass_value_get_int32(col, &i);
    return Json::Value(i);
  } else if (type == CASS_VALUE_TYPE_BIGINT) {
    cass_int64_t i;
    cass_value_get_int64(col, &i);
    return Json::Value(i);
  } else if (type == CASS_VALUE_TYPE_FLOAT) {
    float f;
    cass_value_get_float(col, &f);
    return Json::Value(f);
  } else if (type == CASS_VALUE_TYPE_DOUBLE) {
    double d;
    cass_value_get_double(col, &d);
    return Json::Value(d);
  } else if (type == CASS_VALUE_TYPE_BOOLEAN) {
    cass_bool_t b;
    cass_value_get_bool(col, &b);
    return Json::Value(b == cass_true);
  } else if (type == CASS_VALUE_TYPE_TEXT || type == CASS_VALUE_TYPE_VARCHAR) {
    CassString s;
    cass_value_get_string(col, &s);
    return Json::Value(s.data, s.data + s.length);
  } else if (type == CASS_VALUE_TYPE_MAP) {
    CassIterator *ite = cass_iterator_from_map(col);
    Json::Value root(Json::objectValue);
    
    while (cass_iterator_next(ite)) {
      CassString key, value;
      cass_value_get_string(cass_iterator_get_map_key(ite), &key);
      cass_value_get_string(cass_iterator_get_map_value(ite), &value);
      root[std::string(key.data, key.length)] = Json::Value(value.data, value.data + value.length);
    }
    cass_iterator_free(ite);
    return root;
  } else if (type == CASS_VALUE_TYPE_LIST || type == CASS_VALUE_TYPE_SET) {
    CassIterator *ite = cass_iterator_from_collection(col);
    Json::Value root(Json::arrayValue);

    while (cass_iterator_next(ite)) {
      CassString value;
      cass_value_get_string(cass_iterator_get_value(ite), &value);
      root.append(Json::Value(value.data, value.data + value.length));
    }
    cass_iterator_free(ite);
    return root;
  } else {
    fprintf(stderr, "unsupport column type\n");
    return Json::Value();
  }
}          

bool execCql(CaCtx *ctx, const char *query, std::string *json)
{
  bool read = false;
  if (strncasecmp(query, "select", 6) == 0) {
    read = true;
  } else if (strncasecmp(query, "update", 6) != 0 ||
             strncasecmp(query, "insert", 6) != 0 ||
             strncasecmp(query, "delete", 6) != 0) {
    fprintf(stderr, "unsupport cql %s\n", query);
    return false;
  }
  
  CassStatement *statm = cass_statement_new(cass_string_init(query), 0);
  CassFuture *resultFuture = cass_session_execute(ctx->session, statm);
  cass_future_wait(resultFuture);

  if (cass_future_error_code(resultFuture) != CASS_OK) {
    CassString msg = cass_future_error_message(resultFuture);
    fprintf(stderr, "exec %s error %.*s\n", query, (int) msg.length, msg.data);
    return false;
  }

  if (read) {
    Json::Value root(Json::arrayValue);
    
    const CassResult *result = cass_future_get_result(resultFuture);
    CassIterator *ite = cass_iterator_from_result(result);

    while (cass_iterator_next(ite)) {
      Json::Value line(Json::arrayValue);      
      const CassRow *row = cass_iterator_get_row(ite);

      for (size_t i = 0; i < 100; ++i) {
        const CassValue *col = cass_row_get_column(row, i);
        if (!col) break;

        line.append(col2json(col));
      }

      root.append(line);
    }
    cass_iterator_free(ite);
    *json = Json::FastWriter().write(root);
  }

  return true;
}
