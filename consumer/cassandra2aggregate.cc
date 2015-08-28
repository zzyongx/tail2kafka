#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <string>
#include <vector>
#include <list>
#include <utility>
#include <algorithm>

#include <cassandra.h>
#include <jsoncpp/json/json.h>

// -lpthread -lcassandra -ljsoncpp

typedef std::vector<std::string>                           StringList;
typedef std::vector< std::pair<std::string, std::string> > StringPairList; 
typedef std::list< std::pair<CassFuture *, std::string> >  CassFutureList;

enum TimeUnit { DAY, HOUR, MIN, SECOND };
enum DataMode { ALL, SAMP, CACHEONLY, SECOND1 };
struct CaCtx {
  const char         *topic;
  const char         *id;
  time_t              now;
  DataMode            mode;
  
  CassCluster        *cluster;
  CassSession        *session;
  CassFuture         *connect;
  const CassPrepared *cachePrep;
  const CassPrepared *realPrep;
  const CassPrepared *updateCachePrep;
};

bool consTimeSeq(const char *start, const char *end, bool asc, DataMode mode,
                 StringList *ts, TimeUnit *unit);

bool initCaCtx(const char *db, const char *topic, const char *id, DataMode mode, CaCtx *ctx);
void destroyCaCtx(CaCtx *ctx);

void getRealData(CaCtx *ctx, const StringList &ts, StringPairList *results = 0);
void getCacheData(CaCtx *ctx, const StringList &ts, TimeUnit unit, StringPairList *results = 0);

int main(int argc, char *argv[])
{
  if (argc < 7) {
    fprintf(stderr, "usage: %s cassandra-cluster topic id start end\n"
                    "          [asc|desc] [all|samp|cacheonly|second1]\n",
            argv[0]);
    return EXIT_FAILURE;
  }

  bool asc = strcmp(argv[6], "asc") == 0;
  
  DataMode mode = ALL;
  if (argc == 8) {
    if (strcmp(argv[7], "all") == 0) {
      mode = ALL;
    } else if (strcmp(argv[7], "samp") == 0) {
      mode = SAMP;
    } else if (strcmp(argv[7], "cacheonly") == 0) {
      mode = CACHEONLY;
    } else if (strcmp(argv[7], "second1") == 0) {
      mode = SECOND1;
    }
  }

  const char *db    = argv[1];
  const char *topic = argv[2];
  const char *id    = argv[3];

  StringList ts;
  TimeUnit   unit;
  if (!consTimeSeq(argv[4], argv[5], asc, mode, &ts, &unit)) return EXIT_FAILURE;

  CaCtx ctx;

  if (!initCaCtx(db, topic, id, mode, &ctx)) return EXIT_FAILURE;

  if (unit == DAY || unit == HOUR || unit == MIN) {
    getCacheData(&ctx, ts, unit);
  } else {
    getRealData(&ctx, ts);
  }

  destroyCaCtx(&ctx);
  return EXIT_SUCCESS;
}

const CassPrepared *caPrepare(CaCtx *ctx, const char *query)
{
  CassFuture *prepareFuture = cass_session_prepare(ctx->session, cass_string_init(query));
  if (cass_future_error_code(prepareFuture) != CASS_OK) {
    CassString msg = cass_future_error_message(prepareFuture);
    fprintf(stderr, "prepare %s error %.*s\n", query, (int) msg.length, msg.data);
    return 0;
  }
  const CassPrepared *prepared = cass_future_get_prepared(prepareFuture);
  cass_future_free(prepareFuture);
  return prepared;
}

bool initCaCtx(const char *db, const char *topic, const char *id, DataMode mode, CaCtx *ctx)
{
  ctx->cluster = cass_cluster_new();
  ctx->session = cass_session_new();

  cass_cluster_set_contact_points(ctx->cluster, db);
  cass_cluster_set_max_connections_per_host(ctx->cluster, 1024);
  cass_cluster_set_max_concurrent_creation(ctx->cluster, 100);
  cass_cluster_set_request_timeout(ctx->cluster, 30000);  

  ctx->connect = cass_session_connect(ctx->session, ctx->cluster);
  if (cass_future_error_code(ctx->connect) != CASS_OK) {
    CassString msg = cass_future_error_message(ctx->connect);
    fprintf(stderr, "connect %s error %.*s\n", db, (int) msg.length, msg.data);
    return false;
  }

  ctx->topic = topic;
  ctx->id    = id;
  ctx->mode   = mode;
  time(&ctx->now);

  const char *query = "SELECT datas FROM de.cachedata WHERE topic = ? AND id = ? AND timestamp = ?";
  if (!(ctx->cachePrep = caPrepare(ctx, query))) return false;

  query = "SELECT datas FROM de.realdata WHERE topic = ? AND id = ? AND timestamp = ?";
  if (!(ctx->realPrep = caPrepare(ctx, query))) return false;

  query = "UPDATE de.cachedata USING TTL ? SET datas = ? WHERE topic = ? AND id = ? AND timestamp = ?";
  if (!(ctx->updateCachePrep = caPrepare(ctx, query))) return false;
 
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

  if (ctx->cachePrep) cass_prepared_free(ctx->cachePrep);
  if (ctx->realPrep) cass_prepared_free(ctx->realPrep);
  if (ctx->updateCachePrep) cass_prepared_free(ctx->updateCachePrep);

  if (ctx->session) cass_session_free(ctx->session);
  if (ctx->cluster) cass_cluster_free(ctx->cluster);
}

bool raw2json(const char *data, size_t size, Json::Value &root)
{
  std::string host;
  std::string key;
  int value = 0;
  enum {WaitHost, WaitKey, WaitValue} status = WaitHost;

  for (size_t i = 0; i < size; ++i) {
    if (data[i] == '=') {
      if (status == WaitKey) status = WaitValue;
      else return false;
    } else if (data[i] == ' ') {
      if (status == WaitHost) {
        status = WaitKey;
        if (!root.isMember(host)) {
          root[host] = Json::Value(Json::objectValue);
        }
      } else if (status == WaitValue) {
        status = WaitKey;
        int oval = root[host].isMember(key) ? root[host][key].asInt() : 0;
        root[host][key] = oval + value;
        
        key.clear();
        value = 0;
      } else return false;
    } else if (status == WaitHost) {
      host.append(1, data[i]);
    } else if (status == WaitKey) {
      key.append(1, data[i]);
    } else if (status == WaitValue) {
      if (data[i] >= '0' && data[i] <= '9') {
        value = value * 10 + data[i] - '0';
      } else if (data[i] != '.') {
        return false;
      }
    }
  }
  if (status == WaitValue) {
    int oval = root[host].isMember(key) ? root[host][key].asInt() : 0;
    root[host][key] = oval + value;
  } else {
    return false;
  }

  // fprintf(stderr, "%.*s\n%s", (int) size, data, Json::FastWriter().write(root).c_str());

  return true;
}

void pollRealWaitFuture(CassFutureList *cflist, StringPairList *results, bool wait)
{
  for (CassFutureList::iterator ite = cflist->begin(); ite != cflist->end();) {
    bool ready;
    if (wait || cflist->size() > 128) {
      cass_future_wait(ite->first);
      ready = true;
    } else {
      ready = cass_future_ready(ite->first);
    }
    
    if (ready) {
      std::string json;
      
      if (cass_future_error_code(ite->first) != CASS_OK) {
        CassString msg = cass_future_error_message(ite->first);
        fprintf(stderr, "Bad query %s %.*s\n", ite->second.c_str(),
                (int) msg.length, msg.data);
      } else {
        const CassResult *result = cass_future_get_result(ite->first);
        // fprintf(stderr, "timestamp %s\n", ite->second.c_str());
        if (cass_result_row_count(result) != 0) {
          CassIterator *ite = cass_iterator_from_collection(
            cass_row_get_column(cass_result_first_row(result), 0));
          Json::Value root(Json::objectValue);

          while (cass_iterator_next(ite)) {
            CassString value;
            cass_value_get_string(cass_iterator_get_value(ite), &value);

            if (!raw2json(value.data, value.length, root)) {
              fprintf(stderr, "invalid data %.*s\n", (int) value.length, value.data);
            }
          }

          json = Json::FastWriter().write(root);
          cass_iterator_free(ite);
        }
      }

      if (results) {
        results->push_back(std::make_pair(ite->second, json));
      } else {
        // json end with \n
        if (!json.empty()) {
          printf("id: %s\ndata: %s\n", ite->second.c_str(), json.c_str());
        }
      }

      cass_future_free(ite->first);
      ite = cflist->erase(ite);
    } else {
      ++ite;
    }
  }
}

void getRealData(CaCtx *ctx, const StringList &ts, StringPairList *results)
{
  CassFutureList cflist;
  for (StringList::const_iterator ite = ts.begin(); ite != ts.end(); ++ite) {
    pollRealWaitFuture(&cflist, results, false);
    
    CassStatement *statm = cass_prepared_bind(ctx->realPrep);
    // cass_statement_set_consistency(statm, CASS_CONSISTENCY_TWO);

    cass_statement_bind_string(statm, 0, cass_string_init(ctx->topic));
    cass_statement_bind_string(statm, 1, cass_string_init(ctx->id));
    cass_statement_bind_string(statm, 2, cass_string_init(ite->c_str()));

    CassFuture *resultFuture = cass_session_execute(ctx->session, statm);
    cflist.push_back(std::make_pair(resultFuture, *ite));
  }
  pollRealWaitFuture(&cflist, results, true);
}

void cacheSum(const StringPairList &results, std::string *json)
{
  Json::Value obj(Json::objectValue);
  
  for (StringPairList::const_iterator ite = results.begin();
       ite != results.end(); ++ite) {
    if (ite->second.empty()) continue;
    
    Json::Value root;
    Json::Reader reader;
    bool rc = reader.parse(ite->second, root);
    assert(rc);

    for (Json::ValueIterator it = root.begin(); it != root.end(); ++it) {
      assert(it.key().isString());
      std::string key = it.key().asString();

      if (!obj.isMember(key)) obj[key] = Json::Value(Json::objectValue);
      
      for (Json::ValueIterator jt = (*it).begin(); jt != (*it).end(); ++jt) {
        assert(jt.key().isString());
        std::string key2 = jt.key().asString();
        
        if (!obj[key].isMember(key2)) obj[key][key2] = 0;
        obj[key][key2] = obj[key][key2].asUInt() + (*jt).asUInt();
      }
    }
  }

  if (!obj.empty()) *json = Json::FastWriter().write(obj);
}

std::string timeToStr(time_t time, TimeUnit unit);

void cacheIt(CaCtx *ctx, const char *time, TimeUnit unit, const std::string &data)
{
  /* this MIN, this HOUR, this DAY will not be cached */
  std::string now = timeToStr(ctx->now, unit);
  if (now.compare(time) == 0) return;

  if (data.empty()) return;

  int ttl;
  if (unit == MIN) ttl = 86400 * 90;
  else ttl = 86400 * 365 * 10;
  
  CassStatement *statm = cass_prepared_bind(ctx->updateCachePrep);
  // cass_statement_set_consistency(statm, CASS_CONSISTENCY_TWO);

  cass_statement_bind_int32(statm, 0, ttl);
  cass_statement_bind_string(statm, 1, cass_string_init(data.c_str()));
  cass_statement_bind_string(statm, 2, cass_string_init(ctx->topic));
  cass_statement_bind_string(statm, 3, cass_string_init(ctx->id));
  cass_statement_bind_string(statm, 4, cass_string_init(time));

  CassFuture *resultFuture = cass_session_execute(ctx->session, statm);
  cass_future_wait(resultFuture);
  if (cass_future_error_code(resultFuture) != CASS_OK) {
    CassString msg = cass_future_error_message(resultFuture);
    fprintf(stderr, "Bad query %s %.*s\n", time, (int) msg.length, msg.data);
  }
  cass_future_free(resultFuture);
  cass_statement_free(statm);
}

void getDayCacheData(CaCtx *ctx, const char *timeDay, std::string *json)
{
  char tbuf[64];
  StringList ts;
  for (int i = 0; i < 24; ++i) {
    snprintf(tbuf, 64, "%sT%02d", timeDay, i);
    ts.push_back(tbuf);
  }

  StringPairList results;
  getCacheData(ctx, ts, HOUR, &results);
  cacheSum(results, json);
  cacheIt(ctx, timeDay, DAY, *json);
}

void getHourCacheData(CaCtx *ctx, const char *timeHour, std::string *json)
{
  char tbuf[64];
  StringList ts;
  for (int i = 0; i < 60; ++i) {
    snprintf(tbuf, 64, "%s:%02d", timeHour, i);
    ts.push_back(tbuf);
  }

  StringPairList results;
  getCacheData(ctx, ts, MIN, &results);
  cacheSum(results, json);
  cacheIt(ctx, timeHour, HOUR, *json);
}

void getMinCacheData(CaCtx *ctx, const char *timeMin, std::string *json)
{
  char tbuf[64];
  StringList ts;
  if (ctx->mode == SECOND1) {
    snprintf(tbuf, 64, "%s:%02d", timeMin, 0);
    ts.push_back(tbuf);
  } else {
    for (int i = 0; i < 60; ++i) {
      snprintf(tbuf, 64, "%s:%02d", timeMin, i);
      ts.push_back(tbuf);
    }
  }

  StringPairList results;
  getRealData(ctx, ts, &results);
  cacheSum(results, json);
  cacheIt(ctx, timeMin, MIN, *json);
}  

void pollCacheWaitFuture(CaCtx *ctx, CassFutureList *cflist, TimeUnit unit,
                         StringPairList *results, bool wait)
{
  for (CassFutureList::iterator ite = cflist->begin(); ite != cflist->end();) {
    bool ready;
    if (wait) {
      cass_future_wait(ite->first);
      ready = true;
    } else {
      ready = cass_future_ready(ite->first);
    }
    
    if (ready) {
      std::string json;
      
      if (cass_future_error_code(ite->first) != CASS_OK) {
        CassString msg = cass_future_error_message(ite->first);
        fprintf(stderr, "Bad query %s %.*s\n", ite->second.c_str(),
                (int) msg.length, msg.data);
      } else {
        const CassResult *result = cass_future_get_result(ite->first);
        if (cass_result_row_count(result) == 0) {
          if (ctx->mode != CACHEONLY) {
            if (unit == DAY) getDayCacheData(ctx, ite->second.c_str(), &json);
            else if (unit == HOUR) getHourCacheData(ctx, ite->second.c_str(), &json);
            else if (unit == MIN) getMinCacheData(ctx, ite->second.c_str(), &json);
            else assert(0);
          }
        } else {
          CassString value;
          cass_value_get_string(
            cass_row_get_column(cass_result_first_row(result), 0),
            &value);
          json.assign(value.data, value.length);
        }
      }

      if (results) {
        results->push_back(std::make_pair(ite->second, json));
      } else {
        if (json.empty()) json = "{}\n";
        if (json.at(json.size()-1) == '\n') {
          printf("id: %s\ndata: %s\n", ite->second.c_str(), json.c_str());
        } else {
          printf("id: %s\ndata: %s\n\n", ite->second.c_str(), json.c_str());
        }
      }

      cass_future_free(ite->first);
      ite = cflist->erase(ite);
    } else {
      ++ite;
    }
  }
}

void getCacheData(CaCtx *ctx, const StringList &ts, TimeUnit unit, StringPairList *results)
{
  CassFutureList cflist;
  for (StringList::const_iterator ite = ts.begin(); ite != ts.end(); ++ite) {
    pollCacheWaitFuture(ctx, &cflist, unit, results, false);
    
    CassStatement *statm = cass_prepared_bind(ctx->cachePrep);
    // cass_statement_set_consistency(statm, CASS_CONSISTENCY_TWO);

    cass_statement_bind_string(statm, 0, cass_string_init(ctx->topic));
    cass_statement_bind_string(statm, 1, cass_string_init(ctx->id));
    cass_statement_bind_string(statm, 2, cass_string_init(ite->c_str()));
    
    CassFuture *resultFuture = cass_session_execute(ctx->session, statm);
    cflist.push_back(std::make_pair(resultFuture, *ite));    
  }
  pollCacheWaitFuture(ctx, &cflist, unit, results, true);
}

/* 2015-04-17T09:57:36 */
bool strToTime(const char *str, time_t *time, TimeUnit *unit)
{
  struct tm tm;
  memset(&tm, 0x00, sizeof(tm));

  enum { WaitYear, WaitMonth, WaitDay, WaitHour, WaitMin, WaitSec } status = WaitYear;
  
  const char *p = str;
  while (*p) {
    if (*p == '-') {
      if (status == WaitYear) status = WaitMonth;
      else if (status == WaitMonth) status = WaitDay;
      else return false;
    } else if (*p == 'T') {
      if (status == WaitDay) status = WaitHour;
      else return false;
    } else if (*p == ':') {
      if (status == WaitHour) status = WaitMin;
      else if (status == WaitMin) status = WaitSec;
      else return false;
    } else if (*p >= '0' && *p <= '9') {
      int d = *p - '0';
      if (status == WaitYear) tm.tm_year = tm.tm_year * 10 + d;
      else if (status == WaitMonth) tm.tm_mon = tm.tm_mon * 10 + d;
      else if (status == WaitDay) tm.tm_mday = tm.tm_mday * 10 + d;
      else if (status == WaitHour) tm.tm_hour = tm.tm_hour * 10 + d;
      else if (status == WaitMin) tm.tm_min = tm.tm_min * 10 + d;
      else if (status == WaitSec) tm.tm_sec = tm.tm_sec * 10 + d;
    } else {
      return false;
    }
    p++;
  }

  tm.tm_year -= 1900; /* number of years since 1900 */
  tm.tm_mon -= 1;

  if (status == WaitDay) *unit = DAY;
  else if (status == WaitHour) *unit = HOUR;
  else if (status == WaitMin) *unit = MIN;
  else if (status == WaitSec) *unit = SECOND;
  else return false;
  
  if (tm.tm_mon < 0 || tm.tm_mon > 11) return false; /* 0-11 */
  if (tm.tm_mday < 1 || tm.tm_mday > 31) return false; /* 1-31 */
  if (tm.tm_hour > 23) return false; /* 0-23 */
  if (tm.tm_min > 59) return false;  /* 0-59 */
  if (tm.tm_sec > 59) return false;  /* 0-59 */
  if (tm.tm_year < 0) return false;

  *time = mktime(&tm);  
  return true;
}

std::string timeToStr(time_t time, TimeUnit unit)
{
  std::string s;
  struct tm tm;
  localtime_r(&time, &tm);
  tm.tm_year += 1900;
  tm.tm_mon += 1;

  s.append(1, tm.tm_year / 1000 % 10 + '0');
  s.append(1, tm.tm_year / 100 % 10 + '0');
  s.append(1, tm.tm_year / 10 % 10 + '0');
  s.append(1, tm.tm_year % 10 + '0');
  
  s.append(1, '-');
  s.append(1, tm.tm_mon / 10 + '0');
  s.append(1, tm.tm_mon % 10 + '0');

  s.append(1, '-');
  s.append(1, tm.tm_mday / 10 + '0');
  s.append(1, tm.tm_mday % 10 + '0');

  if (unit == DAY) return s;

  s.append(1, 'T');
  s.append(1, tm.tm_hour / 10 + '0');
  s.append(1, tm.tm_hour % 10 + '0');

  if (unit == HOUR) return s;

  s.append(1, ':');
  s.append(1, tm.tm_min / 10 + '0');
  s.append(1, tm.tm_min % 10 + '0');

  if (unit == MIN) return s;

  s.append(1, ':');
  s.append(1, tm.tm_sec / 10 + '0');
  s.append(1, tm.tm_sec % 10 + '0');

  return s;  
}


static const int MAX_SCAN = 3600;

bool consTimeSeq(const char *startStr, const char *endStr, bool asc,
                 DataMode mode, StringList *ts, TimeUnit *unit)
{
  time_t start, end;
  
  if (!strToTime(startStr, &start, unit)) {
    fprintf(stderr, "invalid start time %s\n", startStr);
    return false;
  }
  if (!strToTime(endStr, &end, unit)) {
    fprintf(stderr, "invalid end time %s\n", endStr);
    return false;
  }

  time_t now = time(0);
  if (end > now) end = now;
  if (start > end) start = end;

  int step = 0;
  if (*unit == DAY) {
    step = 86400;
  } else if (*unit == HOUR) {
    step = 3600;
  } else if (*unit == MIN) {
    step = 60;
  } else if (*unit == SECOND) {
    step = (mode == ALL) ? 1 : 6;
  }

  if ((start - end)/step > MAX_SCAN) {
    fprintf(stderr, "%s step %d to %s, too many items\n", startStr, step, endStr);
    return false;
  }

  srand(time(0));

  if (step == 6) {
    for (time_t i = start; i <= end; i += step) {
      int t = i + rand() % 6;
      if (t <= end) ts->push_back(timeToStr(t, SECOND));
    }
  } else {
    for (time_t i = start; i <= end; i += step) {
      ts->push_back(timeToStr(i, *unit));
    }
  }

  if (!asc) std::reverse(ts->begin(), ts->end());

  return true;
}
