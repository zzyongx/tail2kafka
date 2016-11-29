#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <climits>
#include <cassert>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <errno.h>
#include <time.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <poll.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <sys/socket.h>
#include <netdb.h>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}
#include <librdkafka/rdkafka.h>

#define safe_close(fd) do { \
  close((fd));              \
  (fd) = -1;                \
} while (0)

static const char   NL             = '\n';
static const int    UNSET_INT      = INT_MAX;
static const size_t MAX_LINE_LEN   = 204800;
static const size_t MAX_FILES      = 64;
static const size_t ONE_EVENT_SIZE = sizeof(struct inotify_event) + NAME_MAX;
static const size_t MAX_ERR_LEN    = 512;

struct LuaCtx;

typedef std::map<std::string, int>         SIHash;
typedef std::map<std::string, SIHash>      SSIHash;
typedef std::vector<std::string>           StringList;
typedef std::vector<std::string *>         StringPtrList;
typedef std::vector<int>                   IntList;
typedef std::vector<LuaCtx *>              LuaCtxPtrList;
typedef std::map<int, LuaCtx *>            WatchCtxHash;
typedef std::map<std::string, std::string> SSHash;
typedef std::vector<rd_kafka_topic_t *>    TopicList;

struct CnfCtx {
  lua_State    *L;
  
  std::string host;
  std::string pidfile;
  std::string brokers;
  int         partition;
  int         pollLimit;
  SSHash      kafkaGlobal;
  SSHash      kafkaTopic;

  rd_kafka_t            *rk;
  TopicList              rkts;

  size_t                 count;
  LuaCtxPtrList          luaCtxs;
  WatchCtxHash           wch;
  int                    wfd;
  int                    accept;
  int                    server;
  uint64_t               sn;
  
  CnfCtx() {
    rk     = 0;
    accept = -1;
    server = -1;
    wfd    = -1;
    sn     = 0;
    count  = 0;
  }
};

typedef bool (*transform_pt)(
  LuaCtx *ctx, const char *line, size_t nline,
  std::string *result);
bool transform(LuaCtx *ctx, const char *line, size_t nline, std::string *result);

typedef bool(*grep_pt)(
  LuaCtx *ctx, const StringList &fields,
  std::string *result);
bool grep(LuaCtx *ctx, const StringList &fields, std::string *result);

typedef bool (*aggregate_pt)(
  LuaCtx *ctx, const StringList &fields,
  StringPtrList *results);
bool aggregate(LuaCtx *ctx, const StringList &fields, StringPtrList *results);

typedef bool (*filter_pt)(
  LuaCtx *ctx, const StringList &fields,
  std::string *result);
bool filter(LuaCtx *ctx, const StringList &fields, std::string *result);

#define FILE_MOVED     0x01
#define FILE_TRUNCATED 0x02

struct LuaCtx {
  int           idx;
  
  CnfCtx       *main;
  lua_State    *L;
  
  int           fd;
  ino_t         inode;
  bool          autocreat;
  std::string   file;
  off_t         size;
  std::string   topic;

  int           lines;
  time_t        start;

  bool          autosplit;
  bool          withhost;
  bool          withtime;
  int           timeidx;
  
  uint32_t      addr;
  bool          autoparti;  
  int           partition;
  bool          rawcopy;

  transform_pt  transform;
  std::string   transformFun;
  
  grep_pt       grep;
  std::string   grepFun;

  std::string   pkey;
  std::string   lasttime;
  SSIHash       cache;
  aggregate_pt  aggregate;
  std::string   aggregateFun;

  IntList       filters;
  filter_pt     filter;

  char         *buffer;
  size_t        npos;

  uint64_t      sn;
  int           stat;

  LuaCtx       *next;

  LuaCtx();
};

LuaCtx::LuaCtx()
{
  L  = 0;
  fd = -1;
  autocreat = false;

  autosplit = false;
  withhost  = true;
  withtime  = true;
  autoparti = false;
  partition = RD_KAFKA_PARTITION_UA;
  timeidx   = UNSET_INT;
  rawcopy   = true;
  
  buffer = new char[MAX_LINE_LEN];
  npos = 0;

  transform = 0;
  grep      = 0;
  aggregate = 0;
  filter    = 0;
  next      = 0;

  sn = 0;
}

struct OneTaskReq {
  int            idx;
  std::string   *data;
  StringPtrList *datas;
};

CnfCtx *loadCnf(const char *dir, char *errbuf);
void unloadCnfCtx(CnfCtx *ctx);
bool initSignal(char *errbuf);
bool initSingleton(CnfCtx *ctx, char *errbuf);
pid_t spawn(CnfCtx *ctx, CnfCtx *octx, char *errbuf);
int runForeGround(CnfCtx *ctx, char *errbuf);

enum Want {WAIT, START1, START2, RELOAD, STOP} want;

#ifndef UNITTEST
int main(int argc, char *argv[])
{
  if (argc != 2) {
    fprintf(stderr, "%s confdir\n", argv[0]);
    return EXIT_FAILURE;
  }

  const char *dir = argv[1];
  pid_t pid = -1;
  char errbuf[MAX_ERR_LEN];

  CnfCtx *ctx = loadCnf(dir, errbuf);
  if (!ctx) {
    fprintf(stderr, "load cnf error %s\n", errbuf);
    return EXIT_FAILURE;
  }

  bool daemonOff = getenv("DAEMON_OFF");

  if (!daemonOff) {
    if (getenv("TAIL2KAFKA_NOSTDIO")) {
      daemon(1, 0);
    } else {
      daemon(1, 1);
    }
  }
  want = START1;

  if (!initSingleton(ctx, errbuf)) {
    fprintf(stderr, "init singleton %s\n", errbuf);
    return EXIT_FAILURE;
  }

  if (!initSignal(errbuf)) {
    fprintf(stderr, "init signal %s\n", errbuf);
    return EXIT_FAILURE;
  }

  if (daemonOff) return runForeGround(ctx, errbuf);

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGCHLD);
  sigaddset(&set, SIGTERM);
  sigaddset(&set, SIGHUP);
  if (sigprocmask(SIG_BLOCK, &set, NULL) == -1) {
    fprintf(stderr, "sigprocmask failed, %s\n", strerror(errno));
    return EXIT_FAILURE;
  }
  
  sigemptyset(&set);

  int rc = EXIT_SUCCESS;

  while (want != STOP) {
    if (want == START2) {
      pid_t opid;
      if ((opid = waitpid(-1, NULL, WNOHANG)) > 0) {
        if (opid != pid) want = WAIT;
      } else {
        want = WAIT;
      }
    }
    
    if (want == START1 || want == START2) {
      pid = spawn(ctx, 0, errbuf);
      if (pid == -1) {
        fprintf(stderr, "spawn failed, exit\n");
        rc = EXIT_FAILURE;
        break;
      }
    } else if (want == RELOAD) {
      CnfCtx *nctx = loadCnf(dir, errbuf);
      if (nctx) {
        pid_t npid = spawn(nctx, ctx, errbuf);
        if (npid != -1) {
          fprintf(stderr, "reload cnf\n");
          kill(pid, SIGTERM);
          ctx = nctx;
          pid = npid;
        } else {
          unloadCnfCtx(nctx);
        }
      } else {
        fprintf(stderr, "load cnf error %s\n", errbuf);
      }
      want = WAIT;
    }
    sigsuspend(&set);
  }

  if (pid != -1) kill(pid, SIGTERM);
  
  unloadCnfCtx(ctx);
  return rc;
}
#endif

void unloadLuaCtx(LuaCtx *ctx)
{
  if (ctx->L) lua_close(ctx->L);
  if (ctx->fd != -1) close(ctx->fd);
  if (ctx->buffer) delete[] ctx->buffer;
  delete ctx;
}

static bool initLuaCtxFilter(LuaCtx *ctx, const char *file, char *errbuf)
{
  bool rc = false;
  int size;
  lua_getglobal(ctx->L, "filter");
  
  if (!lua_isnil(ctx->L, 1)) {
    if (!lua_istable(ctx->L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s filter must be table", file);
      goto ret;
    }
    
    size = luaL_getn(ctx->L, 1);
    if (size == 0) {
      snprintf(errbuf, MAX_ERR_LEN, "%s filter element number must >0", file);
      goto ret;
    }
    
    for (int i = 0; i < size; ++i) {
      lua_pushinteger(ctx->L, i+1);
      lua_gettable(ctx->L, 1);
      if (!lua_isnumber(ctx->L, -1)) {
        snprintf(errbuf, MAX_ERR_LEN, "%s filter element must be number", file);
        goto ret;
      }
      ctx->filters.push_back((int) lua_tonumber(ctx->L, -1));
    }
    ctx->filter = filter;
  }
  rc = true;

  ret:
  lua_settop(ctx->L, 0);
  return rc;
}

inline bool ensureCnfCtxFun(CnfCtx *ctx, const char *name)
{
  bool rc;
  lua_getglobal(ctx->L, name);
  rc = lua_isfunction(ctx->L, 1);
  lua_settop(ctx->L, 0);
  return rc;
}

static bool initLuaCtxAggregate(CnfCtx *cnfCtx, LuaCtx *ctx, const char *file, char *errbuf)
{
  bool rc = false;
  const char *fun;
  lua_getglobal(ctx->L, "aggregate");
  
  if (!lua_isnil(ctx->L, 1)) {
    if (lua_isstring(ctx->L, 1)) {
      fun = lua_tostring(ctx->L, 1);
      if (ensureCnfCtxFun(cnfCtx, fun)) {
        ctx->aggregateFun = fun;
      } else {
        snprintf(errbuf, MAX_ERR_LEN, "%s:%s is not a function in main", file, fun);
        goto ret;
      }
    } else if (!lua_isfunction(ctx->L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s aggregate must be function", file);
      goto ret;
    }

    ctx->aggregate = aggregate;
    if (ctx->timeidx == UNSET_INT) {
      snprintf(errbuf, MAX_ERR_LEN, "%s aggreagte must have timeidx", file);
      goto ret;
    }
  }
  rc = true;

  ret:
  lua_settop(ctx->L, 0);
  return rc;
}

static bool initLuaCtxGrep(CnfCtx *cnfCtx, LuaCtx *ctx, const char *file, char *errbuf)
{
  bool rc = false;
  const char *fun;
  lua_getglobal(ctx->L, "grep");
  
  if (!lua_isnil(ctx->L, 1)) {
    if (lua_isstring(ctx->L, 1)) {
      fun = lua_tostring(ctx->L, 1);
      if (ensureCnfCtxFun(cnfCtx, fun)) {
        ctx->grepFun = fun;
      } else {
        snprintf(errbuf, MAX_ERR_LEN, "%s:%s is not a function in main", file, fun);
        goto ret;
      }
    } else if (!lua_isfunction(ctx->L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s grep must be function", file);
      goto ret;
    }
    ctx->grep = grep;
  }
  rc = true;
  
  ret:
  lua_settop(ctx->L, 0);
  return rc;
}

static bool initLuaCtxTransform(CnfCtx *cnfCtx, LuaCtx *ctx, const char *file, char *errbuf)
{
  bool rc = true;
  const char *fun;
  lua_getglobal(ctx->L, "transform");
  
  if (!lua_isnil(ctx->L, 1)) {
    if (lua_isstring(ctx->L, 1)) {
      fun = lua_tostring(ctx->L, 1);
      if (ensureCnfCtxFun(cnfCtx, fun)) {
        ctx->transformFun = fun;
      } else {
        snprintf(errbuf, MAX_ERR_LEN, "%s:%s is not a function in main", file, fun);
        goto ret;
      }
    } else if (!lua_isfunction(ctx->L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s transform must be function", file);
      goto ret;
    }
    ctx->transform = transform;
  }
  rc = true;

  ret:
  lua_settop(ctx->L, 0);
  return rc;  
}

static bool hostAddr(const std::string &host, uint32_t *addr, char *errbuf)
{
  struct addrinfo *ai;
  struct addrinfo  hints;

  memset(&hints, 0x00, sizeof(hints));
  hints.ai_family = AF_INET;

  int rc = getaddrinfo(host.c_str(), NULL, &hints, &ai);
  if (rc != 0) {
    snprintf(errbuf, MAX_ERR_LEN, "getaddrinfo() error %s\n",
             rc == EAI_SYSTEM ? strerror(errno) : gai_strerror(rc));
    return false;
  }

  struct sockaddr_in *in = (struct sockaddr_in *) ai->ai_addr;
  *addr = in->sin_addr.s_addr;
  freeaddrinfo(ai);
  return true;
}

static LuaCtx *loadLuaCtx(CnfCtx *cnfCtx, const char *file, char *errbuf)
{
  int fd;
  LuaCtx *ctx = new LuaCtx;
  
  lua_State *L;
  ctx->L = L = luaL_newstate();
  luaL_openlibs(L);
  if (luaL_dofile(L, file) != 0) {
    snprintf(errbuf, MAX_ERR_LEN, "load %s error\n%s", file, lua_tostring(L, 1));
    goto error;
  }

  lua_getglobal(L, "autocreat");
  if (lua_isboolean(L, 1)) {
    ctx->autocreat = lua_toboolean(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "file");
  if (!lua_isstring(L, 1)) {
    snprintf(errbuf, MAX_ERR_LEN, "%s file must be string", file);
    goto error;
  }

  ctx->file = lua_tostring(L, 1);
  struct stat st;
  if (stat(ctx->file.c_str(), &st) == -1) {
    if (errno == ENOENT && ctx->autocreat) {
      fd = creat(ctx->file.c_str(), 0644);
      if (fd == -1) {
        snprintf(errbuf, MAX_ERR_LEN, "%s file %s autocreat failed", file, ctx->file.c_str());
        goto error;
      }
      safe_close(fd);
    } else {
      snprintf(errbuf, MAX_ERR_LEN, "%s file %s stat failed", file, ctx->file.c_str());
      goto error;
    }
  }
  lua_settop(L, 0);

  lua_getglobal(L, "topic");
  if (!lua_isstring(L, 1)) {
    snprintf(errbuf, MAX_ERR_LEN, "%s topic must be string", file);
    goto error;
  }
  ctx->topic = lua_tostring(L, 1);
  lua_settop(L, 0);

  lua_getglobal(L, "autosplit");
  if (!lua_isnil(L, 1)) {
    if (!lua_isboolean(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s autosplit must be boolean", file);
      goto error;
    }
    ctx->autosplit = lua_toboolean(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "paritition");
  if (!lua_isnil(L, 1)) {
    if (!lua_isnumber(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s partition must be number", file);
      goto error;
    }
    ctx->partition = (int) lua_tonumber(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "autoparti");
  if (!lua_isnil(L, 1)) {
    if (!lua_isboolean(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s autoparti must be boolean", file);
      goto error;
    }
    ctx->autoparti = lua_toboolean(L, 1);
    if (ctx->autoparti) {
      if (!hostAddr(cnfCtx->host, &ctx->addr, errbuf)) goto error;
    }
  }
  lua_settop(L, 0);

  lua_getglobal(L, "rawcopy");
  if (!lua_isnil(L, 1)) {
    if (!lua_isboolean(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s rawcopy must be boolean", file);
      goto error;
    }
    ctx->rawcopy = lua_toboolean(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "timeidx");
  if (!lua_isnil(L, 1)) {
    if (!lua_isnumber(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s timeidx must be number", file);
      goto error;
    }
    ctx->timeidx = (int) lua_tonumber(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "withtime");
  if (!lua_isnil(L, 1)) {
    if (!lua_isboolean(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s withtime must be boolean", file);
      goto error;
    }
    ctx->withtime = lua_toboolean(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "withhost");
  if (!lua_isnil(L, 1)) {
    if (!lua_isboolean(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s withhost must be boolean", file);
      goto error;
    }
    ctx->withhost = lua_toboolean(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "pkey");
  if (lua_isstring(L, 1) || lua_isnumber(L, 1)) {
    ctx->pkey = lua_tostring(L, 1);
  }
  lua_settop(L, 0);
  
  if (!initLuaCtxFilter(ctx, file, errbuf)) goto error;
  if (!initLuaCtxAggregate(cnfCtx, ctx, file, errbuf)) goto error;
  if (!initLuaCtxGrep(cnfCtx, ctx, file, errbuf)) goto error;
  if (!initLuaCtxTransform(cnfCtx, ctx, file, errbuf)) goto error;

  return ctx;
 error:
  unloadLuaCtx(ctx);
  return 0;
}

bool initKafka(CnfCtx *ctx, char *errbuf);
void uninitKafka(CnfCtx *ctx);

void unloadCnfCtx(CnfCtx *ctx)
{
  for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin();
       ite != ctx->luaCtxs.end(); ++ite) {
    LuaCtx *lctx = *ite;
    while (lctx) {
      LuaCtx *next = lctx->next;
      unloadLuaCtx(lctx);
      lctx = next;
    }
  }
  if (ctx->L) lua_close(ctx->L);
  if (ctx->wfd != -1) close(ctx->wfd);
  
  uninitKafka(ctx);
  if (ctx->accept != -1) close(ctx->accept);
  if (ctx->server != -1) close(ctx->server);
  delete ctx;
}

bool shell(const char *cmd, std::string *output, char *errbuf)
{
  FILE *fp = popen(cmd, "r");
  if (!fp) {
    snprintf(errbuf, MAX_ERR_LEN, "%s exec error", cmd);
    return false;
  }

  char buf[256];
  while (fgets(buf, 256, fp)) {
    output->append(buf);
  }

  int status = pclose(fp);
  if (status != 0) {
    snprintf(errbuf, MAX_ERR_LEN, "%s exit %d", cmd, status);
    return false;
  }

  return true;
}  

CnfCtx *loadCnfCtx(const char *file, char *errbuf)
{
  CnfCtx *ctx = new CnfCtx;

  lua_State *L;
  L = luaL_newstate();
  luaL_openlibs(L);
  if (luaL_dofile(L, file) != 0) {
    snprintf(errbuf, MAX_ERR_LEN, "load %s error\n%s", file, lua_tostring(L, 1));
    goto error;
  }

  lua_getglobal(L, "hostshell");
  if (!lua_isstring(L, 1)) {
    snprintf(errbuf, MAX_ERR_LEN, "%s hostshell must be string", file);
    goto error;
  }
  if (!shell(lua_tostring(L, 1), &ctx->host, errbuf)) {
    goto error;
  }
  lua_settop(L, 0);

  lua_getglobal(L, "pidfile");
  if (!lua_isstring(L, 1)) {
    snprintf(errbuf, MAX_ERR_LEN, "%s pidfile must be string", file);
    goto error;
  }
  ctx->pidfile = lua_tostring(L, 1);
  lua_settop(L, 0);

  lua_getglobal(L, "brokers");
  if (!lua_isstring(L, 1)) {
    snprintf(errbuf, MAX_ERR_LEN, "%s borkers must be string", file);
    goto error;
  }
  ctx->brokers = lua_tostring(L, 1);
  lua_settop(L, 0);

  lua_getglobal(L, "partition");
  if (lua_isnil(L, 1)) {
    ctx->partition = 0;
  } else {
    if (!lua_isnumber(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s partition must be number", file);
      goto error;
    }
    ctx->partition = lua_tonumber(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "polllimit");
  if (lua_isnil(L, 1)) {
    ctx->pollLimit = 100;
  } else {
    if (!lua_isnumber(L, 1)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s polllimit must be number", file);
      goto error;
    }
    ctx->pollLimit = lua_tonumber(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "kafka_global");
  if (!lua_istable(L, 1)) {
    snprintf(errbuf, MAX_ERR_LEN, "%s kafka_global must be hash table", file);
    goto error;
  }
  lua_pushnil(L);
  while (lua_next(L, 1) != 0) {
    if (lua_type(L, -2) != LUA_TSTRING) {
      snprintf(errbuf, MAX_ERR_LEN, "%s kafka_global key must be string", file);
      goto error;
    }
    if (lua_type(L, -1) != LUA_TSTRING && lua_type(L, -1) != LUA_TNUMBER) {
      snprintf(errbuf, MAX_ERR_LEN, "%s kafka_global value must be string", file);
      goto error;
    }

    ctx->kafkaGlobal.insert(std::make_pair(lua_tostring(L, -2), lua_tostring(L, -1)));
    lua_pop(L, 1);
  }
  lua_settop(L, 0);

  lua_getglobal(L, "kafka_topic");
  if (!lua_istable(L, 1)) {
    snprintf(errbuf, MAX_ERR_LEN, "%s kafka_topic must be hash table", file);
    goto error;
  }
  lua_pushnil(L);
  while (lua_next(L, 1) != 0) {
    if (lua_type(L, -2) != LUA_TSTRING) {
      snprintf(errbuf, MAX_ERR_LEN, "%s kafka_topic key must be string", file);
      goto error;
    }
    if (lua_type(L, -1) != LUA_TSTRING && lua_type(L, -1) != LUA_TNUMBER) {
      snprintf(errbuf, MAX_ERR_LEN, "%s kafka_topic value must be string", file);
      goto error;
    }

    ctx->kafkaTopic.insert(std::make_pair(lua_tostring(L, -2), lua_tostring(L, -1)));
    lua_pop(L, 1);
  }
  lua_settop(L, 0);

  ctx->L = L;
  return ctx;

  error:
  lua_close(L);
  unloadCnfCtx(ctx);
  return 0;
  
}

CnfCtx *loadCnf(const char *dir, char *errbuf)
{
  DIR *dh = opendir(dir);
  if (!dir) {
    snprintf(errbuf, MAX_ERR_LEN, "could not opendir %s", dir);
    return 0;
  }

  static const size_t N = 1024;
  char fullpath[N];

  snprintf(fullpath, N, "%s/main.lua", dir);

  CnfCtx *ctx = loadCnfCtx(fullpath, errbuf);
  if (!ctx) return 0;

  struct dirent *ent;
  while ((ent = readdir(dh))) {
    size_t len = strlen(ent->d_name);
    if (len <= 4 || ent->d_name[len-4] != '.' ||
        ent->d_name[len-3] != 'l' || ent->d_name[len-2] != 'u' ||
        ent->d_name[len-1] != 'a' || strcmp(ent->d_name, "main.lua") == 0) {
      continue;
    }

    snprintf(fullpath, N, "%s/%s", dir, ent->d_name);
    LuaCtx *lctx = loadLuaCtx(ctx, fullpath, errbuf);
    if (!lctx) {
      unloadCnfCtx(ctx);
      closedir(dh);
      return 0;
    }

    lctx->idx = ctx->count++;
    lctx->main = ctx;

    bool find = false;
    for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin();
         ite != ctx->luaCtxs.end(); ++ite) {
      if ((*ite)->file == lctx->file) {
        lctx->next = *ite;
        *ite = lctx;
        find = true;
        break;
      }
    }
    if (!find) ctx->luaCtxs.push_back(lctx);
  }

  closedir(dh);

  int fd[2];
  if (pipe(fd) == -1) {
    snprintf(errbuf, MAX_ERR_LEN, "pipe error");
    unloadCnfCtx(ctx);
    return 0;
  }
  ctx->accept = fd[0];
  ctx->server = fd[1];
  
  return ctx;
}

bool transform(LuaCtx *ctx, const char *line, size_t nline, std::string *result)
{
  lua_State *L = ctx->transformFun.empty() ? ctx->L : ctx->main->L;
  lua_getglobal(L, ctx->transformFun.empty() ? "transform" : ctx->transformFun.c_str());
  
  lua_pushlstring(L, line, nline);
  if (lua_pcall(L, 1, 1, 0) != 0) {
    fprintf(stderr, "%s transform error %s\n", ctx->file.c_str(), lua_tostring(L, -1));
    lua_settop(L, 0);
    return false;
  }

  if (lua_isnil(L, 1)) {
    result->clear();
    lua_settop(L, 0);
    return true;
  }
  
  if (!lua_isstring(L, 1)) {
    fprintf(stderr, "%s transform return #1 must be string(nil)\n", ctx->file.c_str());
    lua_settop(L, 0);
    return false;
  }

  if (ctx->withhost) result->assign(ctx->main->host).append(1, ' ');
  else result->clear();
  result->append(lua_tostring(L, 1));
  lua_settop(L, 0);
  return true;
}

bool grep(LuaCtx *ctx, const StringList &fields, std::string *result)
{
  lua_State *L = ctx->grepFun.empty() ? ctx->L : ctx->main->L;
  lua_getglobal(L, ctx->grepFun.empty() ? "grep" : ctx->grepFun.c_str());

  lua_newtable(L);
  int table = lua_gettop(L);

  for (size_t i = 0; i < fields.size(); ++i) {
    lua_pushinteger(L, i+1);
    lua_pushstring(L, fields[i].c_str());
    lua_settable(L, table);
  }

  if (lua_pcall(L, 1, 1, 0) != 0) {
    fprintf(stderr, "%s grep error %s\n", ctx->file.c_str(), lua_tostring(L, -1));
    lua_settop(L, 0);
    return false;
  }

  if (lua_isnil(L, 1)) {
    lua_settop(L, 0);
    result->clear();
    return true;
  }

  if (!lua_istable(L, 1)) {
    fprintf(stderr, "%s grep return #1 must be table\n", ctx->file.c_str());
    lua_settop(L, 0);
    return false;
  }

  int size = luaL_getn(L, 1);
  if (size == 0) {
    fprintf(stderr, "%s grep return #1 must be not empty\n", ctx->file.c_str());
    lua_settop(L, 0);
    return false;
  }

  if (ctx->withhost) result->assign(ctx->main->host);
  else result->clear();

  for (int i = 0; i < size; ++i) {
    if (!result->empty()) result->append(1, ' ');
    lua_pushinteger(L, i+1);
    lua_gettable(L, 1);
    if (!lua_isstring(L, -1) && !lua_isnumber(L, -1)) {
      fprintf(stderr, "%s grep return #1[%d] is not string", ctx->file.c_str(), i);
      lua_settop(L, 0);
      return false;
    }
    result->append(lua_tostring(L, -1));
  }

  lua_settop(L, 0);
  return true;
}  

inline int absidx(int idx, size_t total)
{
  assert(total != 0);
  return idx > 0 ? idx-1 : total + idx;
}

inline std::string to_string(int i)
{
  std::string s;
  do {
    s.append(1, i%10 + '0');
    i /= 10;
  } while (i);
  std::reverse(s.begin(), s.end());
  return s;
}

void serializeCache(LuaCtx *ctx, StringPtrList *results)
{
  
  for (SSIHash::iterator ite = ctx->cache.begin(); ite != ctx->cache.end(); ++ite) {
    std::string *s = new std::string;
    if (ctx->withhost) s->append(ctx->main->host).append(1, ' ');
    if (ctx->withtime) s->append(ctx->lasttime).append(1, ' ');

    s->append(ite->first);
    for (SIHash::iterator jte = ite->second.begin(); jte != ite->second.end(); ++jte) {
      s->append(1, ' ').append(jte->first).append(1, '=').append(to_string(jte->second));
    }
    results->push_back(s);
  }
}

void flushCache(CnfCtx *ctx, bool timeout)
{
  for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin(), end = ctx->luaCtxs.end();
       ite != end; ++ite) {
    LuaCtx *lctx = *ite;
    while (lctx) {
      if (!lctx->cache.empty()) {
        if (timeout || lctx->sn + 1000 < ctx->sn) {
          fprintf(stderr, "%s timeout flush cache\n", lctx->file.c_str());
          lctx->sn = ctx->sn;

          OneTaskReq req = {lctx->idx, 0, new StringPtrList};
          serializeCache(lctx, req.datas);

          lctx->lines += req.datas->size();
          ssize_t nn = write(ctx->server, &req, sizeof(OneTaskReq));
          assert(nn != -1 && nn == sizeof(OneTaskReq));
          lctx->cache.clear();
        }
      }
      lctx = lctx->next;
    }
  }
}  

bool aggregate(LuaCtx *ctx, const StringList &fields, StringPtrList *results)
{
  std::string curtime = fields[absidx(ctx->timeidx, fields.size())];
  if (!ctx->lasttime.empty() && curtime != ctx->lasttime) {
    serializeCache(ctx, results);
    ctx->cache.clear();
  }
  ctx->lasttime = curtime;

  lua_State *L = ctx->aggregateFun.empty() ? ctx->L : ctx->main->L;
  lua_getglobal(L, ctx->aggregateFun.empty() ? "aggregate": ctx->aggregateFun.c_str());

  lua_newtable(L);
  int table = lua_gettop(L);
  
  for (size_t i = 0; i < fields.size(); ++i) {
    lua_pushinteger(L, i+1);
    lua_pushstring(L, fields[i].c_str());
    lua_settable(L, table);
  }

  if (lua_pcall(L, 1, 2, 0) != 0) {
    fprintf(stderr, "%s aggregate error %s\n", ctx->file.c_str(), lua_tostring(L, -1));
    lua_settop(L, 0);
    return false;
  }

  if (lua_isnil(L, 1)) {
    lua_settop(L, 0);
    return false;
  }

  if (!lua_isstring(L, 1)) {
    fprintf(stderr, "%s aggregate return #1 must be string\n", ctx->file.c_str());
    lua_settop(L, 0);
    return false;
  }
  std::string pkey = lua_tostring(L, 1);

  if (!lua_istable(L, 2)) {
    fprintf(stderr, "%s aggregate return #2 must be hash table\n", ctx->file.c_str());
    lua_settop(L, 0);
    return false;
  }
  lua_pushnil(L);  
  while (lua_next(L, 2) != 0) {
    if (lua_type(L, -2) != LUA_TSTRING) {
      fprintf(stderr, "%s aggregate return #3 key must be string\n", ctx->file.c_str());
      lua_settop(L, 0);
      return false;
    }
    if (lua_type(L, -1) != LUA_TNUMBER) {
      fprintf(stderr, "%s aggregate return #3 value must be number\n", ctx->file.c_str());
      lua_settop(L, 0);
      return false;
    }

    std::string key = lua_tostring(L, -2);
    int value = (int) lua_tonumber(L, -1);

    ctx->cache[pkey][key] += value;
    if (!ctx->pkey.empty()) ctx->cache[ctx->pkey][key] += value;
    lua_pop(L, 1);
  }

  lua_settop(L, 0);
  return true;
}

bool filter(LuaCtx *ctx, const StringList &fields, std::string *result)
{
  std::string s;

  if (ctx->withhost) result->assign(ctx->main->host);
  else result->clear();
  
  for (IntList::iterator ite = ctx->filters.begin();
       ite != ctx->filters.end(); ++ite) {
    int idx = absidx(*ite, fields.size());
    if (idx < 0 || (size_t) idx >= fields.size()) return false;

    if (!result->empty()) result->append(1, ' ');
    result->append(fields[idx]);
  }
  return true;
}

struct Signal {
  int signo;
  Want want;
};

static const Signal signals[] = {
  {SIGTERM, STOP},
  {SIGHUP,  RELOAD},
  {SIGCHLD, START2},
};

void sigHandle(int signo)
{
  if (signo == SIGTERM) want = STOP;
  else if (signo == SIGHUP) want = RELOAD;
  else if (signo == SIGCHLD) want = START2;
}

bool initSignal(char *errbuf)
{
  struct sigaction sa;
  for (size_t i = 0; i < sizeof(signals)/sizeof(signals[0]); ++i) {
    memset(&sa, 0x00, sizeof(sa));
    sa.sa_handler = sigHandle;
    sigemptyset(&sa.sa_mask);
    if (sigaction(signals[i].signo, &sa, NULL) == -1) {
      snprintf(errbuf, MAX_ERR_LEN, "sigaction error %d:%s", errno, strerror(errno));
      return false;
    }
  }
  return true;
}

inline bool copyRawRequired(LuaCtx *ctx)
{
#ifdef DISABLE_COPYRAW
  return false;
#else
  return !(ctx->transform || ctx->aggregate || ctx->filter || ctx->grep) &&
    ctx->main->pollLimit && ctx->rawcopy;
#endif
}

bool processLine(LuaCtx *ctx, char *line, size_t nline);
void processLines(LuaCtx *ctx)
{
  size_t n = 0;
  char *pos;

  if (copyRawRequired(ctx)) {
    if ((pos = (char *) memrchr(ctx->buffer, NL, ctx->npos))) {
      processLine(ctx, ctx->buffer, pos - ctx->buffer);
      n = (pos+1) - ctx->buffer;
    }
  } else {
    while ((pos = (char *) memchr(ctx->buffer + n, NL, ctx->npos - n))) {
      processLine(ctx, ctx->buffer + n, pos - (ctx->buffer + n));
      n = (pos+1) - ctx->buffer;
      if (n == ctx->npos) break;
    }
  }

  if (n == 0) {
    if (ctx->npos == MAX_LINE_LEN) {
      fprintf(stderr, "%s line length exceed, truncate\n", ctx->file.c_str());
      ctx->npos = 0;
    }
  } else if (ctx->npos > n) {
    ctx->npos -= n;
    memmove(ctx->buffer, ctx->buffer + n, ctx->npos);
  } else {
    ctx->npos = 0;
  }
}  

inline void propagateTailContent(LuaCtx *ctx, size_t size, uint64_t sn)
{
  LuaCtx *nxt = ctx->next;
  while (nxt) {
    memcpy(nxt->buffer + nxt->npos, ctx->buffer + ctx->npos, size);
    nxt->npos += size;
    nxt->sn = sn;
    nxt = nxt->next;
  }
  ctx->sn = sn;
  ctx->npos += size;
}

inline void propagateProcessLines(LuaCtx *ctx)
{
  while (ctx) {
    processLines(ctx);
    ctx = ctx->next;
  }
}

bool tail2kafka(LuaCtx *ctx, uint64_t sn)
{
  struct stat st;
  if (fstat(ctx->fd, &st) != 0) {
    fprintf(stderr, "%s %d stat error\n", ctx->file.c_str(), ctx->fd);
    return false;
  }
  ctx->size = st.st_size;

  off_t off = lseek(ctx->fd, 0, SEEK_CUR);
  if (off == (off_t) -1) {
    fprintf(stderr, "%s seek cur error\n", ctx->file.c_str());
    return false;
  }

  if (off > ctx->size) {
    ctx->stat |= FILE_TRUNCATED;
    return true;
  }
  
  while (off < ctx->size) {
    size_t min = std::min(ctx->size - off, (off_t) (MAX_LINE_LEN - ctx->npos));
    assert(min > 0);
    ssize_t nn = read(ctx->fd, ctx->buffer + ctx->npos, min);
    if (nn == -1) {
      fprintf(stderr, "%s read error\n", ctx->file.c_str());
      return false;
    } else if (nn == 0) { // file was truncated
      ctx->stat |= FILE_TRUNCATED;
      break;
    }
    off += nn;

    propagateTailContent(ctx, nn, sn);
    propagateProcessLines(ctx);
  }
  return true;
}

bool lineAlign(LuaCtx *ctx)
{
  if (ctx->size == 0) return true;

  off_t min = std::min(ctx->size, (off_t) MAX_LINE_LEN);
  lseek(ctx->fd, ctx->size - min, SEEK_SET);

  if (read(ctx->fd, ctx->buffer, MAX_LINE_LEN) != min) {
    return false;
  }
  
  LuaCtx *nxt = ctx->next;
  while (nxt) {
    memcpy(nxt->buffer, ctx->buffer, min);
    nxt = nxt->next;
  }

  nxt = ctx;
  while (nxt) {
    char *pos = (char *) memrchr(nxt->buffer, NL, min);
    if (!pos) return false;

    nxt->npos = nxt->buffer + min - (pos+1);
    memmove(nxt->buffer, pos+1, nxt->npos);
    nxt = nxt->next;
  }
  return true;
}

/* watch IN_DELETE_SELF does not work
 * luactx hold fd to the deleted file, the file will never be real deleted
 * so DELETE will be inotified
 */
static const uint32_t WATCH_EVENT = IN_MODIFY | IN_MOVE_SELF;

void closeParentFd(CnfCtx *ctx)
{
  for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin(); ite != ctx->luaCtxs.end(); ++ite) {
    LuaCtx *lctx = *ite;
    if (lctx->fd != -1) safe_close(lctx->fd);
  }
}

bool addWatch(CnfCtx *ctx, char *errbuf)
{
  for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin(); ite != ctx->luaCtxs.end(); ++ite) {
    LuaCtx *lctx = *ite;

    if (lctx->fd != -1) close(lctx->fd);
    for (int i = 0; i < 15; i++) {
      lctx->fd = open(lctx->file.c_str(), O_RDONLY);
      // try best to watch the file
      if (lctx->fd == -1) sleep(1);
      else break;
    }
    if (lctx->fd == -1) {
      snprintf(errbuf, MAX_ERR_LEN, "%s open error", lctx->file.c_str());
      return false;
    }
    
    struct stat st;
    fstat(lctx->fd, &st);
    lctx->size = st.st_size;
    lctx->inode = st.st_ino;
    lctx->stat = 0;
    lctx->lines = 0;
    lctx->start = time(0);

    if (!lineAlign(lctx)) {
      snprintf(errbuf, MAX_ERR_LEN, "%s align new line error", lctx->file.c_str());
      return false;
    }

    int wd = inotify_add_watch(ctx->wfd, lctx->file.c_str(), WATCH_EVENT);
    if (wd == -1) {
      snprintf(errbuf, MAX_ERR_LEN, "%s add watch error %d:%s", lctx->file.c_str(),
               errno, strerror(errno));
      return false;
    }
    ctx->wch.insert(std::make_pair(wd, lctx));
  }
  return true;
}

void tryReWatch(CnfCtx *ctx)
{
  for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin(), end = ctx->luaCtxs.end();
       ite != end; ++ite) {
    LuaCtx *lctx = *ite;
    if (lctx->fd != -1) continue;
    lctx->fd = open(lctx->file.c_str(), O_RDONLY);
    if (lctx->fd != -1) {
      struct stat st;
      fstat(lctx->fd, &st);

      printf("rewatch %s\n", lctx->file.c_str());
        
      int wd = inotify_add_watch(ctx->wfd, lctx->file.c_str(), WATCH_EVENT);
      ctx->wch.insert(std::make_pair(wd, lctx));
      lctx->inode = st.st_ino;
      lctx->stat = 0;
      lctx->lines = 0;
      lctx->start = time(0);

      tail2kafka(lctx, ctx->sn);
    }
  }
}

/* moved */
inline void tryRmWatch(LuaCtx *ctx, int wd)
{
  printf("tag remove %d %s\n", wd, ctx->file.c_str());
  ctx->stat |= FILE_MOVED;
}

/* unlink or truncate */
void tryRmWatch(CnfCtx *ctx)
{
  for (WatchCtxHash::iterator ite = ctx->wch.begin(); ite != ctx->wch.end(); ) {
    LuaCtx *lctx = ite->second;
    
    struct stat st;
    fstat(lctx->fd, &st);

    // if file was unlinked, moved or truncated
    if ((lctx->stat & FILE_MOVED) || (lctx->stat & FILE_TRUNCATED) ||
        st.st_ino != lctx->inode || st.st_nlink == 0 || st.st_size < lctx->size) {
      printf("remove %s, %d lines sended in %d seconds\n", lctx->file.c_str(), lctx->lines,
             (int) (time(0) - lctx->start));

      inotify_rm_watch(lctx->main->wfd, ite->first);
      lctx->main->wch.erase(ite++);
      safe_close(lctx->fd);
    } else {
      ++ite;
    }
  }
}

inline LuaCtx *wd2ctx(CnfCtx *ctx, int wd)
{
  WatchCtxHash::iterator pos = ctx->wch.find(wd);
  if (pos != ctx->wch.end()) return pos->second;
  else return 0;
}

bool watchInit(CnfCtx *ctx, char *errbuf)
{
  // inotify_init1 Linux 2.6.27
  if (ctx->wfd == -1) {
    ctx->wfd = inotify_init();
    if (ctx->wfd == -1) {
      snprintf(errbuf, MAX_ERR_LEN, "inotify_init error");
      return false;
    }
    
    int nb = 1;
    ioctl(ctx->wfd, FIONBIO, &nb);
  }

  return addWatch(ctx, errbuf);
}

inline void nanosleep(int ms)
{
  struct timespec spec = {0, ms * 1000 * 1000};
  nanosleep(&spec, 0);
}

bool watchLoop(CnfCtx *ctx)
{
  const size_t eventBufferSize = ctx->count * ONE_EVENT_SIZE * 5;
  char *eventBuffer = (char *) malloc(eventBufferSize);

  struct pollfd fds[] = {
    {ctx->wfd, POLLIN, 0 }
  };

  while (want == WAIT) {
    int nfd = poll(fds, 1, 500);
    if (nfd == -1) {
      if (errno != EINTR) return false;
    } else if (nfd == 0) {
      flushCache(ctx, true);
    } else {
      ctx->sn++;    

      ssize_t nn = read(ctx->wfd, eventBuffer, eventBufferSize);
      assert(nn > 0);

      char *p = eventBuffer;
      while (p < eventBuffer + nn) {
        /* IN_IGNORED when watch was removed */
        struct inotify_event *event = (struct inotify_event *) p;
        if (event->mask & IN_MODIFY) {
          LuaCtx *lctx = wd2ctx(ctx, event->wd);
          if (lctx) tail2kafka(lctx, ctx->sn);
        }
        if (event->mask & IN_MOVE_SELF) {
          LuaCtx *lctx = wd2ctx(ctx, event->wd);
          if (lctx) tryRmWatch(lctx, event->wd);
        }
        p += sizeof(struct inotify_event) + event->len;
      }

      flushCache(ctx, false);
    }
    tryRmWatch(ctx);
    tryReWatch(ctx);
    if (ctx->pollLimit) nanosleep(ctx->pollLimit);
  }

  want = STOP;
  return true;
}

void split(const char *line, size_t nline, StringList *items)
{
  bool esc = false;
  char want = '\0';
  size_t pos = 0;

  for (size_t i = 0; i < nline; ++i) {
    if (esc) {
      esc = false;
    } else if (line[i] == '\\') {
      esc = true;
    } else if (want == '"') {
      if (line[i] == '"') {
        want = '\0';
        items->push_back(std::string(line + pos, i-pos));
        pos = i+1;
      }
    } else if (want == ']') {
      if (line[i] == ']') {
        want = '\0';
        items->push_back(std::string(line + pos, i-pos));
        pos = i+1;
      }
    } else {
      if (line[i] == '"') {
        want = line[i];
        pos++;
      } else if (line[i] == '[') {
        want = ']';
        pos++;
      } else if (line[i] == ' ') {
        if (i != pos) items->push_back(std::string(line + pos, i - pos));
        pos = i+1;
      }
    }
  }
  if (pos != nline) items->push_back(std::string(line + pos, nline - pos));
}

static const char *MonthAlpha[12] = {
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };

// 28/Feb/2015:12:30:23 +0800 -> 2015-03-30T16:31:53
bool iso8601(const std::string &t, std::string *iso)
{
  enum { WaitYear, WaitMonth, WaitDay, WaitHour, WaitMin, WaitSec } status = WaitDay;
  int year, mon, day, hour, min, sec;
  year = mon = day = hour = min = sec = 0;
  
  const char *p = t.c_str();
  while (*p && *p != ' ') {
    if (*p == '/') {
      if (status == WaitDay) status = WaitMonth;
      else if (status == WaitMonth) status = WaitYear;
      else return false;
    } else if (*p == ':') {
      if (status == WaitYear) status = WaitHour;
      else if (status == WaitHour) status = WaitMin;
      else if (status == WaitMin) status = WaitSec;
      else return false;
    } else if (*p >= '0' && *p <= '9') {
      int n = *p - '0';
      if (status == WaitYear) year = year * 10 + n;
      else if (status == WaitDay) day = day * 10 + n;
      else if (status == WaitHour) hour = hour * 10 + n;
      else if (status == WaitMin) min = min * 10 + n;
      else if (status == WaitSec) sec = sec * 10 + n;
      else return false;
    } else if (status == WaitMonth) {
      size_t i;
      for (i = 0; i < 12; ++i) {
        if (strncmp(p, MonthAlpha[i], 3) == 0) {
          mon = i+1;
          break;
        }
      }
    } else {
      return false;
    }
    p++;
  }

  iso->reserve(sizeof("yyyy-mm-ddThh:mm:ss"));
  iso->resize(sizeof("yyyy-mm-ddThh:mm:ss")-1);
  sprintf((char *) iso->data(), "%04d-%02d-%02dT%02d:%02d:%02d",
          year, mon, day, hour, min, sec);

  return true;
}

/* line without NL */
bool processLine(LuaCtx *ctx, char *line, size_t nline)
{
  /* ignore empty line */
  if (nline == 0) return true;

  OneTaskReq req = {ctx->idx, 0, 0};
  bool rc;
  
  if (ctx->transform) {
    req.data = new std::string;
    rc = ctx->transform(ctx, line, nline-1, req.data);
  } else if (ctx->aggregate || ctx->filter || ctx->grep) {
    StringList fields;
    split(line, nline, &fields);
		
		if (ctx->timeidx != UNSET_INT) {
			int idx = absidx(ctx->timeidx, fields.size());
			if (idx < 0 || (size_t) idx >= fields.size()) return false;
			iso8601(fields[idx], &fields[idx]);
		}
		
    if (ctx->aggregate) {
      req.datas = new StringPtrList;
      rc = ctx->aggregate(ctx, fields, req.datas);
    } else {
      req.data = new std::string;
      rc = ctx->grep ? ctx->grep(ctx, fields, req.data) :
				ctx->filter(ctx, fields, req.data);
    }
  } else {
    req.data = new std::string(line, nline);
    req.data->append(1, '\n');
    rc = true;
  }
  
  if (req.data || (req.datas && !req.datas->empty())) {
    if (req.data) ++ctx->lines;
    else ctx->lines += req.datas->size();

    ssize_t nn = write(ctx->main->server, &req, sizeof(OneTaskReq));
    assert(nn != -1 && nn == sizeof(OneTaskReq));
  } else if (req.datas) {  // if req.datas->empty()
    delete req.datas;
  }

  return rc;
}

static void dr_cb(
  rd_kafka_t *, void *, size_t, rd_kafka_resp_err_t, void *, void *data)
{
  std::string *p = (std::string *) data;
  delete p;
}

static int32_t partitioner_cb (
  const rd_kafka_topic_t *, const void *, size_t, int32_t pc, void *opaque, void *)
{
  LuaCtx *ctx = (LuaCtx *) opaque;
  if (ctx->partition == RD_KAFKA_PARTITION_UA) {
    if (ctx->autoparti) {
      ctx->partition = (ntohl(ctx->addr) & 0xff) % pc;
      return ctx->partition;
    } else {
      return ctx->main->partition;
    }
  } else {
    return ctx->partition;
  }
}

bool initKafka(CnfCtx *ctx, char *errbuf)
{
  char errstr[512];
  
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  for (SSHash::iterator ite = ctx->kafkaGlobal.begin();
       ite != ctx->kafkaGlobal.end(); ++ite) {
    rd_kafka_conf_res_t res;
    res = rd_kafka_conf_set(conf, ite->first.c_str(), ite->second.c_str(),
                            errstr, sizeof(errstr));
    if (res != RD_KAFKA_CONF_OK) {
      snprintf(errbuf, MAX_ERR_LEN, "kafka conf %s=%s %s",
               ite->first.c_str(), ite->second.c_str(), errstr);
      rd_kafka_conf_destroy(conf);
      return false;
    }
  }
  rd_kafka_conf_set_dr_cb(conf, dr_cb);

  /* rd_kafka_t will own conf */
  ctx->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!ctx->rk) {
    snprintf(errbuf, MAX_ERR_LEN, "new kafka produce error %s\n", errstr);
    return false;
  }

  if (rd_kafka_brokers_add(ctx->rk, ctx->brokers.c_str()) < 1) {
    snprintf(errbuf, MAX_ERR_LEN, "kafka invalid brokers %s\n", ctx->brokers.c_str());
    return false;
  }

  ctx->rkts.resize(ctx->count);
  for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin(); ite != ctx->luaCtxs.end(); ++ite) {
    LuaCtx *lctx = (*ite);
    while (lctx) {
      rd_kafka_topic_conf_t *tconf = rd_kafka_topic_conf_new();
      for (SSHash::iterator jte = ctx->kafkaTopic.begin(); jte != ctx->kafkaTopic.end(); ++jte) {
        rd_kafka_conf_res_t res;
        res = rd_kafka_topic_conf_set(tconf, jte->first.c_str(), jte->second.c_str(),
                                      errstr, sizeof(errstr));
        if (res != RD_KAFKA_CONF_OK) {
          snprintf(errbuf, MAX_ERR_LEN, "kafka topic conf %s=%s %s\n",
                   jte->first.c_str(), jte->second.c_str(), errstr);
          rd_kafka_topic_conf_destroy(tconf);
          return false;
        }
      }

      rd_kafka_topic_conf_set_opaque(tconf, lctx);
      rd_kafka_topic_conf_set_partitioner_cb(tconf, partitioner_cb);
      
      rd_kafka_topic_t *rkt;
      /* rd_kafka_topic_t will own tconf */
      rkt = rd_kafka_topic_new(ctx->rk, lctx->topic.c_str(), tconf);
      if (!rkt) {
        snprintf(errbuf, MAX_ERR_LEN, "kafka_topic_new error");
        return false;
      }
      ctx->rkts[lctx->idx] = rkt;
      lctx = lctx->next;
    }
  }

  return true;
}

void uninitKafka(CnfCtx *ctx)
{
  for (TopicList::iterator ite = ctx->rkts.begin(); ite != ctx->rkts.end(); ++ite) {
    rd_kafka_topic_destroy(*ite);
  }
  if (ctx->rk) rd_kafka_destroy(ctx->rk);
}

void kafka_produce(rd_kafka_t *rk, rd_kafka_topic_t *rkt, std::string *data)
{
  int i = 0;
  again:
  int rc = rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, 0,
                            (void *) data->c_str(), data->size(),
                            0, 0, data);
  if (rc != 0) {
    if (errno == ENOBUFS) {
      fprintf(stderr, "%s kafka produce error(#%d) %s\n", rd_kafka_topic_name(rkt), ++i,
              strerror(errno));
      rd_kafka_poll(rk, 10);
      goto again;
    } else {
      fprintf(stderr, "%s kafka produce error %d:%s\n", rd_kafka_topic_name(rkt), errno,
              strerror(errno));
      delete data;
    }
  }
}

void kafka_produce(rd_kafka_t *rk, rd_kafka_topic_t *rkt, StringPtrList *datas)
{
  std::vector<rd_kafka_message_t> rkmsgs;  
  rkmsgs.resize(datas->size());
  
  size_t i = 0;
  for (StringPtrList::iterator ite = datas->begin(), end = datas->end();
       ite != end; ++ite, ++i) {
    rkmsgs[i].payload  = (void *) (*ite)->c_str();
    rkmsgs[i].len      = (*ite)->size();
    rkmsgs[i].key      = 0;
    rkmsgs[i].key_len  = 0;
    rkmsgs[i]._private = *ite;
    // printf("%s kafka produce '%s'\n", rd_kafka_topic_name(rkt), (*ite)->c_str());
  }
    
  rd_kafka_produce_batch(rkt, RD_KAFKA_PARTITION_UA, 0,
                         &rkmsgs[0], rkmsgs.size());

  for (std::vector<rd_kafka_message_t>::iterator ite = rkmsgs.begin(), end = rkmsgs.end();
       ite != end; ++ite) {
    if (ite->err) {
      fprintf(stderr, "%s kafka produce batch error %s\n", rd_kafka_topic_name(rkt),
              rd_kafka_message_errstr(&(*ite)));
      
      rd_kafka_poll(rk, 10);
      kafka_produce(rk, rkt, (std::string *) ite->_private);
    }
  }
}
    
void *routine(void *data)
{
  CnfCtx *ctx = (CnfCtx *) data;
  OneTaskReq req;

  while (want == WAIT) {
    ssize_t nn = read(ctx->accept, &req, sizeof(OneTaskReq));
    if (nn == -1) {
      if (errno != EINTR) break;
      else continue;
    } else if (nn == 0) {
      break;
    }

    if (nn != sizeof(OneTaskReq)) {
      fprintf(stderr, "invalid req size %d\n", (int) nn);
      break;
    }

    if (!req.data && !req.datas) break;  // terminate task

    rd_kafka_topic_t *rkt = ctx->rkts[req.idx];

    if (req.data) {
      kafka_produce(ctx->rk, rkt, req.data);
    } else {
      kafka_produce(ctx->rk, rkt, req.datas);
    }

    delete req.datas;
    rd_kafka_poll(ctx->rk, 0);
  }
  
  want = STOP;
  fprintf(stderr, "routine exit\n");
  return NULL;
}

/* pidfile may stale, this's not a perfect method */
bool initSingleton(CnfCtx *ctx, char *errbuf)
{
  int fd = open(ctx->pidfile.c_str(), O_CREAT | O_WRONLY, 0644);
  if (lockf(fd, F_TLOCK, 0) == 0) {
    ftruncate(fd, 0);
    std::string pid = to_string(getpid());
    write(fd, pid.c_str(), pid.size());
    return true;
  } else {
    snprintf(errbuf, MAX_ERR_LEN, "lock %s failed", ctx->pidfile.c_str());
    return false;
  }
}

inline void terminateRoutine(CnfCtx *ctx)
{
  OneTaskReq req = {0, 0, 0};
  write(ctx->server, &req, sizeof(req));
}

int runForeGround(CnfCtx *ctx, char *errbuf)
{
  if (!watchInit(ctx, errbuf)) {
    fprintf(stderr, "watch init error %s\n", errbuf);
    return EXIT_FAILURE;
  }

  want = WAIT;
    
  sigset_t set;
  sigemptyset(&set);
  sigprocmask(SIG_SETMASK, &set, NULL);
    
  /* initKafka startup librdkafka thread */
  if (!initKafka(ctx, errbuf)) {
    fprintf(stderr, "init kafka error %s\n", errbuf);
    exit(EXIT_FAILURE);
  }
    
  pthread_t tid;
  pthread_create(&tid, NULL, routine, ctx);
  watchLoop(ctx);
  terminateRoutine(ctx);
  pthread_join(tid, NULL);
  unloadCnfCtx(ctx);

  return EXIT_SUCCESS;
}

pid_t spawn(CnfCtx *ctx, CnfCtx *octx, char *errbuf)
{
  if (!watchInit(ctx, errbuf)) {
    fprintf(stderr, "watch init error %s\n", errbuf);
    return -1;
  }

  /* unload old ctx before fork */
  if (octx) unloadCnfCtx(octx);

  int pid = fork();
  if (pid == 0) {
    want = WAIT;
    
    sigset_t set;
    sigemptyset(&set);
    sigprocmask(SIG_SETMASK, &set, NULL);
    
    /* initKafka startup librdkafka thread */
    if (!initKafka(ctx, errbuf)) {
      fprintf(stderr, "init kafka error %s\n", errbuf);
      exit(EXIT_FAILURE);
    }
    
    pthread_t tid;
    pthread_create(&tid, NULL, routine, ctx);
    watchLoop(ctx);
    terminateRoutine(ctx);    
    pthread_join(tid, NULL);
    unloadCnfCtx(ctx);
    exit(EXIT_SUCCESS);
  }

  closeParentFd(ctx);
  return pid;
}

#ifdef UNITTEST
#define check(r, fmt, arg...) \
  do { if (!(r)) { fprintf(stderr, "%04d %s -> "fmt"\n", __LINE__, #r, ##arg); exit(1); } } while(0)

#define TEST(x) test_##x

void TEST(split)()
{
  StringList list;
  
  const char *s1 = "hello \"1 [] 2\"[world] [] [\"\"]  bj";
  split(s1, strlen(s1), &list);
  check(list.size() == 6, "%d", (int) list.size());
  assert(list[0] == "hello");
  assert(list[1] == "1 [] 2");
  assert(list[2] == "world");
  assert(list[3] == "");
  check(list[4] == "\"\"", "%s", list[4].c_str());
  assert(list[5] == "bj");
}

void TEST(iso8601)()
{
  std::string iso;
  bool rc;

  rc = iso8601("28/Feb/2015:12:30:23", &iso);
  check(rc, "%s", "28/Feb/2015:12:30:23");
  check(iso == "2015-02-28T12:30:23", "%s", iso.c_str());

  rc = iso8601("28/Feb:12:30:23", &iso);
  check(rc == false, "%s", "28/Feb:12:30:23");

  rc = iso8601("28/Feb/2015:12:30", &iso);
  check(rc, "%s", "28/Feb/2015:12:30");
  check(iso == "2015-02-28T12:30:00", "%s", iso.c_str());
}

void TEST(loadLuaCtx)()
{
  char errbuf[MAX_ERR_LEN];
  LuaCtx *ctx;

  CnfCtx cnf;
  cnf.host = "127.0.0.1";
  
  ctx = loadLuaCtx(&cnf, "./basic.lua", errbuf);
  check(ctx, "%s", errbuf);
  check(ctx->fd == -1, "%d", ctx->fd);
  check(ctx->file == "./basic.log", "%s", ctx->file.c_str());
  check(ctx->topic == "basic", "%s", ctx->topic.c_str());
  check(ctx->autoparti, "%s", (ctx->autoparti ? "TRUE" : "FALSE"));
  check(ctx->partition == RD_KAFKA_PARTITION_UA, "%d", ctx->partition);
  unloadLuaCtx(ctx);

  ctx = loadLuaCtx(0, "./filter.lua", errbuf);
  check(ctx, "%s", errbuf);
  check(ctx->autosplit == false, "%s", (ctx->autosplit ? "TRUE" : "FALSE"));
  check(ctx->timeidx == 4, "%d", ctx->timeidx);
  check(ctx->filters.size() == 4, "%d", (int) ctx->filters.size());
  check(ctx->filters[0] == 4, "%d", ctx->filters[0]);
  check(ctx->filters[1] == 5, "%d", ctx->filters[1]);
  check(ctx->filters[2] == 6, "%d", ctx->filters[2]);
  check(ctx->filters[3] == -1, "%d", ctx->filters[3]);
  check(ctx->filter, "%s", (ctx->filter ? "FUNC" : "NULL"));
  unloadLuaCtx(ctx);

  ctx = loadLuaCtx(0, "./aggregate.lua", errbuf);
  check(ctx, "%s", errbuf);
  check(ctx->autosplit == true, "%s", (ctx->autosplit ? "TRUE" : "FALSE"));
  check(ctx->withhost == true, "%s", (ctx->withhost ? "TRUE" : "FALSE"));
  check(ctx->withtime == true, "%s", (ctx->withtime ? "TRUE" : "FALSE"));
  check(ctx->aggregate, "%s", (ctx->aggregate ? "FUNC" : "NULL"));
  unloadLuaCtx(ctx);

  ctx = loadLuaCtx(0, "./transform.lua", errbuf);
  check(ctx, "%s", errbuf);
  check(ctx->transform, "%s", (ctx->transform ? "FUNC" : "NULL"));  
}

void TEST(loadCnf)()
{
  char errbuf[MAX_ERR_LEN];
  CnfCtx *ctx;
  
  ctx = loadCnf(".", errbuf);
  check(ctx != 0, "loadCnf . %s", errbuf);
  
  assert(ctx->rk == 0);

  check(ctx->host == "zzyong.paas.user.vm", "%s", ctx->host.c_str());
  check(ctx->brokers == "127.0.0.1:9092", "%s", ctx->brokers.c_str());
  check(ctx->partition == 1, "%d", ctx->partition);
  check(ctx->pollLimit == 300, "%d", ctx->pollLimit);
  check(ctx->kafkaGlobal["client.id"] == "tail2kafka",
        "%s", ctx->kafkaGlobal["client.id"].c_str());
  check(ctx->kafkaTopic["request.required.acks"] == "1",
        "%s", ctx->kafkaTopic["request.required.acks"].c_str());
  
  check(ctx->count == 5, "%d", (int) ctx->count);
  for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin(); ite != ctx->luaCtxs.end(); ++ite) {
    assert((*ite)->main == ctx);
  }
  unloadCnfCtx(ctx);
}

void TEST(transform)()
{
  char errbuf[MAX_ERR_LEN];
  CnfCtx *main;
  LuaCtx *ctx;
  std::string data;

  main = loadCnf(".", errbuf);
  assert(main != 0);

  ctx = loadLuaCtx(0, "./transform.lua", errbuf);
  assert(ctx != 0);
  ctx->main = main;

  ctx->transform(ctx, "[error] this", sizeof("[error] this")-1, &data);
  check(data == main->host + " [error] this", "'%s'", data.c_str());

  ctx->withhost = false;
  ctx->transform(ctx, "[error] this", sizeof("[error] this")-1, &data);
  check(data == "[error] this", "'%s'", data.c_str());  

  ctx->transform(ctx, "[debug] that", sizeof("[debug] that")-1, &data);
  check(data.empty() == true, "'%s'", data.empty() ? "TRUE" : "FALSE");

  unloadLuaCtx(ctx);  
}

void TEST(aggregate)()
{
  char errbuf[MAX_ERR_LEN];
  CnfCtx *main;
  LuaCtx *ctx;
  StringPtrList datas;
    
  main = loadCnf(".", errbuf);
  assert(main != 0);

  ctx = loadLuaCtx(0, "./aggregate.lua", errbuf);
  assert(ctx != 0);
  ctx->main = main;

  const char *fields1[] = {
    "-", "-", "-", "2015-04-02T12:05:04", "-",
    "-", "-", "-", "200", "230",
    "0.1", "-", "-", "-", "-",
    "10086"};

  ctx->aggregate(ctx, StringList(fields1, fields1 + 16), &datas);
  check(datas.empty() == true, "%s", datas.empty() ? "TRUE" : "FALSE");

  const char *fields2[] = {
    "-", "-", "-", "2015-04-02T12:05:04", "-",
    "-", "-", "-", "200", "270",
    "0.2", "-", "-", "-", "-",
    "10086"};
  
  ctx->aggregate(ctx, StringList(fields2, fields2 + 16), &datas);
  check(datas.empty() == true, "%s", datas.empty() ? "TRUE" : "FALSE");  

  const char *fields3[] = {
    "-", "-", "-", "2015-04-02T12:05:05", "-",
    "-", "-", "-", "404", "250",
    "0.2", "-", "-", "-", "-",
    "95555"};
  ctx->aggregate(ctx, StringList(fields3, fields3 + 16), &datas);
  check(datas.size() == 2, "%d", (int) datas.size());
  const char *msg = "2015-04-02T12:05:04 10086 reqt<0.1=1 reqt<0.3=1 size=500 status_200=2";
  check((*datas[0]) == main->host + " " + msg, "%s", datas[0]->c_str());
  msg = "2015-04-02T12:05:04 yuntu reqt<0.1=1 reqt<0.3=1 size=500 status_200=2";
  check((*datas[1]) == main->host + " " + msg, "%s", datas[0]->c_str());  
  
  unloadLuaCtx(ctx);  
}

void TEST(grep)()
{
  char errbuf[MAX_ERR_LEN];
  CnfCtx *main;
  LuaCtx *ctx;
  std::string data;
    
  main = loadCnf(".", errbuf);
  assert(main != 0);

  ctx = loadLuaCtx(main, "./grep.lua", errbuf);
  assert(ctx != 0);
  ctx->main = main;
  
  const char *fields1[] = {
    "-", "-", "-", "2015-04-02T12:05:05", "GET / HTTP/1.0",
    "200", "-", "-", "95555"};
  
  ctx->grep(ctx, StringList(fields1, fields1+9), &data);
  check(data == main->host + " [2015-04-02T12:05:05] \"GET / HTTP/1.0\" 200 95555",
        "%s", data.c_str());

  unloadLuaCtx(ctx);
}

void TEST(filter)()
{
  char errbuf[MAX_ERR_LEN];
  CnfCtx *main;
  LuaCtx *ctx;
  std::string data;
    
  main = loadCnf(".", errbuf);
  assert(main != 0);

  ctx = loadLuaCtx(main, "./filter.lua", errbuf);
  assert(ctx != 0);
  ctx->main = main;
  
  const char *fields1[] = {
    "-", "-", "-", "2015-04-02T12:05:05", "GET / HTTP/1.0",
    "200", "-", "-", "95555"};
  
  ctx->filter(ctx, StringList(fields1, fields1+9), &data);
  check(data == main->host + " 2015-04-02T12:05:05 GET / HTTP/1.0 200 95555",
        "%s", data.c_str());

  unloadLuaCtx(ctx);  
}

void *TEST(watchLoop)(void *data)
{
  CnfCtx *ctx = (CnfCtx *) data;
  watchLoop(ctx);
  return 0;
}

void TEST(watchInit)()
{
  bool rc;
  char errbuf[MAX_ERR_LEN];
  CnfCtx *ctx;

  int fd = open("./basic.log", O_WRONLY);
  assert(fd != -1);
  write(fd, "12\n456", 6);

  ctx = loadCnf(".", errbuf);
  rc = watchInit(ctx, errbuf);
  check(rc == true, "%s", errbuf);

  /* for test */
  ctx->pollLimit = 0;

  LuaCtx *lctx;
  for (LuaCtxPtrList::iterator ite = ctx->luaCtxs.begin(); ite != ctx->luaCtxs.end(); ++ite) {
    if ((*ite)->topic == "basic") {
      lctx = *ite;
      off_t cur = lseek(lctx->fd, 0, SEEK_CUR);
      check(lctx->size == cur, "%d = %d", (int) lctx->size, (int) cur);
      check(lctx->size == 6, "%d", (int) lctx->size);
      check(lctx->npos == 3, "%d", (int) lctx->npos);
      check(std::string(lctx->buffer, lctx->npos) == "456", "%.*s", (int) lctx->npos, lctx->buffer);
      break;
    }
  }

  pthread_t tid;
  pthread_create(&tid, NULL, TEST(watchLoop), ctx);

  write(fd, "\n789\n", 5);
  close(fd);
  // unlink("./basic.log");
  rename("./basic.log", "./basic.log.old");

  OneTaskReq req;
  read(ctx->accept, &req, sizeof(OneTaskReq));

  /* test wath */
  check(req.idx == lctx->idx, "%d = %d", req.idx, lctx->idx);
  check(*req.data == "456\n", "%s", req.data->c_str());

  read(ctx->accept, &req, sizeof(OneTaskReq));
  check(*req.data == "789\n", "%s", req.data->c_str());

  check(lctx->size == 11, "%d", (int) lctx->size);

  /* check rmwatch */
  sleep(1);
  for (WatchCtxHash::iterator ite = ctx->wch.begin(); ite != ctx->wch.end(); ++ite) {
    check(ite->second != lctx, "%s should be remove from inotify", lctx->file.c_str());
  }

  /* enable raw copy */
  ctx->pollLimit = 300;
  
  /* test rewatch */
  fd = open("./basic.log", O_CREAT | O_WRONLY, 0644);
  assert(fd != -1);
  write(fd, "abcd\nefg\n", sizeof("abcd\nefg\n")-1);
  close(fd);

  read(ctx->accept, &req, sizeof(OneTaskReq));
  check(*req.data == "abcd\nefg\n", "%s", req.data->c_str());

  want = STOP;
  pthread_join(tid, 0);
}

void TEST(initKafka)()
{
  bool rc;
  char errbuf[MAX_ERR_LEN];
  CnfCtx *ctx;

  ctx = loadCnf(".", errbuf);
  rc = initKafka(ctx, errbuf);
  check(rc == true, "%s", errbuf);

  unloadCnfCtx(ctx);
}  

static const char *files[] = {
  "./basic.log",
  "./filter.log",
  "./aggregate.log",
  "./grep.log",
  "./transform.log",
  0
};

void TEST(prepare)()
{
  for (int i = 0; files[i]; ++i) {
    int fd = creat(files[i], 0644);
    if (fd != -1) close(fd);
  }
}

void TEST(clean)()
{
  for (int i = 0; files[i]; ++i) {
    unlink(files[i]);
  }
  unlink("./basic.log.old");
}
  
int main(int argc, char *argv[])
{
  TEST(prepare)();
    
  TEST(split)();
  TEST(iso8601)();
  
  TEST(loadLuaCtx)();
  TEST(loadCnf)();
  
  TEST(transform)();
  TEST(aggregate)();
  TEST(grep)();
  TEST(filter)();

  TEST(watchInit)();
  TEST(initKafka)();

  TEST(clean)();
  printf("OK\n");
  return 0;
}
#endif
