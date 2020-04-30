#include <cstdio>
#include <cstring>
#include <memory>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>

#include "util.h"
#include "sys.h"
#include "common.h"
#include "logger.h"
#include "filereader.h"
#include "luahelper.h"
#include "luactx.h"

static bool loadHistoryFile(const std::string &libdir, const std::string &name, std::deque<std::string> *q)
{
  std::string file = libdir + "/" + name + ".history";

  FILE *fp = fopen(file.c_str(), "r");
  if (!fp) {
    if (errno == ENOENT) return true;
    else return false;
  }

  char buffer[2048];
  while (fgets(buffer, 2048, fp)) {
    size_t n = strlen(buffer);
    if (buffer[n-1] != '\n') {
      fclose(fp);
      return false;
    }

    buffer[n-1] = '\0';

    struct stat st;
    if (stat(buffer, &st) != 0) {
      log_info(0, "history file %s ignore %s", file.c_str(), buffer);
      continue;
    }

    if (std::find(q->begin(), q->end(), std::string(buffer)) == q->end()) {
      q->push_back(buffer);  // uniq
    }
  }

  fclose(fp);
  return true;
}

static bool writeHistoryFile(const std::string &libdir, const std::string &name, const std::deque<std::string> &q)
{
  std::string file = libdir + "/" + name + ".history";

  if (q.empty()) {
    bool rc = unlink(file.c_str()) == 0 || errno == ENOENT;
    if (rc) {
      log_info(0, "delete history file %s", file.c_str());
    } else {
      log_fatal(errno, "delete history file %s error", file.c_str());
    }
    return rc;
  }

  std::string flist = util::join(q.begin(), q.end(), ',');
  log_info(0, "write history file %s start %s", file.c_str(), flist.c_str());

  int fd = open(file.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd == -1) {
    log_fatal(errno, "write history file %s error", file.c_str());
    return false;
  }
  for (std::deque<std::string>::const_iterator ite = q.begin(); ite != q.end(); ++ite) {
    struct iovec iovs[2] = {{(void *) ite->c_str(), ite->size()}, {(void *) "\n", 1}};
    if (writev(fd, iovs, 2) == -1) {
      log_fatal(errno, "write history file %s line %s error", file.c_str(), ite->c_str());
    }
  }
  close(fd);

  log_info(0, "wirte history file %s end %s", file.c_str(), flist.c_str());
  return true;
}

static bool parseUserGroup(const char *username, uid_t *uid, gid_t *gid, char *errbuf)
{
  const char *colon = strchr(username, ':');
  std::string u, g;
  if (colon) {
    u.assign(username, colon - username);
    g.assign(colon+1);
  } else {
    u.assign(username);
    g.assign(u);
  }

  struct passwd *pwd = getpwnam(u.c_str());
  if (!pwd) {
    snprintf(errbuf, MAX_ERR_LEN, "getpwnam(%s) error, %s", u.c_str(), strerror(errno));
    return false;
  }

  struct group *grp = getgrnam(g.c_str());
  if (!grp) {
    snprintf(errbuf, MAX_ERR_LEN, "getgrnam(%s) error, %s", g.c_str(), strerror(errno));
    return false;
  }

  *uid = pwd->pw_uid;
  *gid = grp->gr_gid;
  return true;
}

bool LuaCtx::testFile(const char *luaFile, char *errbuf)
{
  std::string filename;
  if (fileWithTimeFormat_) {
    timeFormatFile_ = sys::timeFormat(cnf_->fasttime(), file_.c_str(), file_.size());
    filename = timeFormatFile_;
  } else {
    filename = file_;
  }

  if (filename.size() >= 1024) {
    snprintf(errbuf, MAX_ERR_LEN, "%s filename %s too long", luaFile, filename.c_str());
    return false;
  }

  struct stat st;
  if (::stat(filename.c_str(), &st) == -1) {
    if (errno == ENOENT && autocreat_) {
      int fd = creat(filename.c_str(), 0644);
      if (fd == -1) {
        snprintf(errbuf, MAX_ERR_LEN, "%s file %s autocreat failed", luaFile, filename.c_str());
        return false;
      }
      if (!fileOwner_.empty() && chown(filename.c_str(), uid_, gid_) != 0) {
        snprintf(errbuf, MAX_ERR_LEN, "%s file %s chown(%s) failed",
                 luaFile, filename.c_str(), fileOwner_.c_str());
        return false;
      }
      close(fd);
    } else {
      snprintf(errbuf, MAX_ERR_LEN, "%s file %s stat failed", luaFile, filename.c_str());
      return false;
    }
  }
  return true;
}

bool LuaCtx::loadHistoryFile()
{
  fqueue_.clear();
  if (!::loadHistoryFile(cnf_->libdir(), fileAlias_, &fqueue_)) {
    snprintf(cnf_->errbuf(), MAX_ERR_LEN, "load history file %s/%s.history error %d:%s",
             cnf_->libdir().c_str(), fileAlias_.c_str(), errno, strerror(errno));
    return false;
  }

  return true;
}

LuaCtx *LuaCtx::loadFile(CnfCtx *cnf, const char *file)
{
  std::auto_ptr<LuaCtx> ctx(new LuaCtx);
  ctx->cnf_ = cnf;

  std::auto_ptr<LuaHelper> helper(new LuaHelper);
  if (!helper->dofile(file, cnf->errbuf())) return 0;

  if (!helper->getBool("autocreat", &ctx->autocreat_, false)) return 0;
  if (!helper->getString("fileOwner", &ctx->fileOwner_, "")) return 0;
  if (ctx->fileOwner_.empty()) {
    ctx->uid_ = 0;
    ctx->gid_ = 0;
  } else {
    if (!parseUserGroup(ctx->fileOwner_.c_str(), &ctx->uid_, &ctx->gid_, cnf->errbuf())) return 0;
  }

  if (!helper->getBool("fileWithTimeFormat", &ctx->fileWithTimeFormat_, false)) return 0;

  if (!helper->getString("file", &ctx->file_)) return 0;
  if (!ctx->testFile(file, cnf->errbuf())) return 0;

  std::string esIndex, esDoc;

  if (!helper->getString("topic", &ctx->topic_, "")) return 0;
  if (!helper->getString("es_index", &esIndex, "")) return 0;
  if (!helper->getString("es_doc", &esDoc, "")) return 0;

  if (!esIndex.empty() && !esDoc.empty()) {
    if (!ctx->parseEsIndexDoc(esIndex, esDoc, cnf->errbuf())) return 0;
  } else if (!esIndex.empty() && esDoc.empty()) {
    snprintf(cnf->errbuf(), MAX_ERR_LEN, "%s es_index requires es_doc", file);
    return 0;
  } else if (!esDoc.empty() && esIndex.empty()) {
    snprintf(cnf->errbuf(), MAX_ERR_LEN, "%s es_doc requires es_index", file);
    return 0;
  }

  if (!helper->getString("fileAlias", &ctx->fileAlias_, ctx->topic_)) return 0;
  for (std::vector<LuaCtx *>::iterator ite = cnf->getLuaCtxs().begin(); ite != cnf->getLuaCtxs().end(); ++ite) {
    for (LuaCtx *existCtx = *ite; existCtx; existCtx = existCtx->next_) {
      if (!ctx->topic_.empty() && ctx->fileAlias_ == existCtx->fileAlias_) {
        snprintf(cnf->errbuf(), MAX_ERR_LEN, "%s and %s has the same fileAlias %s",
                 file, existCtx->helper_->file(), ctx->fileAlias_.c_str());
        return 0;
      }
    }
  }

  if (!helper->getString("startpos", &ctx->startPosition_, "LOG_START")) return 0;
  if (FileReader::stringToStartPosition(ctx->startPosition_.c_str()) == FileReader::NIL) {
    snprintf(cnf->errbuf(), MAX_ERR_LEN, "%s unknow start position %s", file, ctx->startPosition_.c_str());
    return 0;
  }

  if (!helper->getInt("partition", &ctx->partition_, -1)) return 0;
  if (!helper->getBool("autoparti", &ctx->autoparti_, false)) return 0;
  if (ctx->autoparti_) {
    if (!hostAddr(cnf->host(), &ctx->addr_, cnf->errbuf())) return 0;
  }

  if (!helper->getBool("md5sum", &ctx->md5sum_, false)) return 0;
  if (!helper->getBool("rawcopy", &ctx->rawcopy_, false)) return 0;
  if (!helper->getInt("timeidx", &ctx->timeidx_, -1)) return 0;
  if (!helper->getBool("withtime", &ctx->withtime_, true)) return 0;
  if (!helper->getBool("autonl", &ctx->autonl_, true)) return 0;
  if (!helper->getBool("withhost", &ctx->withhost_, true)) return 0;
  if (!helper->getString("pkey", &ctx->pkey_, "")) return 0;

  LuaFunction::Type luafType;
  if (!ctx->topic_.empty()) luafType = LuaFunction::KAFKAPLAIN;
  else if (!esIndex.empty()) luafType = LuaFunction::ESPLAIN;
  else luafType = LuaFunction::NIL;

  if (!(ctx->function_ = LuaFunction::create(ctx.get(), helper.get(), luafType))) return 0;
  if (!ctx->loadHistoryFile()) return 0;

  // es
  if (ctx->topic_.empty()) ctx->partition_ = PARTITIONER_RANDOM;

  ctx->helper_ = helper.release();
  return ctx.release();
}

bool LuaCtx::parseEsIndexDoc(const std::string &esIndex, const std::string &esDoc, char errbuf[])
{
  if (esIndex.size() >= 56) {
    snprintf(errbuf, MAX_ERR_LEN, "len(%s) must <= 56", esIndex.c_str());
    return false;
  }

  size_t pos;
  esIndex_ = esIndex;

  esIndexPos_ = -1;
  if (esIndex[0] == '#') {
    esIndexPos_ = 0;
    for (pos = 1; pos < esIndex.size(); ++pos) {
      if (esIndex[pos] >= '0' && esIndex[pos] <= '9') {
        esIndexPos_ = esIndexPos_ * 10 + esIndex[pos] - '0';
      } else {
        break;
      }
    }
    if (pos < esIndex.size()) esIndex_ = esIndex.substr(pos);
    else esIndex_.clear();
  }

  esIndexWithTimeFormat_ = esIndex_.find('%') != std::string::npos;

  if (esDoc[0] != '#') {
    snprintf(errbuf, MAX_ERR_LEN, "esDoc requires format ##/NGINX_JSON or ##/NGINX_LOG");
    return false;
  }

  esDocPos_ = 0;
  for (pos = 1; pos < esDoc.size(); ++pos) {
    if (esDoc[pos] >= '0' && esDoc[pos] <= '9') {
      esDocPos_ = esDocPos_ * 10 + esDoc[pos] - '0';
    } else {
      break;
    }
  }

  if (esIndexPos_ < 1 || esDocPos_ <= esIndexPos_) {
    snprintf(errbuf, MAX_ERR_LEN, "esIndexPos must > 0 and esDocPos must >= esIndexPos");
    return false;
  }

  std::string format = esDoc.substr(pos);
  if (format == "/NGINX_JSON") {
    esDocDataFormat_ = ESDOC_DATAFORMAT_NGINX_JSON;
  } else if (format == "/NGINX") {
    esDocDataFormat_ = ESDOC_DATAFORMAT_NGINX_LOG;
  } else {
    snprintf(errbuf, MAX_ERR_LEN, "esDoc requires format ##/NGINX_JSON or ##/NGINX_LOG");
    return false;
  }

  return true;
}

bool LuaCtx::addHistoryFile(const std::string &historyFile)
{
  if (fqueue_.empty() || fqueue_.back() != historyFile) {
    if (std::find(fqueue_.begin(), fqueue_.end(), historyFile) == fqueue_.end()) {
      fqueue_.push_back(historyFile);
      writeHistoryFile(cnf_->libdir(), fileAlias_, fqueue_);
    }
    return true;
  } else {
    return false;
  }
}

bool LuaCtx::removeHistoryFile()
{
  if (!fqueue_.empty()) {
    fqueue_.pop_front();
    writeHistoryFile(cnf_->libdir(), fileAlias_, fqueue_);
  }

  return fqueue_.empty();
}

bool LuaCtx::initFileReader(FileReader *reader, char *errbuf)
{
  std::auto_ptr<FileReader> fileReader(new FileReader(this));
  if (reader) {
    fileReader->init(reader);
  } else {
    if (!fileReader->init(errbuf)) return false;
  }
  fileReader_ = fileReader.release();
  return true;
}

LuaCtx::LuaCtx()
{
  helper_     = 0;
  function_   = 0;
  fileReader_ = 0;

  partition_ = -1;
  timeidx_  = -1;
  next_ = 0;
}

LuaCtx::~LuaCtx() {
  if (helper_) delete helper_;
  if (function_) delete function_;
  if (fileReader_) delete fileReader_;
}
