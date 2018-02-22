#include <cstdio>
#include <cstring>
#include <memory>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>

#include "util.h"
#include "sys.h"
#include "common.h"
#include "logger.h"
#include "filereader.h"
#include "luahelper.h"
#include "luactx.h"

static bool loadHistoryFile(const std::string &libdir, const std::string &name, std::deque<std::string> *q)
{
  char buffer[2048];
  snprintf(buffer, 2048, "%s/%s.history", libdir.c_str(), name.c_str());

  FILE *fp = fopen(buffer, "r");
  if (!fp) {
    if (errno == ENOENT) return true;
    else return false;
  }

  while (fgets(buffer, 2048, fp)) {
    size_t n = strlen(buffer);
    if (buffer[n-1] != '\n') {
      fclose(fp);
      return false;
    }

    buffer[n-1] = '\0';
    q->push_back(buffer);
  }

  fclose(fp);
  return true;
}

 bool writeHistoryFile(const std::string &libdir, const std::string &name, const std::deque<std::string> &q)
{
  char path[2048];
  snprintf(path, 2048, "%s/%s.history", libdir.c_str(), name.c_str());

  if (q.empty()) {
    if (unlink(path) == -1) {
      log_fatal(errno, "delete history file %s error", path);
      return false;
    } else {
      log_info(0, "delete history file %s", path);
      return true;
    }
  }

  std::string flist = util::join(q.begin(), q.end(), ',');
  log_info(0, "write history file %s start %s", path, flist.c_str());

  int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd == -1) {
    log_fatal(errno, "writeHistoryFile %s error", path);
    return false;
  }
  for (std::deque<std::string>::const_iterator ite = q.begin(); ite != q.end(); ++ite) {
    struct iovec iovs[2] = {{(void *) ite->c_str(), ite->size()}, {(void *) "\n", 1}};
    if (writev(fd, iovs, 2) == -1) {
      log_fatal(errno, "writeHistoryFile %s line %s error", path, ite->c_str());
    }
  }
  close(fd);

  log_info(0, "wirte history file %s end %s", path, flist.c_str());
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
      close(fd);
    } else {
      snprintf(errbuf, MAX_ERR_LEN, "%s file %s stat failed", luaFile, filename.c_str());
      return false;
    }
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
  if (!helper->getBool("fileWithTimeFormat", &ctx->fileWithTimeFormat_, false)) return 0;

  if (!helper->getString("topic", &ctx->topic_)) return 0;
  if (!helper->getString("file", &ctx->file_)) return 0;
  if (!ctx->testFile(file, cnf->errbuf())) return 0;

  if (!helper->getString("fileAlias", &ctx->fileAlias_, ctx->topic_)) return false;
  for (std::vector<LuaCtx *>::iterator ite = cnf->getLuaCtxs().begin(); ite != cnf->getLuaCtxs().end(); ++ite) {
    for (LuaCtx *existCtx = *ite; existCtx; existCtx = existCtx->next_) {
      if (ctx->fileAlias_ == existCtx->fileAlias_) {
        snprintf(cnf->errbuf(), MAX_ERR_LEN, "%s and %s has the same fileAlias %s",
                 file, existCtx->helper_->file(), ctx->fileAlias_.c_str());
        return 0;
      }
    }
  }

  if (!helper->getString("startpos", &ctx->startPosition_, "LOG_START")) return 0;
  if (FileReader::stringToStartPosition(ctx->startPosition_.c_str()) == FileReader::NIL) {
    snprintf(cnf->errbuf(), MAX_ERR_LEN, "%s unknow start position %s", file, ctx->startPosition_.c_str());
    return false;
  }

  if (!helper->getInt("partition", &ctx->partition_, -1)) return 0;
  if (!helper->getBool("autoparti", &ctx->autoparti_, false)) return 0;
  if (ctx->autoparti_) {
    if (!hostAddr(cnf->host(), &ctx->addr_, cnf->errbuf())) return 0;
  }

  if (!helper->getBool("md5sum", &ctx->md5sum_, true)) return 0;
  if (!helper->getBool("rawcopy", &ctx->rawcopy_, false)) return 0;
  if (!helper->getInt("timeidx", &ctx->timeidx_, -1)) return 0;
  if (!helper->getBool("withtime", &ctx->withtime_, true)) return 0;
  if (!helper->getBool("autonl", &ctx->autonl_, true)) return 0;
  if (!helper->getBool("withhost", &ctx->withhost_, true)) return 0;
  if (!helper->getInt("rotateDelay_", &ctx->rotateDelay_, -1)) return 0;
  if (!helper->getString("pkey", &ctx->pkey_, "")) return 0;

  if (!(ctx->function_ = LuaFunction::create(ctx.get(), helper.get()))) return 0;

  if (!loadHistoryFile(cnf->libdir(), ctx->fileAlias_, &ctx->fqueue_)) {
    snprintf(cnf->errbuf(), MAX_ERR_LEN, "load history file %s/%s.history eror %d:%s",
             cnf->libdir().c_str(), ctx->fileAlias_.c_str(), errno, strerror(errno));
    return 0;
  }

  ctx->helper_ = helper.release();
  return ctx.release();
}

bool LuaCtx::addHistoryFile(const std::string &historyFile)
{
  if (fqueue_.empty() || fqueue_.back() != historyFile) {
    fqueue_.push_back(historyFile);
    writeHistoryFile(cnf_->libdir(), fileAlias_, fqueue_);
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

bool LuaCtx::initFileReader(char *errbuf)
{
  std::auto_ptr<FileReader> fileReader(new FileReader(this));
  if (!fileReader->init(errbuf)) return false;
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
