#include <memory>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "sys.h"
#include "common.h"
#include "filereader.h"
#include "luahelper.h"
#include "luactx.h"

bool LuaCtx::testFile(const char *luaFile, char *errbuf)
{
  std::string filename;
  if (fileWithTimeFormat_) {
    timeFormatFile_ = sys::timeFormat(cnf_->fasttime(), file_.c_str(), file_.size());
    filename = timeFormatFile_;
  } else {
    filename = file_;
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

  if (!helper->getBool("rawcopy", &ctx->rawcopy_, false)) return 0;
  if (!helper->getInt("timeidx", &ctx->timeidx_, -1)) return 0;
  if (!helper->getBool("withtime", &ctx->withtime_, true)) return 0;
  if (!helper->getBool("autonl", &ctx->autonl_, true)) return 0;
  if (!helper->getBool("withhost", &ctx->withhost_, true)) return 0;
  if (!helper->getInt("rotateDelay_", &ctx->rotateDelay_, -1)) return 0;
  if (!helper->getString("pkey", &ctx->pkey_, "")) return 0;

  if (!(ctx->function_ = LuaFunction::create(ctx.get(), helper.get()))) return 0;

  ctx->helper_ = helper.release();
  return ctx.release();
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
