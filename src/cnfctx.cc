#include <memory>

#include "sys.h"
#include "luahelper.h"
#include "luactx.h"
#include "cnfctx.h"

CnfCtx *CnfCtx::loadCnf(const char *dir, char *errbuf)
{
  std::vector<std::string> luaFiles;
  if (!sys::readdir(dir, ".lua", &luaFiles, errbuf)) return 0;

  std::string mainlua = std::string(dir) + "/main.lua";
  CnfCtx *cnf = CnfCtx::loadFile(mainlua.c_str(), errbuf);
  if (!cnf) return 0;

  for (std::vector<std::string>::iterator ite = luaFiles.begin(); ite != luaFiles.end(); ++ite) {
    if (sys::endsWith(ite->c_str(), "/main.lua")) continue;

    LuaCtx *ctx = LuaCtx::loadFile(cnf, ite->c_str());
    if (!ctx) return 0;

    cnf->addLuaCtx(ctx);
  }

  return cnf;
}

CnfCtx *CnfCtx::loadFile(const char *file, char *errbuf)
{
  std::auto_ptr<CnfCtx> cnf(new CnfCtx);

  std::auto_ptr<LuaHelper> helper(new LuaHelper);
  if (!helper->dofile(file, errbuf)) return 0;

  std::string hostshell;
  if (!helper->getString("hostshell", &hostshell)) return 0;
  if (!shell(hostshell.c_str(), &cnf->host_, errbuf)) return 0;
  if (!hostAddr(cnf->host_, &cnf->addr_, errbuf)) return 0;
  if (cnf->host_.size() >= 1024) {
    snprintf(errbuf, MAX_ERR_LEN, "hostname %s is too long", cnf->host_.c_str());
    return 0;
  }

  if (!helper->getString("pidfile", &cnf->pidfile_)) return 0;
  if (!helper->getString("brokers", &cnf->brokers_)) return 0;

  if (!helper->getInt("partition", &cnf->partition_, -1)) return 0;
  if (!helper->getInt("polllimit", &cnf->pollLimit_, 100)) return 0;
  if (!helper->getInt("rotatedelay", &cnf->rotateDelay_, -1)) return 0;

  if (!helper->getString("pingbackurl", &cnf->pingbackUrl_, "")) return 0;

  if (!helper->getTable("kafka_global", &cnf->kafkaGlobal_)) return 0;
  if (!helper->getTable("kafka_topic", &cnf->kafkaTopic_)) return 0;

  if (!helper->getString("libdir", &cnf->libdir_, "/var/lib/tail2kafka")) return 0;
  if (!sys::isdir(cnf->libdir_.c_str(), errbuf)) return 0;

  if (!helper->getString("logdir", &cnf->logdir_, "/var/log/tail2kafka")) return 0;
  if (!sys::isdir(cnf->logdir_.c_str(), errbuf)) return 0;

  cnf->helper_ = helper.release();

  int fd[2];
  if (pipe(fd) == -1) {
    snprintf(errbuf, MAX_ERR_LEN, "pipe error");
    return 0;
  }

  cnf->accept = fd[0];
  cnf->server = fd[1];

  cnf->errbuf_ = errbuf;
  return cnf.release();
}

void CnfCtx::addLuaCtx(LuaCtx *ctx)
{
  count_++;
  bool find = false;
  for (std::vector<LuaCtx *>::iterator ite = luaCtxs_.begin(); ite != luaCtxs_.end(); ++ite) {
    if ((*ite)->file() == ctx->file()) {
      ctx->setNext(*ite);
      *ite = ctx;
      find = true;
      break;
    }
  }
  if (!find) luaCtxs_.push_back(ctx);
}

bool CnfCtx::initKafka()
{
  std::auto_ptr<KafkaCtx> kafka(new KafkaCtx());
  if (!kafka->init(this, errbuf_)) return false;
  kafka_ = kafka.release();
  return true;
}

bool CnfCtx::initFileOff()
{
  std::auto_ptr<FileOff> fileOff(new FileOff);
  if (!fileOff->init(this, errbuf_)) return false;
  fileOff_ = fileOff.release();
  return true;
}

bool CnfCtx::initFileReader()
{
  for (std::vector<LuaCtx *>::iterator ite = luaCtxs_.begin(); ite != luaCtxs_.end(); ++ite) {
    LuaCtx *ctx = *ite;
    while (ctx) {
      if (!ctx->initFileReader(errbuf_)) return false;
      ctx = ctx->next();
    }
  }
  return true;
}

CnfCtx::CnfCtx() {
  partition_ = -1;

  helper_  = 0;
  kafka_   = 0;
  fileOff_ = 0;

  accept = -1;
  server = -1;
  count_  = 0;
  gettimeofday(&timeval_, 0);
}

CnfCtx::~CnfCtx()
{
  for (std::vector<LuaCtx *>::iterator ite = luaCtxs_.begin(); ite != luaCtxs_.end(); ++ite) {
    LuaCtx *ctx = *ite;
    while (ctx) {
      LuaCtx *next = ctx->next();
      delete ctx;
      ctx = next;
    }
  }

  if (helper_)  delete helper_;
  if (kafka_)   delete kafka_;
  if (fileOff_) delete fileOff_;

  if (accept != -1) close(accept);
  if (server != -1) close(server);
}
