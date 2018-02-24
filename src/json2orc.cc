#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <memory>
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <getopt.h>
#include <json/json.h>
#include <orc/OrcFile.hh>
#include <orc/ColumnPrinter.hh>

#ifdef _DEBUG_
# define debug(fmt, args...) fprintf(stderr, fmt, ##args);
# define printColumnVectorBatch(type, batch, rows) printColumnVectorBatchImpl(type, batch, rows)
#else
# define debug(fmt, args...)
# define printColumnVectorBatch(type, batch, rows)
#endif

struct Option {
  uint64_t help;
  uint64_t stripe;
  uint64_t block;
  uint64_t batch;
  orc::CompressionKind compression;

  std::string schema;
  std::string input;
  std::string output;

  static Option *parse(int argc, char *argv[], char *errbuf);
};

Option *Option::parse(int argc, char *argv[], char *errbuf)
{
  std::auto_ptr<Option> option(new Option);
  option->help        = false;
  option->stripe      = (128 << 20);  // 128M
  option->block       =  64 << 10;    // 64K
  option->batch       = 1024;
  option->compression = orc::CompressionKind_ZLIB;

  struct option longOptions[] = {
    {"help", no_argument, 0, 'h'},
    {"stripe", required_argument, 0, 's'},
    {"block",  required_argument, 0, 'b'},
    {"batch",  required_argument, 0, 'B'},
    {"compress", required_argument, 0, 'c'},
    {0, 0, 0, 0}
  };

  const char *error = 0;
  char *tail;
  int opt;
  do {
    opt = getopt_long(argc, argv, "s:b:B:c:h", longOptions, 0);
    switch (opt) {
    case '?':
      error = "";
    case 'h':
      option->help = true;
      break;
    case 's':
      option->stripe = strtoul(optarg, &tail, 10);
      if (*tail != '\0') error = "the --stripe parameter requires an integer option.";
      break;
    case 'b':
      option->block = strtoul(optarg, &tail, 10);
      if (*tail != '\0') error = "the --block parameter requires an integer option.";
      break;
    case 'B':
      option->batch = strtoul(optarg, &tail, 10);
      if (*tail != '\0') error = "the --batch parameter requires an integer option.";
      break;
    case 'c':
      if (strcmp(optarg, "ZLIB") == 0) option->compression = orc::CompressionKind_ZLIB;
      else if (strcmp(optarg, "SNAPPY") == 0) option->compression = orc::CompressionKind_SNAPPY;
      else error = "unknow --compress parameter, use ZLIB|SNAPPY";
      break;
    }
  } while(opt != -1 && !error);

  argc -= optind;
  argv += optind;

  if (argc != 3) {
    if (!option->help) error = "require schema input output";
  } else {
    option->schema = argv[0];
    option->input  = argv[1];
    option->output = argv[2];
  }

  if (error) {
    if (errbuf) strcpy(errbuf, error);
    return 0;
  } else {
    return option.release();
  }
}

static void usage(const char *error = 0)
{
  FILE *output;
  if (error) {
    if (error[0] != '\0') fprintf(stderr, "%s\n\n", error);
    output = stderr;
  } else {
    output = stdout;
  }

  fprintf(output,
          "Usage: json2orc [-h] [--help]\n"
          "                [-s <size>] [--stripe=<size>]\n"
          "                [-b <size>] [--block=<size>]\n"
          "                [-B <size>] [--batch=<size>]\n"
          "                [-c <ZLIB|SNAPPY>] [--compress <ZLIB|SNAPPY>]\n"
          "                <schema> <input> <output>\n");
}

class JsonToOrc {
public:
  JsonToOrc(Option *option);
  bool read(const char *buffer, size_t n);
  ~JsonToOrc();

private:
  Option *option_;
  ORC_UNIQUE_PTR<orc::Type> fileType_;
  ORC_UNIQUE_PTR<orc::OutputStream> outStream_;
  ORC_UNIQUE_PTR<orc::Writer> writer_;
  ORC_UNIQUE_PTR<orc::StructVectorBatch> structBatch_;

  orc::DataBuffer<char> buffer_;
  uint64_t              bufferOffset_;

  size_t row_;
};

JsonToOrc::JsonToOrc(Option *option)
  : buffer_(*orc::getDefaultPool(), 4 * 1024 * 1024), bufferOffset_(0)
{
  std::string schema;
  for (size_t i = 0; i < option->schema.size(); ++i) {
    if (option->schema[i] == '_') schema.append(1, 'X');
    else if (option->schema[i] == 'X') throw std::logic_error("field name can't contain 'X'");
    else schema.append(1, option->schema[i]);
  }

  fileType_ = orc::Type::buildTypeFromString(schema.c_str());  // _ is invalid in this version

  orc::WriterOptions options;
  options.setStripeSize(option->stripe);
  options.setCompressionBlockSize(option->block);
  options.setCompression(option->compression);

  outStream_ = orc::writeLocalFile(option->output.c_str());
  writer_ = orc::createWriter(*fileType_, outStream_.get(), options);
  structBatch_.reset(dynamic_cast<orc::StructVectorBatch*>(writer_->createRowBatch(option->batch).release()));

  row_ = 0;
  option_ = option;
}

bool addFieldRoute(const Json::Value &value, const orc::Type *type, orc::ColumnVectorBatch *batch, int row,
                   orc::DataBuffer<char> *buffer, uint64_t *offset);

template <class Type>
void extendDataBuffer(orc::DataBuffer<Type> *dataBuffer, uint64_t nsize)
{
  if (nsize > dataBuffer->size()) {
    debug("databuffer #%p now %d, need %d\n", dataBuffer, (int) dataBuffer->size(), (int) nsize);
    if (nsize > dataBuffer->size() * 2) abort();
    dataBuffer->resize(dataBuffer->size() * 2);
  }
}

inline bool addField(long val, orc::LongVectorBatch *batch, size_t row)
{
  debug("row %d %d\n", (int) row, (int) batch->data.size());
  extendDataBuffer(&batch->data, row+1);
  batch->data[row] = val;
  return true;
}

inline bool addField(long val, orc::DoubleVectorBatch *batch, size_t row)
{
  extendDataBuffer(&batch->data, row+1);
  batch->data[row] = val;
  return true;
}

inline bool addField(const Json::Value &value, orc::TimestampVectorBatch *batch, size_t row)
{
  extendDataBuffer(&batch->data, row+1);
  extendDataBuffer(&batch->nanoseconds, row+1);

  bool rc = true;
  int64_t seconds;
  if (value.isNumeric()) seconds = value.asInt64();
  else if (value.isString()) {
    struct tm tm;
    const std::string &s = value.asString();
    if (strptime(s.c_str(), "%Y-%m-%d %H:%M:%S", &tm) == s.c_str() + s.size() ||
        strptime(s.c_str(), "%Y-%m-%dT%H:%M:%S", &tm) == s.c_str() + s.size()) {
      seconds = mktime(&tm);
    } else {
      rc = false;
    }
  }

  if (rc) {
    batch->data[row] = seconds;
    batch->nanoseconds[row] = 0;
  }

  return rc;
}

inline bool addField(const std::string &value, orc::StringVectorBatch *batch, size_t row,
                     orc::DataBuffer<char> *buffer, uint64_t *offset)
{
  extendDataBuffer(buffer, *offset + value.size());
  memcpy(buffer->data() + *offset, value.c_str(), value.size());

  extendDataBuffer(&batch->data, row+1);
  extendDataBuffer(&batch->length, row+1);

  batch->data[row] = buffer->data() + *offset;
  batch->length[row] = value.size();

  *offset += value.size();
  return true;
}

void printColumnVectorBatchImpl(const orc::Type *type, const orc::ColumnVectorBatch *batch, size_t rows)
{
  std::string line;

  ORC_UNIQUE_PTR<orc::ColumnPrinter> printer = orc::createColumnPrinter(line, type);
  printer->reset(*batch);
  for (size_t i = 0; i < rows; ++i) {
    line.clear();
    printer->printRow(i);
    fprintf(stderr, "%d %s\n", (int) i, line.c_str());
  }
}

inline bool addField(const Json::Value &array, const orc::Type *type, orc::ListVectorBatch *batch, size_t row,
                     orc::DataBuffer<char> *buffer, uint64_t *bufferOffset)
{
  size_t size = array.size();
  const orc::Type *valueType = type->getSubtype(0);

  assert(batch->elements.get());

  extendDataBuffer(&batch->offsets, row+2);
  if (row == 0) {
    batch->elements->numElements = 0;
    batch->elements->hasNulls = false;
  }

  int64_t offset = batch->elements->numElements;

  bool rc = true;
  int irow = 0;
  for (Json::Value::const_iterator ite = array.begin(); rc && ite != array.end(); ++ite, ++irow) {
    if (ite->isNull()) {
      batch->elements->hasNulls = true;
      batch->elements->notNull[offset + irow] = 0;
    } else {
      batch->elements->notNull[offset + irow] = 1;
      rc = addFieldRoute(*ite, valueType, batch->elements.get(), offset + irow, buffer, bufferOffset);
    }
  }

  if (rc) {
    batch->elements->numElements += size;
    batch->offsets[row] = offset;
    batch->offsets[row+1] = offset + size;

    debug("%d %d\n", (int) batch->offsets[row], (int) batch->elements->numElements);
    printColumnVectorBatch(type, batch, row+1);
  }
  return rc;
}


inline bool addField(const Json::Value &obj, const orc::Type *type, orc::MapVectorBatch *batch, size_t row,
                     orc::DataBuffer<char> *buffer, uint64_t *bufferOffset)
{
  size_t size = 0;
  for (Json::Value::const_iterator ite = obj.begin(); ite != obj.end(); ++ite) size++;
  const orc::Type *valueType = type->getSubtype(1);

  extendDataBuffer(&batch->offsets, row+2);
  if (row == 0) {
    batch->keys->numElements = 0;
    batch->keys->hasNulls = false;
    batch->elements->hasNulls = false;
  }

  int64_t offset = batch->keys->numElements;

  int irow = 0;
  bool rc = true;
  for (Json::Value::const_iterator ite = obj.begin(); ite != obj.end(); ++ite, ++irow) {
    if (ite->isNull()) {
      batch->keys->hasNulls = true;
      batch->keys->notNull[offset + irow] = 0;

      batch->elements->hasNulls = true;
      batch->elements->notNull[offset + irow] = 0;
    } else {
      batch->keys->notNull[offset + irow] = 1;
      batch->elements->notNull[offset + irow] = 1;

      rc = addField(ite.key().asString(), dynamic_cast<orc::StringVectorBatch*>(batch->keys.get()), offset + irow,
                    buffer, bufferOffset);
      if (rc) rc = addFieldRoute(*ite, valueType, batch->elements.get(), offset + irow, buffer, bufferOffset);
    }
  }

  if (rc) {
    batch->keys->numElements += size;
    batch->elements->numElements = batch->keys->numElements;
    batch->offsets[row] = offset;
    batch->offsets[row+1] = offset + size;
  }

  debug("%d %d\n", (int) batch->offsets[row], (int) batch->keys->numElements);
  printColumnVectorBatch(type, batch, row+1);
  return rc;
}

bool addField(const Json::Value &obj, const orc::Type *type, orc::StructVectorBatch *batch, int row,
              orc::DataBuffer<char> *buffer, uint64_t *bufferOffset)
{
  bool rc = true;
  for (uint64_t i = 0; rc && i < type->getSubtypeCount(); ++i) {
    if (rc == 0) batch->fields[i]->hasNulls = false;

    std::string name = type->getFieldName(i);
    for (size_t j = 0; j < name.size(); ++j) if (name[j] == 'X') name[j] = '_';

    const Json::Value &value = obj[name];

    if (value.isNull()) {
      batch->fields[i]->hasNulls = true;
      batch->fields[i]->notNull[row] = 0;
    } else {
      batch->fields[i]->notNull[row] = 1;
      rc = addFieldRoute(value, type->getSubtype(i), batch->fields[i], row, buffer, bufferOffset);
    }
  }

  if (rc) {
    for (uint64_t i = 0; i < type->getSubtypeCount(); ++i) batch->fields[i]->numElements = row+1;
    batch->numElements = row+1;
    return true;
  } else {
    return false;
  }
}

bool addFieldRoute(const Json::Value &value, const orc::Type *type, orc::ColumnVectorBatch *batch, int row,
                   orc::DataBuffer<char> *buffer, uint64_t *offset)
{
  switch(type->getKind()) {
  case orc::BYTE:
  case orc::INT:
  case orc::SHORT:
  case orc::LONG:
    return addField(value.asInt64(), dynamic_cast<orc::LongVectorBatch*>(batch), row);

  case orc::STRING:
  case orc::CHAR:
  case orc::VARCHAR:
  case orc::BINARY:
    return addField(value.asString(), dynamic_cast<orc::StringVectorBatch*>(batch), row, buffer, offset);

  case orc::FLOAT:
  case orc::DOUBLE:
    return addField(value.asDouble(), dynamic_cast<orc::DoubleVectorBatch*>(batch), row);

  case orc::BOOLEAN:
    return addField(value.asInt(), dynamic_cast<orc::LongVectorBatch*>(batch), row);

  case orc::TIMESTAMP:

    return addField(value, dynamic_cast<orc::TimestampVectorBatch*>(batch), row);

  case orc::MAP:
    return value.isObject() &&
      addField(value, type, dynamic_cast<orc::MapVectorBatch*>(batch), row, buffer, offset);

  case orc::LIST:
    return value.isArray() &&
      addField(value, type, dynamic_cast<orc::ListVectorBatch*>(batch), row, buffer, offset);

  case orc::STRUCT:
    return value.isObject() &&
      addField(value, type, dynamic_cast<orc::StructVectorBatch*>(batch), row, buffer, offset);

  default:
    throw std::logic_error("unsupport orc type");
  }
}

bool JsonToOrc::read(const char *buffer, size_t n)
{
  debug("json %.*s\n", (int) n, buffer);

  Json::Value root;
  Json::Reader reader;
  bool rc = reader.parse(buffer, buffer + n, root);
  if (!rc) return rc;

  if ((rc = addField(root, fileType_.get(), structBatch_.get(), row_, &buffer_, &bufferOffset_))) {
    row_++;

    if (row_ == option_->batch) {
      debug("dump struct before write\n");
      printColumnVectorBatch(fileType_.get(), structBatch_.get(), row_);

      writer_->add(*structBatch_);
      bufferOffset_ = 0;
      row_ = 0;
    }
  }

  return rc;
}

JsonToOrc::~JsonToOrc()
{
  if (row_ != 0) {
    debug("dump struct before write\n");
    printColumnVectorBatch(fileType_.get(), structBatch_.get(), row_);

    writer_->add(*structBatch_);
  }
  writer_->close();
}

int main(int argc, char *argv[])
{
  char errbuf[1024];
  Option *option = Option::parse(argc, argv, errbuf);
  if (!option) {
    usage(errbuf);
    return EXIT_FAILURE;
  } else if (option->help) {
    usage();
    return EXIT_SUCCESS;
  }

  FILE *in;
  if (option->input == "-") {
    in = stdin;
  } else {
    in = fopen(option->input.c_str(), "r");
    if (!in) {
      fprintf(stderr, "open %s error %s\n", option->input.c_str(), strerror(errno));
      return EXIT_FAILURE;
    }
  }

  const size_t BUFFER_SIZE = 1024 * 1024 * 10;
  std::auto_ptr<char> buffer(new char[1024 * 1024 * 10]);

  try {
    JsonToOrc jsonToOrc(option);
    while (fgets(buffer.get(), BUFFER_SIZE, in)) {
      size_t size = strlen(buffer.get());
      if (buffer.get()[size-1] != '\n') {
        fprintf(stderr, "line %s too long\n", buffer.get());
        return EXIT_FAILURE;
      }
      buffer.get()[size-1] = '\0';
      if (!jsonToOrc.read(buffer.get(), size-1)) {
        fprintf(stderr, "invalid json %s\n", buffer.get());
        return EXIT_FAILURE;
      }
    }
  } catch (const std::exception &e) {
    fprintf(stderr, "exception %s, use schema %s\n", e.what(), option->schema.c_str());
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
