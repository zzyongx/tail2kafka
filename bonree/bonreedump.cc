#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>
#include <string>
#include <algorithm>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <curl/curl.h>
#include <jsoncpp/json/json.h>

bool getData(const char *url, std::string *data);
bool writeFsData(const char *file, const std::string &data);
bool getFsData(const char *file, std::string *data);
bool writeEsData(const char *es, const std::string &data);

int main(int argc, char *argv[])
{
  if (argc < 3) {
    fprintf(stderr, "%s name url es\n", argv[0]);
    fprintf(stderr, "%s name es\n", argv[0]);
    return EXIT_FAILURE;
  }

  const char *file = argv[1];
  const char *es;
  
  std::string data;
  if (argc == 4) {
    const char *url = argv[2];
    es = argv[3];
    if (!getData(url, &data)) return EXIT_FAILURE;
    writeFsData(file, data);
  } else {
    es = argv[2];
    if (!getFsData(file, &data)) return EXIT_FAILURE;
  }
  
  if (!writeEsData(es, data)) return EXIT_FAILURE;
  return EXIT_SUCCESS;
}

size_t curlGetData(void *ptr, size_t size, size_t nmemb, void *data)
{
  if (data) {
    std::string *str = (std::string *) data;
    str->append((char *) ptr, size * nmemb);
  }
  return size * nmemb;
}

bool getData(const char *url, std::string *data)
{
  CURL *curl = curl_easy_init();
  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlGetData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, data);

  long code;
  CURLcode rc = curl_easy_perform(curl);
  if (rc != CURLE_OK) {
    fprintf(stderr, "%s %s\n", url, curl_easy_strerror(rc));
    code = 0;
  } else {
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    if (code != 200) {
      fprintf(stderr, "%s return %ld", url, code);
    }
  }
  
  curl_easy_cleanup(curl);
  return code == 200;
}

bool writeFsData(const char *file, const std::string &data)
{
  if (data.empty() || data[0] != '[') {
    fprintf(stderr, "no json returned %s\n", data.c_str());
    return false;
  }
  
  FILE *fp = fopen(file, "w");
  if (!fp) {
    fprintf(stderr, "%s open error\n", file);
    return false;
  }
  size_t n = fwrite(data.c_str(), 1, data.size(), fp);
  fclose(fp);
  
  if (data.size() != n) {
    fprintf(stderr, "%s write error\n", file);
    return false;
  }
  return true;
}

bool getFsData(const char *file, std::string *data)
{
  FILE *fp = fopen(file, "r");
  if (!fp) {
    fprintf(stderr, "%s open error\n", file);
    return false;
  }
  
  struct stat st;
  fstat(fileno(fp), &st);
  data->resize(st.st_size);

  size_t n = fread((void *) data->data(), 1, data->size(), fp);
  fclose(fp);
  
  if (data->size() != n) {
    fprintf(stderr, "%s read error\n", file);
    return false;
  }
  return true;
}

struct CurlPostDataCtx {
  const std::string *data;
  size_t             pos;
  CurlPostDataCtx(const std::string *d) : data(d), pos(0) {}
};

size_t curlPostData(void *ptr, size_t size, size_t nmemb, void *data)
{
  CurlPostDataCtx *ctx = (CurlPostDataCtx *) data;
  size_t min = std::min(size * nmemb, ctx->data->size() - ctx->pos);
  memcpy(ptr, ctx->data->data() + ctx->pos, min);
  ctx->pos += min;
  return min;
}

bool writeEsDataImpl(const char *url, const std::string &data)
{
  CurlPostDataCtx ctx(&data);
    
  CURL *curl = curl_easy_init();
  curl_easy_setopt(curl, CURLOPT_URL, url);
  curl_easy_setopt(curl, CURLOPT_POST, 1L);
  curl_easy_setopt(curl, CURLOPT_READFUNCTION, curlPostData);
  curl_easy_setopt(curl, CURLOPT_READDATA, &ctx);
  curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, data.size());

  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlGetData);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, 0);
  // curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
    
  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Expect:");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  long code;
  CURLcode rc = curl_easy_perform(curl);
  if (rc != CURLE_OK) {
    fprintf(stderr, "%s %s\n", url, curl_easy_strerror(rc));
    code = 0;
  } else {
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    if (code != 201) {
      fprintf(stderr, "%s return %ld\n", url, code);
    }
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);
  return code == 201;
}

bool writeEsData(const char *url, const std::string &data)
{
  Json::Value root;
  Json::Reader reader;

  bool rc = reader.parse(data, root);
  if (!rc) {
    fprintf(stderr, "invalid json %s\n", data.c_str());
    return false;
  }

  for (Json::ValueIterator ite = root.begin(); ite != root.end(); ++ite) {
    /*
    Json::Value &time = (*ite)["MONITOR_TIME"];
    if (!time.isString()) continue;
    std::string old = time.asString();
    std::replace(old.begin(), old.end(), ' ', 'T');
    time = old;
    */
    
    std::string e = Json::FastWriter().write(*ite);
    while (!writeEsDataImpl(url, e)) sleep(5);
  }
  
  return true;
}
