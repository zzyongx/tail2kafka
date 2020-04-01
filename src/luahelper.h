#ifndef _LUAHELPER_H_
#define _LUAHELPER_H_

#include <string>
#include <map>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}

#include "common.h"

class LuaHelper {
public:
  LuaHelper() : L_(0) {}

  ~LuaHelper() {
    if (L_) lua_close(L_);
  }

  const char *file() const { return file_.c_str(); }

  bool dofile(const char *f, char *errbuf) {
    lua_State *L = luaL_newstate();
    luaL_openlibs(L);
    if (luaL_dofile(L, f) != 0) {
      snprintf(errbuf, MAX_ERR_LEN, "load %s error %s", f, lua_tostring(L, 1));
      lua_close(L);
      return false;
    }

    if (L_) lua_close(L_);
    L_ = L;

    file_   = f;
    errbuf_ = errbuf;
    return true;
  }

  std::string luaString(lua_State *L, int index) {
    size_t len;
    const char *ptr = lua_tolstring(L, index, &len);
    return std::string(ptr, len);
  }

  void luaString(lua_State *L, int index, std::string *r, bool append = false) {
    size_t len;
    const char *ptr = lua_tolstring(L, index, &len);

    if (append) r->append(ptr, len);
    else r->assign(ptr, len);
  }

  bool getString(const char *name, std::string *value) {
    bool rc = true;
    lua_getglobal(L_, name);
    if (lua_isstring(L_, 1)) {
      luaString(L_, 1, value);
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be string", file_.c_str(), name);
      rc = false;
    }
    lua_settop(L_, 0);
    return rc;
  }

  bool getString(const char *name, std::string *value, const std::string &def) {
    bool rc = true;
    lua_getglobal(L_, name);
    if (lua_isnil(L_, 1)) {
      value->assign(def);
    } else {
      if (lua_isstring(L_, 1)) {
        luaString(L_, 1, value);
      } else {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be string", file_.c_str(), name);
        rc = false;
      }
    }
    lua_settop(L_, 0);
    return rc;
  }

  bool getInt(const char *name, int *value, int def) {
    bool rc = true;
    lua_getglobal(L_, name);
    if (lua_isnil(L_, 1)) {
      *value = def;
    } else {
      if (lua_isnumber(L_, 1)) {
        *value = (int) lua_tonumber(L_, 1);
      } else {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be number", file_.c_str(), name);
        rc = false;
      }

    }
    lua_settop(L_, 0);
    return rc;
  }

  bool getBool(const char *name, bool *value, bool def) {
    bool rc = true;
    lua_getglobal(L_, name);
    if (lua_isnil(L_, 1)) {
      *value = def;
    } else {
      if (lua_isboolean(L_, 1)) {
        *value = lua_toboolean(L_, 1);
      } else {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be boolean", file_.c_str(), name);
        rc = false;
      }
    }
    lua_settop(L_, 0);
    return rc;
  }

  bool getTable(const char *name, std::map<std::string, std::string> *map, bool required = true) {
    bool rc = true;
    lua_getglobal(L_, name);

    if (lua_isnil(L_, 1)) {
      if (required) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be hash table", file_.c_str(), name);
        rc = false;
      }
    } else if (lua_istable(L_, 1)) {
      lua_pushnil(L_);
      while (rc && lua_next(L_, 1) != 0) {
        if (lua_type(L_, -2) != LUA_TSTRING) {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s key must be string", file_.c_str(), name);
          rc = false;
        } else if (lua_type(L_, -1) != LUA_TSTRING && lua_type(L_, -1) != LUA_TNUMBER) {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s value must be string", file_.c_str(), name);
          rc = false;
        } else {
          map->insert(std::make_pair(luaString(L_, -2), luaString(L_, -1)));
        }
        lua_pop(L_, 1);
      }
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be hash table", file_.c_str(), name);
      rc = false;
    }

    lua_settop(L_, 0);
    return rc;
  }

  bool getTable(const char *name, std::map<std::string, std::vector<std::string> > *map, bool required = true) {
    bool rc = true;
    lua_getglobal(L_, name);

    if (lua_isnil(L_, 1)) {
      if (required) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be hash table", file_.c_str(), name);
        rc = false;
      }
    } else if (lua_istable(L_, 1)) {
      lua_pushnil(L_);
      while (rc && lua_next(L_, 1) != 0) {
        std::string key;
        if (lua_type(L_, -2) == LUA_TSTRING) {
          luaString(L_, -2, &key);
        } else {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s key must be string", file_.c_str(), name);
          rc = false;
          break;
        }

        std::vector<std::string> value;
        if (lua_type(L_, -1) == LUA_TSTRING || lua_type(L_, -1) == LUA_TNUMBER) {
          value.push_back(luaString(L_, -1));
          map->insert(std::make_pair(key, value));
        } else if (lua_type(L_, -1) == LUA_TTABLE) {
          int size = lua_objlen(L_, -1);
          int top = lua_gettop(L_);
          for (int i = 0; rc && i < size; ++i) {
            lua_pushinteger(L_, i+1);
            lua_gettable(L_, -2);
            if (lua_type(L_, -1) == LUA_TSTRING || lua_type(L_, -1) == LUA_TNUMBER) {
              value.push_back(luaString(L_, -1));
            } else {
              snprintf(errbuf_, MAX_ERR_LEN, "%s %s->%s element #%d must be string",
                       file_.c_str(), name, key.c_str(), i);
              rc = false;
            }
            lua_settop(L_, top);
          }
          if (rc) map->insert(std::make_pair(luaString(L_, -2), value));
        } else {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s->%s value must be string/array", file_.c_str(), name, key.c_str());
          rc = false;
        }
        lua_pop(L_, 1);
      }
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be hash table", file_.c_str(), name);
      rc = false;
    }

    lua_settop(L_, 0);
    return rc;
  }

  bool getArray(const char *name, std::vector<int> *value, bool required = true) {
    bool rc = true;
    lua_getglobal(L_, name);

    if (lua_isnil(L_, 1)) {
      if (required) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be array", file_.c_str(), name);
        rc = false;
      }
    } else if (lua_istable(L_, 1)) {
      int size = luaL_getn(L_, 1);
      for (int i = 0; rc && i < size; ++i) {
        lua_pushinteger(L_, i+1);
        lua_gettable(L_, 1);
        if (lua_isnumber(L_, -1)) {
          value->push_back((int) lua_tonumber(L_, -1));
        } else {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s element must be number", file_.c_str(), name);
          rc = false;
        }
      }
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be array", file_.c_str(), name);
      rc = false;
    }

    lua_settop(L_, 0);
    return rc;
  }

  bool getArray(const char *name, std::vector<std::string> *value, bool required = true) {
    bool rc = true;
    lua_getglobal(L_, name);

    if (lua_isnil(L_, 1)) {
      if (required) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be array", file_.c_str(), name);
        rc = false;
      }
    } else if (lua_istable(L_, 1)) {
      int size = luaL_getn(L_, 1);
      for (int i = 0; rc && i < size; ++i) {
        lua_pushinteger(L_, i+1);
        lua_gettable(L_, 1);
        if (lua_type(L_, -1) == LUA_TSTRING || lua_type(L_, -1) == LUA_TNUMBER) {
          value->push_back(luaString(L_, -1));
        } else {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s element must be string", file_.c_str(), name);
          rc = false;
        }
      }
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be array", file_.c_str(), name);
      rc = false;
    }

    lua_settop(L_, 0);
    return rc;
  }

  bool getFunction(const char *name, std::string *value, const std::string &def) {
    bool rc = true;
    lua_getglobal(L_, name);

    if (lua_isnil(L_, 1)) {
      value->assign(def);
    } else {
      if (lua_isstring(L_, 1)) {
        luaString(L_, 1, value);
      } else if (lua_isfunction(L_, 1)) {
        value->assign(name);
      } else {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s is not a function or function name", file_.c_str(), name);
        rc = false;
      }
    }

    lua_settop(L_, 0);
    return rc;
  }

  bool callResultNil() {
    if (lua_isnil(L_, 1)) {
      lua_settop(L_, 0);
      return true;
    }
    return false;
  }

  bool call(const char *name, const std::vector<std::string> &fields, int nret) {
    lua_getglobal(L_, name);
    initInputTableBeforeCall(fields);

    if (lua_pcall(L_, 1, nret, 0) != 0) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s error %s", file_.c_str(), name, lua_tostring(L_, -1));
      lua_settop(L_, 0);
      return false;
    }
    return true;
  }

  bool callResultListAsString(const char *name, std::string *result) {
    if (!lua_istable(L_, 1)) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1 must be table", file_.c_str(), name);
      lua_settop(L_, 0);
      return false;
    }

    int size = luaL_getn(L_, 1);
    if (size == 0) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1 must be not empty", file_.c_str(), name);
      lua_settop(L_, 0);
      return false;
    }

    for (int i = 0; i < size; ++i) {
      if (i > 0) result->append(1, ' ');
      lua_pushinteger(L_, i+1);
      lua_gettable(L_, 1);
      if (!lua_isstring(L_, -1) && !lua_isnumber(L_, -1)) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1[%d] is not string", file_.c_str(), name, i);
        lua_settop(L_, 0);
        return false;
      }
      luaString(L_, -1, result, true);
    }

    lua_settop(L_, 0);
    return true;
  }

  bool call(const char *name, const char *line, size_t nline, int nret = 1) {
    lua_getglobal(L_, name);
    lua_pushlstring(L_, line, nline);

    if (lua_pcall(L_, 1, nret, 0) != 0) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s error %s", file_.c_str(), name, lua_tostring(L_, -1));
      lua_settop(L_, 0);
      return false;
    }
    return true;
  }

  bool callResultString(const char *name, std::string *result, bool append = false) {
    if (!lua_isstring(L_, 1)) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1 must be string(nil)", file_.c_str(), name);
      lua_settop(L_, 0);
      return false;
    }

    luaString(L_, 1, result, append);
    lua_settop(L_, 0);
    return true;
  }

  bool callResultString(const char *name, std::string *r1, std::string *r2, bool append = false) {
    std::string *r[2] = {r1, r2};
    for (int i = 0; i < 2; ++i) {
      if (!lua_isstring(L_, i+1)) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #%d must be string(nil)", file_.c_str(), name, i+1);
        lua_settop(L_, 0);
        return false;
      }

      luaString(L_, i+1, r[i], append);
    }

    lua_settop(L_, 0);
    return true;
  }

  bool callResult(const char *name, std::string *s, std::map<std::string, int> *map) {
    if (!lua_isstring(L_, 1)) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1 must be string", file_.c_str(), name);
      lua_settop(L_, 0);
      return false;
    }
    luaString(L_, 1, s);

    if (!lua_istable(L_, 2)) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #2 must be hash table", file_.c_str(), name);
      lua_settop(L_, 0);
      return false;
    }

    lua_pushnil(L_);
    while (lua_next(L_, 2) != 0) {
      if (lua_type(L_, -2) != LUA_TSTRING) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #2 key must be string", file_.c_str(), name);
        lua_settop(L_, 0);
        return false;
      }
      if (lua_type(L_, -1) != LUA_TNUMBER) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #2 value must be number", file_.c_str(), name);
        lua_settop(L_, 0);
        return false;
      }
      map->insert(std::make_pair(luaString(L_, -2), (int) lua_tonumber(L_, -1)));
      lua_pop(L_, 1);
    }

    lua_settop(L_, 0);
    return true;
  }

private:
   void initInputTableBeforeCall(const std::vector<std::string> &fields) {
    lua_newtable(L_);
    int table = lua_gettop(L_);

    for (size_t i = 0; i < fields.size(); ++i) {
      lua_pushinteger(L_, i+1);
      lua_pushstring(L_, fields[i].c_str());
      lua_settable(L_, table);
    }
  }

private:
  lua_State   *L_;
  std::string  file_;
  char        *errbuf_;
};

#endif
