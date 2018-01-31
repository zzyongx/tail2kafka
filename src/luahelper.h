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

  const char *file() const { return file_; }

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

  bool getString(const char *name, std::string *value) {
    bool rc = true;
    lua_getglobal(L_, name);
    if (lua_isstring(L_, 1)) {
      value->assign(lua_tostring(L_, 1));
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be string", file_, name);
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
        value->assign(lua_tostring(L_, 1));
      } else {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be string", file_, name);
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
        *value = lua_tonumber(L_, 1);
      } else {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be number", file_, name);
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
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be boolean", file_, name);
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
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be hash table", file_, name);
        rc = false;
      }
    } else if (lua_istable(L_, 1)) {
      lua_pushnil(L_);
      while (rc && lua_next(L_, 1) != 0) {
        if (lua_type(L_, -2) != LUA_TSTRING) {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s key must be string", file_, name);
          rc = false;
        } else if (lua_type(L_, -1) != LUA_TSTRING && lua_type(L_, -1) != LUA_TNUMBER) {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s value must be string", file_, name);
          rc = false;
        } else {
          map->insert(std::make_pair(lua_tostring(L_, -2), lua_tostring(L_, -1)));
        }
        lua_pop(L_, 1);
      }
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be hash table", file_, name);
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
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be array", file_, name);
        rc = false;
      }
    } else if (lua_istable(L_, 1)) {
      int size = luaL_getn(L_, 1);
      for (int i = 0; rc && i < size; ++i) {
        lua_pushinteger(L_, i+1);
        lua_gettable(L_, 1);
        if (lua_isnumber(L_, -1)) {
          value->push_back(lua_tonumber(L_, -1));
        } else {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s element must be number", file_, name);
          rc = false;
        }
      }
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be array", file_, name);
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
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be array", file_, name);
        rc = false;
      }
    } else if (lua_istable(L_, 1)) {
      int size = luaL_getn(L_, 1);
      for (int i = 0; rc && i < size; ++i) {
        lua_pushinteger(L_, i+1);
        lua_gettable(L_, 1);
				if (lua_type(L_, -1) == LUA_TSTRING || lua_type(L_, -1) == LUA_TNUMBER) {
					value->push_back(lua_tostring(L_, -1));
        } else {
          snprintf(errbuf_, MAX_ERR_LEN, "%s %s element must be string", file_, name);
          rc = false;
        }
      }
    } else {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s must be array", file_, name);
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
        value->assign(lua_tostring(L_, 1));
      } else if (lua_isfunction(L_, 1)) {
        value->assign(name);
      } else {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s is not a function or function name", file_, name);
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
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s error %s", file_, name, lua_tostring(L_, -1));
      lua_settop(L_, 0);
      return false;
    }
    return true;
  }

  bool callResultListAsString(const char *name, std::string *result) {
    if (!lua_istable(L_, 1)) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1 must be table", file_, name);
      lua_settop(L_, 0);
      return false;
    }

    int size = luaL_getn(L_, 1);
    if (size == 0) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1 must be not empty", file_, name);
      lua_settop(L_, 0);
      return false;
    }

    for (int i = 0; i < size; ++i) {
      if (i > 0) result->append(1, ' ');
      lua_pushinteger(L_, i+1);
      lua_gettable(L_, 1);
      if (!lua_isstring(L_, -1) && !lua_isnumber(L_, -1)) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1[%d] is not string", file_, name, i);
        lua_settop(L_, 0);
        return false;
      }
      result->append(lua_tostring(L_, -1));
    }

    lua_settop(L_, 0);
    return true;
  }

  bool call(const char *name, const char *line, size_t nline) {
    lua_getglobal(L_, name);
    lua_pushlstring(L_, line, nline);

    if (lua_pcall(L_, 1, 1, 0) != 0) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s error %s", file_, name, lua_tostring(L_, -1));
      lua_settop(L_, 0);
      return false;
    }
    return true;
  }

  bool callResultString(const char *name, std::string *result) {
    if (!lua_isstring(L_, 1)) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1 must be string(nil)", file_, name);
      lua_settop(L_, 0);
      return false;
    }

    result->append(lua_tostring(L_, 1));
    lua_settop(L_, 0);
    return true;
  }

  bool callResult(const char *name, std::string *s, std::map<std::string, int> *map) {
    if (!lua_isstring(L_, 1)) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #1 must be string", file_, name);
      lua_settop(L_, 0);
      return false;
    }
    s->assign(lua_tostring(L_, 1));

    if (!lua_istable(L_, 2)) {
      snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #2 must be hash table", file_, name);
      lua_settop(L_, 0);
      return false;
    }

    lua_pushnil(L_);
    while (lua_next(L_, 2) != 0) {
      if (lua_type(L_, -2) != LUA_TSTRING) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #2 key must be string", file_, name);
        lua_settop(L_, 0);
        return false;
      }
      if (lua_type(L_, -1) != LUA_TNUMBER) {
        snprintf(errbuf_, MAX_ERR_LEN, "%s %s return #2 value must be number", file_, name);
        lua_settop(L_, 0);
        return false;
      }
      map->insert(std::make_pair(lua_tostring(L_, -2), lua_tonumber(L_, -1)));
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
  lua_State *L_;
  const char *file_;
  char *errbuf_;
};

#endif
