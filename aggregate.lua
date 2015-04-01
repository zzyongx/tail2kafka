file      = "./nginx.log"
topic     = "nginx"
autosplit = true
timeidx   = 4
withhost  = true
withtime  = true

aggregate = function(fields)
  local n = table.getn(fields)
  if n < 16 then return nil end

  local reqt  = tonumber(fields[11]);
  if not reqt then return nil end
  
  local time   = fields[4]
  local status = "status_" .. fields[9]
  local size   = fields[10]
  local appid  = fields[n];
  
  if reqt < 0.1 then respt = "reqt<0.1"
  elseif reqt < 0.3 then reqt = "reqt<0.3"
  elseif reqt < 0.5 then reqt = "reqt<0.5"
  elseif reqt < 1   then reqt = "reqt<1"
  else reqt = "reqt_show" end

  local tbl = {size = size};
  tbl[status] = 1
  tbl[reqt] = 1

  return time, appid, tbl
end
