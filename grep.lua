file     = "./access_log"
topic    = "httpd2"
timeidx  = 4
grep     = function(fields)
  return {fields[4], '"' .. fields[5] .. '"', fields[6], fields[table.maxn(fields)]}
end
