file  = "logs/indexdoc.log"
autocreat = true
startpos = "LOG_START"
indexdoc = function(line)
  return "indexdoc", line
end
