file     = "transform.log"
topic    = "transform"
autocreat = true
transform = function(line)
  local s = string.sub(line, 1, 7);
  if s == "[error]" then return line
  else return nil end
end
