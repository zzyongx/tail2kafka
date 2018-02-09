-- time_local, request must exist
informat = {
  "remote_addr", "-", "#remote_user", "time_local", "request",
  "status", "#body_bytes_sent", "request_time", "#http_referer",
  "#http_user_agent", "#http_x_forwarded_for",
}

delete_request_field = true
time_local_format = "iso8601"

-- format time_local to iso8601

-- request: GET /pingback/storage?event=UPLOAD&hdfs_src=/pathtosrc&hdfs_dst=/hdfspath HTTP/1.1
-- auto add request_method GET
-- auto add request_uri    /pingback/storage
-- auto add request_qs     TABLE, event=UPLOAD, hdfs_src=/pathtosrc, hdfs_dst=/hdfspath

-- move method/uri/field in querystring to fields 
request_map = {
  ["uri"]         = "__uri__",
  ["querystring"] = "__query__",

  ["event"] = "event",
}

request_type = {
  ["status"]          = "i",
  ["request_time"]    = 'f',
}

-- if transform_param_fields is not nil, pass the selected fields to transform
-- the third value transform return must be nil
-- transform_param_fields = {}

-- return timestamp, type, field-table
transform = function(fields)
  return time, nil, fields
end
