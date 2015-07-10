function init_zoom(cf, ec)
{
  cf.zoomlen = ec.option.dataZoom.end - ec.option.dataZoom.start;
  
  ec.handle.on(echarts.config.EVENT.DATA_ZOOM, function(param) {
    var zoom = param.zoom;
    var zoomlen = zoom.end - zoom.start;
    
    var bef, start, end;
    if (zoom.start == 0 && zoom.end != 100 && zoomlen == cf.zoomlen) {
      bef = get_bef(cf.unit);
      end = cf.start;
      start = cf.start = iso8601(ago(bef, cf.unit, get_millisecond(cf.start)), cf.unit);
      console.log("get more old data", cf.start, " ", cf.end);
    } else if (zoom.end == 100 && zoom.start != 0 && zoomlen == cf.zoomlen) {
      bef = get_bef(cf.unit);
      start = cf.end;
      var t = ago(bef * -1, cf.unit, get_millisecond(cf.end));
      if (t > (new Date()).getTime()) t = 0;
      end = cf.end = iso8601(t, cf.unit);
      console.log("get more new data", cf.start, " ", cf.end);
    } else {
      cf.zoomlen = zoom.end - zoom.start;
    }
    if (bef != undefined) {
      disable_auto_fresh(cf);
      show_data(cf, ec, start, end);
    }
  });
}

function ecInit(cf) {
  var ec = echarts.init(document.getElementById('main')); 
  
  var option = {
    tooltip: {
      show: true,
      trigger: 'axis',
    },
    legend: {
      data: [],
    },
    dataZoom: {
      show: true,
      realtime: false,
      start: 50,
      end: 100
    },
    xAxis : [{
      type : 'category',
      data : [],
    }],
    yAxis : [{
      type : 'value',
    }],
    series : [],
  };

  var ret = {handle: ec, option: option};
  init_zoom(cf, ret);
  return ret;
}

function gskey(id)
{
  var re = /(T\d{2}:)(\d{2})(:\d{2})?/;
  var min = id.match(re);
  if (min[2] < 30) min[2] = "00";
  else min[2] = "30";
  return id.replace(re, min[1] + min[2]);
}

function gmkey(cf)
{
  return cf.topic + cf.id;
}

function time_approximate(a, b)
{
  return Math.abs(get_millisecond(a) - get_millisecond(b)) < 7000;
}

function clear_cache(cf)
{
  cf.cache.topic = cf.topic;
  cf.cache.id = cf.id;
  cf.cache.unit = cf.unit;
  cf.cache.data = undefined;
}

function adjust_by_cache(cf)
{
  var range = {start: cf.start, end: cf.end};
  
  if (cf.unit == "d" || cf.unit == "h" || cf.unit == "m") return range;
  
  if (cf.cache.topic == cf.topic && cf.cache.id == cf.id &&
      cf.cache.unit == cf.unit &&
      (cf.cache.start < cf.start || time_approximate(cf.cache.start, cf.start))) {
    if (cf.end > cf.cache.end) {
      range.start = cf.cache.end;
      range.end = cf.end;
    }
  } else {
    clear_cache(cf);
    range.start = cf.start;
    range.end = cf.end;
  }
  return range;
}

function cacheit(cf, id, data)
{
  if (cf.unit == "d" || cf.unit == "h" || cf.unit == "m") return;
  if (cf.topic != cf.cache.topic || cf.id != cf.cache.id &&
      cf.unit != cf.cache.unit) {
    clear_cache(cf);
  }

  if (cf.cache.data == undefined) {
    cf.cache.data = [[id, data]];
    cf.cache.start = cf.cache.end = id
  } else {
    if (id < cf.cache.start) {
      cf.cache.data.unshift([id, data]);
      cf.cache.start = id;
    } else if (id > cf.cache.end) {
      cf.cache.data.push([id, data]);
      cf.cache.end = id;
    }
  }
  cf.last[cf.topic] = data;
}

function ecfresh(cf, ec, id, data, force)
{
  if (id == undefined) {
    ec.handle.setOption(ec.option, true);
    return;
  }
  
  if (ec.option.xAxis[0].data.length == 0) {
    var len = cf.host.length;
    for (var i = 0; i < cf.host.length; i++) {
      for (var j = 0; j < cf.attr.length; j++) {
        var attr = (typeof(cf.attr[j]) == "object") ? cf.attr[j].name : cf.attr[j];
        var name = (cf.host[i] == "cluster" || len == 0) ? attr : cf.host[i] + "/" + attr;
        ec.option.series.push({
          name: name, type: 'line', data: []
        });
        ec.option.legend.data.push(name);
      }
    }
  }

  var asc;
  var len = ec.option.xAxis[0].data.length;
  if (len == 0 || id > ec.option.xAxis[0].data[len-1]) {
    ec.option.xAxis[0].data.push(id);
    asc = true;
  } else if (id < ec.option.xAxis[0].data[0]) {
    ec.option.xAxis[0].data.unshift(id);
    asc = false;
  }

  var cluster = {};
  for (host in data) {
    if (host == "cluster") continue;
    
    for (attr in data[host]) {
      if (cluster[attr] == undefined) {
        cluster[attr] = data[host][attr];
      } else {
        cluster[attr] += data[host][attr];
      }
    }
  }
  data.cluster = cluster;

  var idx = 0;
  for (var i = 0; i < cf.host.length; i++) {
    for (var j = 0; j < cf.attr.length; j++) {
      var val;
      if (typeof(cf.attr[j]) == "object") {
        val = cf.attr[j].ptr(data[cf.host[i]]);
      } else {
        if (data[cf.host[i]] != undefined) {
          val = data[cf.host[i]][cf.attr[j]];
          if (val == undefined) val = 0;
        } else {
          val = 0;
        }
      }
      if (asc != undefined) {
        if (asc) ec.option.series[idx].data.push(val);
        else ec.option.series[idx].data.unshift(val);
      }
      idx++;
    }
  }

  // addData has bug, when dataGrow set true, it does not work
  if (cf.unit == "s" || cf.unit == "ss") {
    if (ec.option.series[0].data % 10 == 0) force = true;
  } else {
    force = true;
  }
  if (force) ec.handle.setOption(ec.option, true);
}

function init_profile()
{
  $('#work-window').hide();
  $('#init-window').show();
      
  $("#init-profile").keyup(function(event){
    if(event.keyCode == 13){
      $("#init-profile").click();
    }
  });

  $('#init-profile').click(function() {
    var user  = $.trim($('#init-user').val());
    if (user == "") {
      alert("user is required");
      return;
    }

    var query = {user: user};

    var topic = $.trim($('#init-topic').val());
    if (topic != "") query.topic = topic;
    
    var id    = $.trim($('#init-id').val());
    if (id != "") query.id = id;
    
    var attr  = $.trim($('#init-attr').val());
    if (attr != "") query.attr = attr;

    $.ajax({
      method: "GET",
      url: "/cgi-bin/de.profile.cgi",
      async: false,
      data: query,
      dataType: "json",
    }).done(function() {
      $(location).attr("search", "");
      $(location).reload();
    }).fail(function(xhr, textStatus) {
      alert("init profile error:" + textStatus);
    });
  });
}

function load_profile()
{
  $('#init-window').hide();

  if ($(location).attr('search') == "?forceinit") {
    init_profile();
    return null;
  }
  
  var error = false;
  var cf;
  $.ajax({
    url: "/cgi-bin/de.profile.cgi",
    async: false,
    dataType: "json",
  }).done(function(data) {
    cf = data;
    console.log(cf);
  }).fail(function(xhr, textStatus) {
    alert("get profile error:" + textStatus);
    error = true;
  });

  if (error) {
    init_profile();
    return null;
  }

  if (cf.attrs == undefined) cf.attrs = {};

  // eval the custom attr
  for (key in cf.attrs) {
    for (fname in cf.attrs[key].func) {
      eval('funcPtr = ' + cf.attrs[key].func[fname].def);
      cf.attrs[key].func[fname].ptr = funcPtr;
    }
  }

  console.log("load profile ", cf);
  for (var i = 0; i < cf.attr.length; i++) {
    if (typeof(cf.attr[i]) == "object") {
      cf.attr[i].ptr = cf.attrs[cf.topic].func[cf.attr[i].name].ptr;
    }
  }

  return load_profile_post(cf);
}

function load_profile_post(cf)
{
  if (cf.unit == undefined) cf.unit = "s";
  if (cf.autofresh == undefined) cf.autofresh = false;
  
  var bef = get_bef(cf.unit);

  cf.start = iso8601(ago(bef, cf.unit), cf.unit);
  cf.end = iso8601(0, cf.unit);
  
  cf.cache = {};
  cf.last = {};
  cf.idset = {};
  return cf;
}

function load_topic(cf)
{
  $.ajax({
    url: "/cgi-bin/de.topic.cgi",
    dataType: "json",
  }).done(function(data) {
    console.log("load_topic", data);
    cf.topics = data;
    init_topic_wind(cf);
  }).fail(function(xhr, textStatus) {
    alert("get topic error:" + textStatus);
  });
}

function load_idset(cf)
{
  if (cf.idset[cf.topic]) {
    fresh_id_wind(cf);
  } else {
    $.ajax({
      url: "/cgi-bin/de.id.cgi",
      data: {topic: cf.topic},
      dataType: "json",
    }).done(function(data) {
      cf.idset[cf.topic] = data;
      fresh_id_wind(cf);
    }).fail(function(xhr, textStatus) {
      alert("get idset error:" + textStatus);
    });
  }
}

function get_millisecond(str) {
  var match;
  var date;
  if ((match = str.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/))) {
    date = new Date(match[1], match[2] - 1, match[3], match[4], match[5], match[6]);
  } else if ((match = str.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/))) {
    date = new Date(match[1], match[2] - 1, match[3], match[4], match[5], 0);
  } else if ((match = str.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2})$/))) {
    date = new Date(match[1], match[2] - 1, match[3], match[4], 0, 0);
  } else if ((match = str.match(/^(\d{4})-(\d{2})-(\d{2})$/))) {
    date = new Date(match[1], match[2] - 1, match[3], 0, 0, 0);
  }
  return date.getTime();
}


// 2015-04-21T16:30:00
function iso8601(ts, unit)
{
  var date = ts == 0 ? new Date() : new Date(ts);
  
  var m = date.getMonth() + 1;
  if (m < 10) m = "0" + m;

  var d = date.getDate();
  if (d < 10) d = "0" + d;

  var ts = date.getFullYear() + "-" + m + "-" + d;
  if (unit == "d") return ts;

  var h = date.getHours();
  if (h < 10) h = "0" + h;

  ts = ts + "T" + h;
  if (unit == "h") return ts;

  var m = date.getMinutes();
  if (m < 10) m = "0" + m;

  ts = ts + ":" + m;
  if (unit == "m") return ts;

  var s = date.getSeconds();
  if (s < 10) s = "0" + s;
  
  return ts + ":" + s;
}

function get_bef(unit)
{
  var bef;
  if (unit == "s" || unit == "ss") bef = 30 * 60;
  else if (unit == "m") bef = 300;
  else bef = 30;
  return bef;
}

function ago(t, unit, now)
{
  if (unit == 'd') t = t * 86400;
  else if (unit == 'h') t = t * 3600;
  else if (unit == 'm') t = t * 60;

  var date = (now == undefined ? new Date() : new Date(now));
  return date.getTime() - t * 1000;
}

function build_query(path, params)
{
  var v = [];
  for (key in params) {
    v.push(key + "=" + params[key]);
  }
  
  return path + "?" + v.join("&");
}

function auto_fresh(cf, ec)
{
  setTimeout(function() {
    auto_fresh_impl(cf,ec);
  }, 1000);
}

function auto_fresh_impl(cf, ec)
{
  if (!cf.autofresh) return;

  var time;
  if (Math.abs((new Date()).getTime() - get_millisecond(cf.end)) < 600 * 1000) {
    time = cf.end;
  } else {
    clear_cache(cf);
    ec_option_init(ec);
    time = iso8601(0, cf.unit);
  }
  var url = build_query("/cgi-bin/de.cgi",
                        {start:time, end: "forever", topic: cf.topic, id: cf.id});
  console.log(url);
  var es = new EventSource(url);
  es.onmessage = function(event) {
    if (!cf.autofresh) {
      es.close();
      return;
    }
    var obj = JSON.parse(event.data);
    // console.log(Object.keys(obj).length);
    cf.end = event.lastEventId;
    ecfresh(cf, ec, event.lastEventId, obj, true, true);
    cacheit(cf, event.lastEventId, obj);
  };
  es.onerror = function(event) {
    consloe.log("auto fresh stop");
    es.close();
    auto_fresh(cf, ec);
  }
}

function init_topic_wind(cf)
{
  var html = '';
  var topics = Object.keys(cf.topics);
  for (var i = 0; i < topics.length; ++i) {
    if (topics[i] == cf.topic) {
      html += '<option value="' + topics[i] + '" selected>' + topics[i] + '</option>';
    } else {
      html += '<option value="' + topics[i] + '">' + topics[i] + '</option>';
    }
  }
  $('#topic-select').html(html);
}

function init_topic_select(cf)
{
  $('#topic-select').change(function() {
    disable_auto_fresh(cf);
    
    var topic = $('#topic-select').val();
    cf.topic = topic;
    cf.id    = cf.topics[topic].id;
    cf.host  = cf.topics[topic].host;
    cf.attr  = cf.topics[topic].attr;

    load_idset(cf);
    fresh_host_wind(cf);
    $('#host-select').html("");
    fresh_attr_wind(cf);
  });
}

function fresh_id_wind(cf)
{
  var html = '';
  var ids = cf.idset[cf.topic];
  
  for (var i = 0; i < ids.length; ++i) {
    if (cf.id == ids[i]) {
      html += '<option value="' + ids[i] + '" selected>' + ids[i] + '</option>';
    } else {
      html += '<option value="' + ids[i] + '">' + ids[i] + '</option>';
    }
  }
  $('#id-select').html(html);
}

function fresh_host_wind(cf, search)
{
  var key = cf.topic;
  if (cf.attrs[key] == undefined) return;
  
  var html = '';
  var hosts = [];
  if (search != undefined) {
    var allhosts = Object.keys(cf.attrs[key].host);
    for (var i = 0; i < allhosts.length; i++) {
      if (allhosts[i].indexOf(search) != -1) {
        hosts.push(allhosts[i]);
      }
    }
  } else {
    hosts = Object.keys(cf.attrs[key].host).sort();
  }
  
  for (var i = 0; i < hosts.length; i++) {
    if (hosts[i] == cf.host) {
      html += '<option value="' + hosts[i] + '" selected>' + hosts[i] + '</option>';
    } else {
      html += '<option value="' + hosts[i] + '">' + hosts[i] + '</option>';
    }
  }
  $('#host-select-pre').html(html);
}

function fresh_attr_wind(cf)
{
  var key = cf.topic;
  if (cf.attrs[key] == undefined) return;
  
  var html = '';
  var attrs = Object.keys(cf.attrs[key].attr).sort();
  for (var i = 0; i < attrs.length; i++) {
    if (attrs[i] == cf.attr) {
      html += '<label class="checkbox-inline"><input type="checkbox" value="'
        + attrs[i] + '" checked>' + attrs[i] + "</label>";
    } else {
      html += '<label class="checkbox-inline"><input type="checkbox" value="'
        + attrs[i] + '">' + attrs[i] + "</label>";
    }
  }
  $('#attr-select').html(html);

  html = '';
  var funcs = Object.keys(cf.attrs[key].func).sort();
  for (var i = 0; i < funcs.length; i++) {
    html += '<label class="checkbox-inline"><input type="checkbox" value="'
      + funcs[i] + '">' + funcs[i] + '</label>';
  }
  $('#func-select').html(html);
}

function fresh_host_attr_wind(cf)
{
  if (cf.attrs[cf.topic] == undefined) return;

  fresh_host_wind(cf);
  fresh_attr_wind(cf);
}

function fetch_host_attr(cf)
{
  setInterval(function() {
    var key = cf.topic;    
    if (cf.last[key] == undefined) return;

    var len1 = 0;
    var len2 = 0;
    if (cf.attrs[key] != undefined) {
      len1 = Object.keys(cf.attrs[key].attr).length;
      len2 = Object.keys(cf.attrs[key].host).length;
    }
    
    for (host in cf.last[key]) {
      if (cf.attrs[key] == undefined) cf.attrs[key] = {host: {}, attr: {}, func:{}};
      cf.attrs[key].host[host] = true;
      for (attr in cf.last[key][host]) {
        cf.attrs[key].attr[attr] = true;
      }
    }

    if (len1 != Object.keys(cf.attrs[key].attr).length ||
        len2 != Object.keys(cf.attrs[key].host).length) {
      fresh_host_attr_wind(cf);
    }

  }, 1 * 1000);
}

function init_time_change(cf)
{
  var f = function() {
    var wind = $(this);
    var time = $.trim(wind.val());
    if (time == "") return;

    var match;
    var year, month, day, hour, min = "00", second = "00";
    if ((match = time.match(/^(\d{4})-(\d{2})-(\d{2})/))) {
      year  = match[1];
      month = match[2];
      day   = match[3];
    } else if ((match = time.match(/^(\d{2})-(\d{2})/))) {
      month = match[1];
      day   = match[2];
    } else if ((match = time.match(/^(\d{2})/))) {
      day   = match[1];
    }

    if ((match = time.match(/\d{2}T(\d{2}):(\d{2}):(\d{2})$/))) {
      hour   = match[1];
      min    = match[2];
      second = match[3];
    } else if ((match = time.match(/\d{2}T(\d{2}):(\d{2})$/))) {
      hour   = match[1];
      min    = match[2];
    } else if ((match = time.match(/\d{2}T(\d{2})$/))) {
      hour   = match[1];
    }

    var error = false;
    if (day == undefined) {
      error = true;
    } else {
      var date = new Date();
      
      if (year == undefined) year = date.getFullYear();
      if (month == undefined) {
        month = date.getMonth()+1;
        if (month < 10) month = "0" + month;
      }
      if (hour == undefined) {
        hour = date.getHours();
        if (hour < 10) hour = "0" + hour;
      }

      if (year   >= 2015 && year  <= 2115 &&
          month  >= 1    && month <= 12 &&
          day    >= 1    && day   <= 31 &&
          hour   >= 0    && hour  <= 23 &&
          min    >= 0    && min   <= 59 &&
          second >= 0    && second <= 59) {
        wind.val(year + "-" + month + "-" + day + "T" + hour + ":" + min + ":" + second);
      } else {
        error = true;
      }
    }

    if (error) {
      alert("invalid date time " + time);
      wind.focus();
      wind.val("");
    }
  };
  
  $('#start-time').change(f);
  $('#end-time').change(f);
}

function init_unit_click(cf)
{
  var buttons = $('#unit button');
  var units = ["d", "h", "m", "s", "ss"];
  
  buttons.each(function(index, value) {
    if (units[index] == cf.unit) {
      $(value).addClass("btn-primary");
    }
    $(value).click(function() {
      var unit = units[index];
      if (unit != "m" && unit != "s" && unit != "ss") {
        alert("please disable auto fresh first");
        return;
      }
      
      buttons.each(function(i, id) {
        $(id).removeClass("btn-primary");
      });
      $(this).addClass("btn-primary");
      cf.unit = units[index];
    });
  });
}

function disable_auto_fresh(cf)
{
  cf.autofresh = false;
  $('#auto-fresh').removeClass("btn-primary");
}

function init_autofresh_click(cf, ec)
{
  var self = $('#auto-fresh');
  if (cf.autofresh) {
    self.addClass("btn-primary");
    auto_fresh(cf, ec);
  }
  
  self.click(function() {
    if (cf.unit != "m" && cf.unit != "s" && cf.unit != "ss") {
      alert("only minute/second/seconds support autofresh");
      return;
    }
    
    cf.autofresh = !cf.autofresh;
    if (cf.autofresh) {
      self.addClass("btn-primary");
      auto_fresh(cf, ec);
    } else {
      self.removeClass("btn-primary");
    }
  });
}

function init_host_search(cf)
{
  $('#host-search').keyup(function() {
    var str = $.trim($('#host-search').val());
    if (str == "") {
      fresh_host_wind(cf);
    } else {
      fresh_host_wind(cf, str);
    }
  });
}

function init_host_add(cf)
{
  $('#host-input-add').click(function() {
    var hosts = $('#host-select-pre').val();
    var options = $('#host-select option');
    for (var i = 0; i < hosts.length; ++i) {
      var j;
      for (j = 0; j < options.length; ++j) {
        if (hosts[i] == $(options[j]).val()) break;
      }
      if (j == options.length) {
        $('#host-select').append(
          $("<option></option>").attr("value", hosts[i]).text(hosts[i]));
      }
    }
  });
}

function init_host_del(cf)
{  
  $('#host-input-del').click(function() {
    var hosts = $('#host-select').val();
    for (var i = 0; i < hosts.length; ++i) {
      $('#host-select option[value="' + hosts[i] + '"]').remove();
    }
  });
}

function init_host_cln(cf, hosts)
{
  $('#host-input-cln').click(function() {
    $('#host-select').html("");
  });
}

function init_attr_name_search(cf)
{
  $('#custom-attr-name').keyup(function() {
    var fname = $.trim($('#custom-attr-name').val());
    
    var funcs = $('#func-select input');
    for (var i = 0; i < funcs.length; i++) {
      if ($(funcs[i]).val() == fname) {
        $('#custom-attr-func').val(cf.attrs[cf.topic].func[fname].def)
        break;
      }
    }
  });
}

function init_attr_define(cf)
{
  $('#custom-attr-define').click(function() {
    var fname = $.trim($('#custom-attr-name').val());
    var fbody = $.trim($('#custom-attr-func').val());
    
    if (fname == "") {
      alert("custom attr need a name");
      return;
    }

    if (fbody == "") {
      var key = cf.topic;
      cf.attrs[key].attr[fname] = true;
      fresh_attr_wind(cf);
      return;
    }

    if (!fname.match(/^[a-z][a-z0-9A-Z_]+$/)) {
      alert(fname + ' need match /^[a-z][A-Za-z0-9_]+$/');
      return;
    }

    if (!eval('funcPtr = ' + fbody)) {
      alert("custom attr " + fbody + " eval failed");
      return;
    }

    var key = cf.topic;
    cf.attrs[key].func[fname] = {ptr: funcPtr, def: fbody};

    var i;
    var funcs = $('#func-select input');
    for (i = 0; i < funcs.length; i++) {
      if ($(funcs[i]).val() == fname) break;
    }
    if ( i == funcs.length) {
      $('#func-select').append(
        $('<label class="checkbox-inline"><input type="checkbox" value="'
          + fname + '">' + fname + '</label>'));
    }
  });
}

function init_attr_undef(cf)
{
  $('#custom-attr-undef').click(function() {
    var fname = $.trim($('#custom-attr-name').val());
    var funcs = $('#func-select label');
    for (var i = 0; i < funcs.length; ++i) {
      if ($(funcs[i]).find("input").val() == fname) {
        delete cf.attrs[cf.topic].func[fname];
        $(funcs[i]).remove();
        break;
      }
    }
  });
}

function get_start_end(cf)
{
  var start = $.trim($("#start-time").val());
  var end = $.trim($("#end-time").val());
  
  if (start != "" || end != "") {
    var bef = get_bef(cf.unit);
    var start_millis, end_millis;
    
    if (start == "") start_millis = ago(bef, cf.unit, get_millisecond(end));
    else start_millis = get_millisecond(start);
      
    if (end == "") end_millis = ago(bef * -1, cf.unit, get_millisecond(start));
    else end_millis = get_millisecond(end);

    var sec;
    if (cf.unit == "d") sec = bef * 86400;
    else if (cf.unit == "h") sec = bef * 3600;
    else if (cf.unit == "m") sec = bef * 60;
    else sec = bef;

    var diff = end_millis - start_millis;
    var error;

    if (diff < 0) {
      error = "end before start\n" +
        "start " + start + "\n" + "end " + end;
    } else if (diff > sec * 1000) {
      error = "need too many data " + diff + "\n" +
        "start " + start + "\n" +
        "end   " + end + "\n" +
        "unit is " + cf.unit;
    }
    if (error != undefined) {
      alert(error);
      $('#start-time').focus();
      return false;
    }

    cf.start = iso8601(start_millis, cf.unit);
    cf.end = iso8601(end_millis, cf.unit);
  } else {
    var bef = get_bef(cf.unit);

    var start = iso8601(ago(bef, cf.unit), cf.unit);
    if (start > cf.end) cf.start = start;
    cf.end = iso8601(0, cf.unit);
  }                       

  return true;
}

function init_show_me(cf, ec)
{
  $('#show-me').click(function() {
    disable_auto_fresh(cf);
    cf.last = {};
    
    var topic = $.trim($('#topic-input').val());
    if (topic == "") topic = $('#topic-select').val();
    if (topic) cf.topic = topic;

    var id = $.trim($('#id-input').val());
    if (id == "") id = $('#id-select').val();
    if (id) cf.id = id;

    if (!get_start_end(cf)) return;
      
    var hosts = [];
    $('#host-select option').each(function(i, o) {
      hosts.push($(o).val());
    });
    if (hosts.length > 0) cf.host = hosts;

    var attrs = [];
    var chks = $('#attr-select input');
    for (var i = 0; i < chks.length; ++i) {
      if ($(chks[i]).prop("checked")) attrs.push($(chks[i]).val());
    }

    chks = $('#func-select input');
    for (var i = 0; i < chks.length; ++i) {
      if ($(chks[i]).prop("checked")) {
        var fname = $(chks[i]).val();
        attrs.push({name:fname, ptr: cf.attrs[cf.topic].func[fname].ptr});
      }
    }
    
    if (attrs.length != 0) cf.attr = attrs;

    var range = adjust_by_cache(cf);

    ec_option_init(ec);
    if (cf.cache.data != undefined) {
      for (var i = 0; i < cf.cache.data.length; ++i) {
        ecfresh(cf, ec, cf.cache.data[i][0], cf.cache.data[i][1], true);
      }
    }
    if (range.start == range.end) return;

    show_data(cf, ec, range.start, range.end);
  });
}

function ec_option_init(ec)
{
  ec.option.xAxis[0].data = [];
  ec.option.series = [];
  ec.option.legend.data = [];
}

function show_data(cf, ec, start, end)
{
	var url = build_query("/cgi-bin/de.cgi",
                        {start: start, end: end, topic: cf.topic,
                         id: cf.id, dataset: cf.unit == "ss" ? "all" : "samp"});
  console.log(url);
	var es = new EventSource(url);

	es.onmessage = function(event) {
    // console.log(event.lastEventId, event.data);
    var obj = JSON.parse(event.data);
    ecfresh(cf, ec, event.lastEventId, obj, false);
    cacheit(cf, event.lastEventId, obj);
	};
	es.onerror = function(event) {
    ecfresh(cf, ec, undefined);
		console.log("stop");
		es.close();
	}
}

function init_save_profile(cf)
{
  var savef = function(async) {
    var sf = {topic: cf.topic, id: cf.id, attr: cf.attr, host: cf.host,
              unit: cf.unit, autofresh: cf.autofresh, attrs: cf.attrs};
    async = async == undefined ? true : false;
    $.ajax({
      method: "PUT",
      url: "/cgi-bin/de.profile.cgi",
      async: async,
      dataType: "text",
      data: JSON.stringify(sf),
    });
  };
  setInterval(savef, 30 * 1000);

  $(window).bind('click', function(event) {
    if(event.target.href)
      $(window).unbind('beforeunload');
  });
  $(window).bind('beforeunload', function(event) {
    savef(false);
    return "^_^";
  });
}

$(function() {
  cf = load_profile();
  if (!cf) return;
  
  ec = ecInit(cf);

  load_topic(cf);
  load_idset(cf);
  show_data(cf, ec, cf.start, cf.end);

  init_topic_select(cf);

  fetch_host_attr(cf);
  fresh_host_attr_wind(cf);

  init_host_search(cf);
  init_host_add(cf);
  init_host_del(cf);
  init_host_cln(cf);
  init_attr_name_search(cf);
  init_attr_define(cf);
  init_attr_undef(cf);
  
  init_time_change(cf);
  init_unit_click(cf);
  init_autofresh_click(cf, ec);
  
  init_show_me(cf, ec);
  init_save_profile(cf);
});
