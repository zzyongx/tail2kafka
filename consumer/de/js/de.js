function zoomIn(param) {
  console.log("dbclick", param);
}

function zoomOut(param) {
  console.log("click", param);
}

function moreData(param) {
  // console.log(param);
}

function ecInit() {
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

  ec.on(echarts.config.EVENT.CLICK, zoomOut);
  ec.on(echarts.config.EVENT.DBLCLICK, zoomIn);
  ec.on(echarts.config.EVENT.DATA_ZOOM, moreData);
  
	return {handle: ec, option: option, unit: "s"};  // d(day) h(hour) m(min) s(sec)
}

function gskey(id)
{
  var re = /(T\d{2}:)(\d{2})(:\d{2})/;
  var min = id.match(re);
  if (min[2] < 30) min[2] = "00";
  else min[2] = "30";
  return id.replace(re, min[1] + min[2]);
}

function gmkey(cf)
{
  return cf.topic + cf.id;
}

function cacheit(cf, id, data)
{
  var mkey = gmkey(cf);
  var key = mkey + gskey(id);
  if (cf.cache[key] == undefined) {
    cf.cache[key] = [[id, data]];
  } else {
    if (cf.cache[key][0][0] > id) {
      cf.cache[key].unshift([id, data]);
    } else {
      cf.cache[key].push([id, data]);
    }
  }
  cf.last[cf.topic] = data;
}

function ecfresh(cf, ec, id, data, asc, force)
{
  if (id == undefined) {
    ec.handle.setOption(ec.option, true);
    return;
  }
  
  if (ec.option.xAxis[0].data.length == 0) {
    var len = cf.host.length;
    for (var i = 0; i < cf.host.length; i++) {
      for (var j = 0; j < cf.attr.length; j++) {
        var name;
        if (cf.host[i] == "cluster" || len == 0) {
          name = cf.attr[j];
        } else {
          name = cf.host[i] + "/" + cf.attr[j];
        }
        ec.option.series.push({
          name: name, type: 'line', data: []
        });
        ec.option.legend.data.push(name);
      }
    }
  }

  if (asc) ec.option.xAxis[0].data.push(id);
  else ec.option.xAxis[0].data.unshift(id);

  data.cluster = [];
  for (host in data) {
    for (attr in data[host]) {
      if (data.cluster[attr] == undefined) {
        data.cluster[attr] = data[host][attr];
      } else {
        data.cluster[attr] += data[host][attr];
      }
    }
  }

  var idx = 0;
  for (var i = 0; i < cf.host.length; i++) {
    for (var j = 0; j < cf.attr.length; j++) {
      var val;
      if (typeof(x) == "function") {
        val = cf.attr[i](data[cf.host[i]]);
      } else {
        val = data[cf.host[i]][cf.attr[j]];
      }
      if (asc) ec.option.series[idx].data.push(val);
      else ec.option.series[idx].data.unshift(val);
      idx++;
    }
  }

  // addData has bug, when dataGrow set true, it does not work
  if (ec.option.series[0].data % 10 == 0) force = true;
  if (force) ec.handle.setOption(ec.option, true);
}

function load_profile()
{
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
  });

  cf.cache = [];
  cf.last = [];
  return cf;
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
  if (unit == "d") return s;

  var h = date.getHours();
  if (h < 10) h = "0" + h;

  ts = ts + "T" + h;
  if (unit == "h") return s;

  var m = date.getMinutes();
  if (m < 10) m = "0" + m;

  var s = date.getSeconds();
  if (s < 10) s = "0" + s;
  
  return ts + ":" + m + ":" + s;
}

function ago(t, unit)
{
  if (unit == 'd') t = t * 86400;
  else if (unit == 'h') t = t * 3600;
  else if (unit == 'm') t = t * 60;

  return (new Date()).getTime() - t*1000;
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
  var time = iso8601(0, "s");
  var url = build_query("/cgi-bin/de.cgi",
                        {start:time, end: "forever", topic: cf.topic, id: cf.id});
  console.log(url);
  var es = new EventSource(url);
  es.onmessage = function(event) {
    eval('var obj = ' + event.data);
    ecfresh(cf, ec, event.lastEventId, obj, true, true);
    cacheit(cf, event.lastEventId, obj);
  };
  es.onerror = function(event) {
    consloe.log("auto fresh stop");
    es.close();
    setInterval(function() {auto_fresh(cf, ec);}, 1000);
  }
}

function fresh_host_wind(cf, search)
{
  var key = cf.topic;
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
    html += '<option value="' + hosts[i] + '">' + hosts[i] + '</option>';
  }
  $('#host-select-pre').html(html);
}

function fresh_attr_wind(cf)
{
  var key = cf.topic;
  var html = '';
  var attrs = Object.keys(cf.attrs[key].attr).sort();
  for (var i = 0; i < attrs.length; i++) {
    html += '<label class="checkbox-inline"><input type="checkbox" value="'
      + attrs[i] + '">' + attrs[i] + "</label>";
  }
  $('#attr-select').html(html);
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
      if (cf.attrs[key] == undefined) cf.attrs[key] = {host: [], attr: []};
      cf.attrs[key].host[host] = 1;
      for (attr in cf.last[key][host]) {
        cf.attrs[key].attr[attr] = 1;
      }
    }

    if (len1 != Object.keys(cf.attrs[key].attr).length ||
        len2 != Object.keys(cf.attrs[key].host).length) {
      fresh_host_attr_wind(cf);
    }

  }, 1 * 1000);
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

function init_host_cln(cf)
{
  $('#host-input-cln').click(function() {
    $('#host-select').html("");
  });
}

function init_attr_define(cf)
{
  $('#custom-attr-define').click(function() {
    var fname = $.trim($('#custom-attr-name').val());
    if (fname == "") {
      alert("custom attr need a name");
      return;
    }

    if (!fname.match(/^[a-z][a-z0-9A-Z_]+$/)) {
      alert(fname + ' need match /^[a-z][A-Za-z0-9_]+$/');
      return;
    }

    var funcs = $('#func-select input');
    for (var i = 0; i < funcs.length; i++) {
      if ($(funcs[i]).val() == fname) {
        alert("custom attr name duplicate");
        return;
      }
    }

    var fbody = $('#custom-attr-func').val();
    if (!eval('funcPtr = ' + fbody)) {
      alert("custom attr " + fbody + " eval failed");
      return;
    }

    var key = cf.topic;
    if (!cf.attrs[key].func) cf.attrs[key].func = [];
    cf.attrs[key].func[fname] = {ptr: funcPtr, def: fbody};

    $('#func-select').append(
      $('<label class="checkbox-inline"><input type="checkbox" value="'
        + fname + '">' + fname + '</label>'));
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

function init_show_me(cf, ec)
{
  $('#show-me').click(function() {
    var hosts = $('#host-select').val();
    if (!host) cf.host = hosts;

    var attrs = [];
    var chks = $('#attr-select input');
    for (var i = 0; i < chks.length; ++i) {
      if ($(chks[i]).prop("checked")) attrs.push($(chks[i]).val());
    }

    chks = $('#func-select input');
    for (var i = 0; i < chks.length; ++i) {
      if ($(chks[i]).prop("checked")) {
        var fname = $(chks[i]).val();
        attrs.push(cf.attrs[cf.topic].func[fname].ptr);
      }
    }
    
    if (attrs.length != 0) cf.attr = attrs;

    ec_option_init(ec);
    show_data(cf, ec);
  });
}

function ec_option_init(ec)
{
  ec.option.xAxis[0].data = [];
  ec.option.series = [];
  ec.option.legend.data = [];
}

function show_data(cf, ec)
{
  var start = iso8601(ago(30, "m"), "s");
  var end = iso8601(0, "s");
  
	var url = build_query("/cgi-bin/de.cgi",
                        {start: start, end: end, topic: cf.topic, id: cf.id});
  console.log(url);
	var es = new EventSource(url);

	es.onmessage = function(event) {
		eval('var obj=' + event.data);
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
    async = async == undefined ? true : false;
    $.ajax({
      method: "PUT",
      url: "/cgi-bin/de.profile.cgi",
      async: async,
      dataType: "text",
      data: JSON.stringify(cf),
    });
  };
  setInterval(savef, 30 * 1000);

  $(window).bind('click', function(event) {
    if(event.target.href)
      $(window).unbind('beforeunload');
  });
  $(window).bind('beforeunload', function(event) {
    savef(false);
  });
}

$(function() {
  cf = load_profile();
  if (!cf) return;
  
  ec = ecInit();

  show_data(cf, ec);

  fetch_host_attr(cf);
  fresh_host_attr_wind(cf);
  // auto_fresh(cf, ec);

  init_host_search(cf);
  init_host_add(cf);
  init_host_del(cf);
  init_host_cln(cf);
  init_attr_define(cf);
  init_attr_undef(cf);
  init_show_me(cf, ec);

  init_save_profile(cf);
});
