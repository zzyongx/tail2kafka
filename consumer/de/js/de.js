function zoomIn(param) {
  console.log(param);
}

function zoomOut(param) {
  console.log(param);
}

function moreData(param) {
  // console.log(param);
}

function ecInit() {
  var ec = echarts.init(document.getElementById('main')); 
  
  var option = {
    tooltip: {
      show: true
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

function msort(data) {
  var newdata = [];
  var ptr = [];
  while (true) {
    var min = undefined;
    var idx = undefined;
    for (var i = 0; i < data.length; ++i) {
      if (ptr[i] == undefined) ptr[i] = 0;
      if (ptr[i] < data[i].length) {
        if (min == undefined || data[i][ptr[i]]) {
          min = data[i][ptr[i]];
          idx = i;
        }
      }
    }
    if (idx == undefined) break;
    newdata.push(data[idx][ptr[idx]]);
    ptr[idx]++;
  }
  return newdata;
}

function ecfresh(cf, ec, id, data, asc) {
  if (ec.option.xAxis[0].data.length == 0) {
    for (var i = 0; i < cf.attr.length; i++) {
      ec.option.series.push({
        name: cf.attr[i], type: 'line', data: []
      });
      ec.option.legend.data.push(cf.attr[i]);
    }
  }

  if (asc) ec.option.xAxis[0].data.push(id);
  else ec.option.xAxis[0].data.push(id);

  var adata = new Array(cf.attr.length);
  for (var i = 0; i < adata.length; i++) adata[i] = 0;

  if (cf.host == "cluster") {
    for (host in data) {
      for (var i = 0; i < cf.attr.length; i++) {
        adata[i] += data[host][cf.attr[i]];
      }
    }
  } else {
    for (var i = 0; i < cf.attr.length; i++) {
      adata[i] = data[cf.host][cf.attr[i]];
    }
  }
  
  for (var i = 0; i < adata.length; i++) {
    if (asc) ec.option.series[i].data.push(adata[i]);
    else ec.option.series[i].data.unshift(adata[i]);
  }

  // addData has bug, when dataGrow set true, it does not work
  ec.handle.setOption(ec.option, true);
}

function load_profile()
{
  return {topic: "yuntu-app-tair", id: "yuntu", attr: ["reqnum"], host: "cluster", idlist: []};
  
  $.get("/cgi-bin/de.profile.cgi", function(data) {
    
  }).fail(function() {
    alert("load profile failed");
  });
  
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

function auto_fresh(cf)
{
  var start = "";
  while (true) {
    var t = iso8601(0, "s");
    if (t == start) {
    
}

$(function() {
  var cf = load_profile();

  var start = iso8601(ago(30, "m"), "s");
  var end = iso8601(0, "s");
  
	ec = ecInit();

	var url = build_query("/cgi-bin/de.cgi",
                        {start: start, end: end, topic: cf.topic, id: cf.id});
  // console.log(url);
	var es = new EventSource(url);

	es.onmessage = function(event) {
		eval('var obj=' + event.data);
    ecfresh(cf, ec, event.lastEventId, obj, false);
	};
	es.onerror = function(event) {
		console.log("stop");
		es.close();
	}


  auto_fresh(cf);

});
