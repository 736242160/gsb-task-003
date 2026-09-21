/* Zero-dependency mesh console front-end.
 * Polls /api/state for snapshots and long-polls /api/events for the log.
 * Topology is rendered on a 2D canvas with animated traffic pulses.
 */
(function () {
  "use strict";

  var canvas = document.getElementById("topo");
  var ctx = canvas.getContext("2d");
  var logEl = document.getElementById("log");
  var statsEl = document.getElementById("global-stats");
  var tableEl = document.getElementById("node-table");
  var connPill = document.getElementById("conn-pill");
  var connText = document.getElementById("conn-text");
  var demoStatus = document.getElementById("demo-status");
  var rateSlider = document.getElementById("rate-slider");
  var rateLabel = document.getElementById("rate-label");
  var faultSelect = document.getElementById("fault-node");
  var autoScroll = document.getElementById("autoscroll");

  var state = null;
  var eventSeq = 0;
  var pulses = {};          // "src->dst" -> [{t, color}]
  var lastLayout = {};      // node id -> {x, y}
  var demoBusy = false;

  // ------------------------------------------------------------------
  // helpers
  function $(id) { return document.getElementById(id); }

  function post(payload) {
    return fetch("/api/action", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }).then(function (r) { return r.json(); });
  }

  function esc(text) {
    return String(text).replace(/[&<>"]/g, function (ch) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[ch];
    });
  }

  function clockTime(ts) {
    var d = new Date(ts * 1000);
    return d.toTimeString().slice(0, 8) + "." + String(d.getMilliseconds()).padStart(3, "0").slice(0, 2);
  }

  var STATE_COLORS = {
    CLOSED: "#34d399",
    OPEN: "#f87171",
    HALF_OPEN: "#fbbf24",
  };

  // ------------------------------------------------------------------
  // canvas sizing (HiDPI aware)
  function resizeCanvas() {
    var wrap = canvas.parentElement;
    var dpr = window.devicePixelRatio || 1;
    var w = wrap.clientWidth;
    var h = wrap.clientHeight;
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    canvas.style.width = w + "px";
    canvas.style.height = h + "px";
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  window.addEventListener("resize", resizeCanvas);

  // ------------------------------------------------------------------
  // layout
  function computeLayout(nodes) {
    var wrap = canvas.parentElement;
    var w = wrap.clientWidth;
    var h = wrap.clientHeight;
    var cols = [w * 0.12, w * 0.38, w * 0.68, w * 0.9];
    var groups = { order: [], pay: [] };
    nodes.forEach(function (n) {
      if (groups[n.service]) groups[n.service].push(n);
    });
    var layout = {};
    function place(list, x) {
      var n = list.length;
      var gap = h / (n + 1);
      list.forEach(function (node, i) {
        layout[node.id] = { x: x, y: gap * (i + 1), node: node };
      });
    }
    layout["__gateway"] = { x: cols[0], y: h / 2, node: null };
    place(groups.order, cols[1]);
    place(groups.pay, cols[2]);
    layout["__storage"] = { x: cols[3], y: h / 2, node: null };
    return layout;
  }

  function roundRect(c, x, y, w, h, r) {
    c.beginPath();
    c.moveTo(x + r, y);
    c.arcTo(x + w, y, x + w, y + h, r);
    c.arcTo(x + w, y + h, x, y + h, r);
    c.arcTo(x, y + h, x, y, r);
    c.arcTo(x, y, x + w, y, r);
    c.closePath();
  }

  // ------------------------------------------------------------------
  // pulses: keep link activity hot based on rps changes
  var prevRps = {};
  function spawnPulses(nodes) {
    nodes.forEach(function (n) {
      var prev = prevRps[n.id] || 0;
      var delta = Math.max(0, n.rps - prev);
      var hot = n.rps > prev ? 2 : (n.rps > 0 ? 1 : 0);
      if (hot) {
        var color = n.state === "OPEN" ? "#f87171"
          : n.state === "HALF_OPEN" ? "#fbbf24" : "#4da3ff";
        var gatewayLink = n.service === "order" ? "__gateway->order:" + n.id
                                                : "order-pool->" + n.id;
        var bucket = pulses[gatewayLink] || (pulses[gatewayLink] = []);
        if (Math.random() < 0.6 && bucket.length < 14) {
          bucket.push({ t: Math.random(), color: color });
        }
      }
      prevRps[n.id] = n.rps;
    });
    Object.keys(pulses).forEach(function (key) {
      var bucket = pulses[key];
      for (var i = bucket.length - 1; i >= 0; i--) {
        bucket[i].t += 0.012;
        if (bucket[i].t >= 1) bucket.splice(i, 1);
      }
    });
  }

  function drawLink(from, to, opts) {
    opts = opts || {};
    var c = ctx;
    c.save();
    c.strokeStyle = opts.color || "#2a3a52";
    c.lineWidth = opts.width || 1.4;
    if (opts.dashed) c.setLineDash([5, 5]);
    c.beginPath();
    c.moveTo(from.x, from.y);
    var midX = (from.x + to.x) / 2;
    c.bezierCurveTo(midX, from.y, midX, to.y, to.x, to.y);
    c.stroke();
    c.restore();
    return { midX: midX };
  }

  function drawPulsesOnBezier(from, to, bucket) {
    if (!bucket || !bucket.length) return;
    var c = ctx;
    var midX = (from.x + to.x) / 2;
    bucket.forEach(function (p) {
      var t = p.t;
      // cubic bezier with control points (midX,from.y),(midX,to.y)
      var mt = 1 - t;
      var x = mt * mt * mt * from.x + 3 * mt * mt * t * midX
            + 3 * mt * t * t * midX + t * t * t * to.x;
      var y = mt * mt * mt * from.y + 3 * mt * mt * t * from.y
            + 3 * mt * t * t * to.y + t * t * t * to.y;
      c.save();
      c.globalAlpha = 0.95;
      c.fillStyle = p.color;
      c.shadowColor = p.color;
      c.shadowBlur = 8;
      c.beginPath();
      c.arc(x, y, 3.2, 0, Math.PI * 2);
      c.fill();
      c.restore();
    });
  }

  function drawGateway(pos) {
    var c = ctx;
    var w = 96, h = 46;
    c.save();
    c.shadowColor = "rgba(77,163,255,.55)";
    c.shadowBlur = 16;
    roundRect(c, pos.x - w / 2, pos.y - h / 2, w, h, 9);
    var g = c.createLinearGradient(pos.x, pos.y - h / 2, pos.x, pos.y + h / 2);
    g.addColorStop(0, "#1c3a5f");
    g.addColorStop(1, "#122537");
    c.fillStyle = g;
    c.fill();
    c.lineWidth = 1.4;
    c.strokeStyle = "#4da3ff";
    c.stroke();
    c.shadowBlur = 0;
    c.fillStyle = "#dbe9ff";
    c.font = "600 12px Consolas, monospace";
    c.textAlign = "center";
    c.fillText("GATEWAY", pos.x, pos.y - 2);
    c.fillStyle = "#7f93b3";
    c.font = "10px Consolas, monospace";
    if (state) {
      var rl = state.kernel.gateway.rate_limit;
      c.fillText("lim " + rl.rate + "/s · burst " + rl.burst, pos.x, pos.y + 13);
    }
    c.restore();
  }

  function drawStorage(pos) {
    var c = ctx;
    c.save();
    c.strokeStyle = "#33415c";
    c.fillStyle = "#101826";
    c.lineWidth = 1.4;
    c.beginPath();
    c.ellipse(pos.x, pos.y - 14, 30, 9, 0, 0, Math.PI * 2);
    c.fill(); c.stroke();
    c.beginPath();
    c.moveTo(pos.x - 30, pos.y - 14);
    c.lineTo(pos.x - 30, pos.y + 14);
    c.ellipse(pos.x, pos.y + 14, 30, 9, 0, Math.PI, Math.PI * 2);
    c.lineTo(pos.x + 30, pos.y - 14);
    c.fill(); c.stroke();
    c.fillStyle = "#64748b";
    c.font = "10px Consolas, monospace";
    c.textAlign = "center";
    c.fillText("downstream", pos.x, pos.y + 34);
    c.restore();
  }

  function drawNode(pos) {
    var n = pos.node;
    var c = ctx;
    var w = 138, h = 66;
    var x = pos.x - w / 2, y = pos.y - h / 2;
    var color = n.offline ? "#4b5563" : STATE_COLORS[n.state] || "#34d399";

    c.save();
    // state glow
    if (!n.offline && n.state !== "CLOSED") {
      c.shadowColor = color;
      c.shadowBlur = 18;
    }
    roundRect(c, x, y, w, h, 9);
    c.fillStyle = n.offline ? "#161b24" : "#121b29";
    c.fill();
    c.lineWidth = 1.6;
    c.strokeStyle = color;
    c.stroke();
    c.shadowBlur = 0;

    // status chip
    c.fillStyle = color;
    roundRect(c, x + 8, y + 8, 62, 17, 4);
    c.globalAlpha = 0.18;
    c.fill();
    c.globalAlpha = 1;
    c.fillStyle = color;
    c.font = "700 9.5px Consolas, monospace";
    c.textAlign = "center";
    c.fillText(n.offline ? "OFFLINE" : n.state, x + 39, y + 20);

    // health bar
    c.textAlign = "left";
    c.fillStyle = "#7f93b3";
    c.font = "9.5px Consolas, monospace";
    c.fillText("health", x + 76, y + 16);
    c.fillStyle = "#cdd9ee";
    c.fillText(Math.round(n.health) + "%", x + 112, y + 16);
    c.fillStyle = "#223048";
    c.fillRect(x + 8, y + 30, w - 16, 4);
    var hc = n.health > 60 ? "#34d399" : n.health > 30 ? "#fbbf24" : "#f87171";
    c.fillStyle = hc;
    c.fillRect(x + 8, y + 30, (w - 16) * Math.max(0, n.health) / 100, 4);

    // name + metrics
    c.fillStyle = "#e4ecf8";
    c.font = "700 12px Consolas, monospace";
    c.fillText(n.id, x + 8, y + 48);
    c.fillStyle = "#8593a9";
    c.font = "9.5px Consolas, monospace";
    var detail = n.ewma_latency + "ms";
    if (n.injected_delay) detail += " +" + n.injected_delay + "ms";
    c.fillText(detail, x + 8, y + 60);
    c.textAlign = "right";
    c.fillStyle = "#4da3ff";
    c.font = "700 12px Consolas, monospace";
    c.fillText(n.rps + " rps", x + w - 8, y + 48);
    c.fillStyle = "#8593a9";
    c.font = "9.5px Consolas, monospace";
    c.fillText("inflight " + n.inflight, x + w - 8, y + 60);
    c.restore();
  }

  function drawTierLabel(text, x, y, color) {
    ctx.save();
    ctx.fillStyle = color || "#5a6a85";
    ctx.font = "700 10px Consolas, monospace";
    ctx.textAlign = "left";
    ctx.fillText(text, x - 30, y);
    ctx.restore();
  }

  // ------------------------------------------------------------------
  function render() {
    var wrap = canvas.parentElement;
    ctx.clearRect(0, 0, wrap.clientWidth, wrap.clientHeight);
    if (!state) return;
    var nodes = state.kernel.nodes;
    var layout = computeLayout(nodes);
    lastLayout = layout;
    spawnPulses(nodes);

    var gw = layout["__gateway"];
    var storage = layout["__storage"];
    var orderNodes = nodes.filter(function (n) { return n.service === "order"; });
    var payNodes = nodes.filter(function (n) { return n.service === "pay"; });

    // tier labels
    drawTierLabel("ENTRY", gw.x - 20, 26);
    drawTierLabel("SERVICE: order", orderNodes.length ? layout[orderNodes[0].id].x - 40 : 0, 26);
    drawTierLabel("SERVICE: pay", payNodes.length ? layout[payNodes[0].id].x - 40 : 0, 26);

    // links gateway -> order
    orderNodes.forEach(function (n) {
      var p = layout[n.id];
      var dead = n.offline || n.state === "OPEN";
      var half = n.state === "HALF_OPEN";
      drawLink(gw, p, {
        color: n.offline ? "#2b3242" : dead ? "#5a2b32" : half ? "#7a5d1c" : "#27405f",
        dashed: n.offline || dead,
        width: dead ? 1 : 1.5,
      });
      drawPulsesOnBezier(gw, p, pulses["__gateway->order:" + n.id]);
    });

    // mesh links order-pool -> pay nodes (fan-in from each order node for visual mesh)
    payNodes.forEach(function (pn) {
      var target = layout[pn.id];
      var dead = pn.offline || pn.state === "OPEN";
      var half = pn.state === "HALF_OPEN";
      orderNodes.forEach(function (on) {
        drawLink(layout[on.id], target, {
          color: pn.offline ? "#222836" : dead ? "#4a252b" : half ? "#5f4a1a" : "#22344d",
          dashed: pn.offline || dead,
          width: 1,
        });
      });
      drawPulsesOnBezier(
        { x: orderNodes.reduce(function (s, o) { return s + layout[o.id].x; }, 0) / orderNodes.length,
          y: orderNodes.reduce(function (s, o) { return s + layout[o.id].y; }, 0) / orderNodes.length },
        target, pulses["order-pool->" + pn.id]);
    });

    // links pay -> storage
    payNodes.forEach(function (pn) {
      drawLink(layout[pn.id], storage, {
        color: pn.offline ? "#222836" : "#2a3a52",
        dashed: pn.offline,
      });
    });

    drawStorage(storage);
    drawGateway(gw);
    nodes.forEach(function (n) { drawNode(layout[n.id]); });
  }

  // ------------------------------------------------------------------
  // HTML updates
  function metric(k, v, cls) {
    return '<div class="metric"><div class="k">' + k + '</div>' +
           '<div class="v ' + (cls || "") + '">' + v + "</div></div>";
  }

  function updateStats() {
    var g = state.kernel.gateway;
    var d = state.demo;
    var cls = g.failed_rps + g.rejected_rps > 0 ? "red" : "green";
    statsEl.innerHTML =
      metric("网关吞吐", g.rps + " rps", "accent") +
      metric("成功", g.ok_rps + " rps", "green") +
      metric("失败/降级", g.failed_rps + " rps", cls) +
      metric("限流削峰", g.rejected_rps + " rps", g.rejected_rps ? "amber" : "") +
      metric("在途请求", g.inflight, "") +
      metric("累计请求", g.total, "");
    demoStatus.textContent = d.active
      ? "演示进行中 [" + d.active + "] " + d.elapsed.toFixed(1) + "s · " + d.message
      : "就绪";
    var busy = !!d.active;
    demoBusy = busy;
    $("btn-demo-burst").disabled = busy;
    $("btn-demo-breaker").disabled = busy;
  }

  function updateNodeTable() {
    var rows = state.kernel.nodes.map(function (n) {
      var color = n.offline ? "#4b5563" : STATE_COLORS[n.state];
      var sub = n.host + " · w=" + n.weight + (n.injected_delay ? " · FAULT +" + n.injected_delay + "ms" : "");
      return '<div class="node-row">' +
        '<span class="node-dot" style="background:' + color + ';box-shadow:0 0 6px ' + color + '"></span>' +
        '<div class="node-meta"><div class="node-name">' + esc(n.id) +
          ' <span style="color:' + color + '">' + (n.offline ? "OFFLINE" : n.state) + "</span></div>" +
        '<div class="node-sub">' + esc(sub) + "</div></div>" +
        '<div class="node-rps"><div class="r">' + n.rps + '</div>' +
        '<div class="h">h:' + Math.round(n.health) + "%</div></div></div>";
    });
    tableEl.innerHTML = rows.join("");
  }

  function populateFaultSelect() {
    if (!state) return;
    var current = faultSelect.value;
    faultSelect.innerHTML = state.kernel.nodes.map(function (n) {
      return '<option value="' + n.id + '">' + n.id + " (" + n.host + ")</option>";
    }).join("");
    if (current) faultSelect.value = current;
  }

  // ------------------------------------------------------------------
  // event log
  var LEVEL_LABEL = {
    DEBUG: "DEBUG", INFO: "INFO", OK: "OK", WARN: "WARN",
    ERROR: "ERROR", REJECT: "REJECT",
  };

  function appendEvents(events) {
    var frag = document.createDocumentFragment();
    events.forEach(function (e) {
      var line = document.createElement("div");
      line.className = "log-line log-" + e.level;
      var label = LEVEL_LABEL[e.level] || e.level;
      line.innerHTML =
        '<span class="ts">' + clockTime(e.ts) + "</span>" +
        '<span class="lv">' + label.padEnd(6, " ") + "</span>" +
        '<span class="msg">' + esc(e.message) + "</span>";
      frag.appendChild(line);
    });
    var atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 24;
    logEl.appendChild(frag);
    // cap DOM nodes
    while (logEl.childNodes.length > 600) logEl.removeChild(logEl.firstChild);
    if (autoScroll.checked && atBottom) logEl.scrollTop = logEl.scrollHeight;
    else if (autoScroll.checked) logEl.scrollTop = logEl.scrollHeight;
  }

  function pollEvents() {
    fetch("/api/events?after=" + eventSeq)
      .then(function (r) { return r.json(); })
      .then(function (data) {
        eventSeq = data.latest;
        if (data.events && data.events.length) appendEvents(data.events);
        setConn(true);
        pollEvents();
      })
      .catch(function () {
        setConn(false);
        setTimeout(pollEvents, 1500);
      });
  }

  function setConn(ok) {
    connPill.className = "status-pill " + (ok ? "online" : "offline");
    connText.textContent = ok ? "内核已连接" : "连接断开，重连中…";
  }

  // ------------------------------------------------------------------
  // state polling
  function pollState() {
    fetch("/api/state")
      .then(function (r) { return r.json(); })
      .then(function (data) {
        state = data;
        if (!faultSelect.options.length) populateFaultSelect();
        updateStats();
        updateNodeTable();
        pollState();
      })
      .catch(function () { setTimeout(pollState, 1000); });
  }

  // ------------------------------------------------------------------
  // controls
  function bindButtons() {
    $("btn-demo-burst").addEventListener("click", function () {
      post({ action: "demo", scenario: "burst" });
    });
    $("btn-demo-breaker").addEventListener("click", function () {
      post({ action: "demo", scenario: "breaker" });
    });
    $("btn-burst").addEventListener("click", function () {
      post({ action: "burst", count: 140 });
    });
    $("btn-stop").addEventListener("click", function () {
      rateSlider.value = 0;
      rateLabel.textContent = "0";
      post({ action: "traffic", rate: 0 });
    });
    rateSlider.addEventListener("input", function () {
      rateLabel.textContent = rateSlider.value;
      post({ action: "traffic", rate: Number(rateSlider.value) });
    });
    $("btn-latency").addEventListener("click", function () {
      post({ action: "inject_latency", node: faultSelect.value, delay: 0.65 });
    });
    $("btn-clear-latency").addEventListener("click", function () {
      post({ action: "inject_latency", node: faultSelect.value, delay: 0 });
    });
    $("btn-fault").addEventListener("click", function () {
      post({ action: "inject_fault", node: faultSelect.value, probability: 0.5 });
    });
    $("btn-clear-fault").addEventListener("click", function () {
      post({ action: "inject_fault", node: faultSelect.value, probability: 0 });
    });
    $("btn-offline").addEventListener("click", function () {
      post({ action: "offline", node: faultSelect.value, offline: true });
    });
    $("btn-online").addEventListener("click", function () {
      post({ action: "offline", node: faultSelect.value, offline: false });
    });
    $("btn-reset").addEventListener("click", function () {
      post({ action: "reset" }).then(function () {
        rateSlider.value = 0;
        rateLabel.textContent = "0";
        prevRps = {};
        pulses = {};
      });
    });
    $("btn-clear-log").addEventListener("click", function () {
      logEl.innerHTML = "";
    });
  }

  // ------------------------------------------------------------------
  // boot
  resizeCanvas();
  bindButtons();
  pollState();
  pollEvents();
  (function frame() {
    render();
    requestAnimationFrame(frame);
  })();
})();
