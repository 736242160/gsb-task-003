/* ==========================================================================
 * app.js  —  纯原生 JavaScript (无任何第三方库 / CDN)
 *
 *  - Canvas 绘制网关 -> 五实例的拓扑网络, 边宽=动态权重, 边色=断路器状态
 *  - SSE 实时接收治理事件, 路由事件转化为链路上的流动粒子
 *  - 200ms 快照轮询刷新健康度/吞吐/节点面板
 * ========================================================================== */

(function () {
  "use strict";

  // ---------------- 全局状态 ----------------

  var latestSnapshot = null;
  var nodesById = {};           // id -> 快照节点 (随轮询更新)
  var edgeFlash = {};           // nodeId -> 最近一次命中时间戳(ms)
  var particles = [];           // 链路流动粒子
  var burstParticles = [];      // 熔断/限流爆裂粒子
  var gateFlash = 0;            // 网关限流闪光
  var lastSeq = 0;
  var logLines = 0;

  var canvas = document.getElementById("topology");
  var ctx = canvas.getContext("2d");
  var canvasCssW = 800;
  var canvasCssH = 460;
  var DPR = Math.max(1, window.devicePixelRatio || 1);

  // 逻辑坐标系尺寸 (与 CSS 像素 1:1, 再整体乘 DPR)
  var LW = 1000;
  var LH = 460;
  var GATE = { x: 86, y: 230 };

  var STATE_COLORS = {
    closed: "#34d399",
    half_open: "#60a5fa",
    open: "#f87171"
  };
  var OUTCOME_COLORS = {
    success: "#34d399",
    timeout: "#fbbf24",
    error: "#f87171"
  };

  // ---------------- 工具 ----------------

  function $(id) { return document.getElementById(id); }
  function nowMs() { return performance.now(); }
  function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }

  function healthColor(h) {
    // 0 红 -> 0.5 黄 -> 1 绿
    var r, g;
    if (h < 0.5) {
      r = 248; g = Math.round(113 + (190 - 113) * (h / 0.5));
    } else {
      r = Math.round(248 - (248 - 52) * ((h - 0.5) / 0.5));
      g = 190 + Math.round((211 - 190) * ((h - 0.5) / 0.5));
    }
    return "rgb(" + r + "," + g + ",120)";
  }

  function postAction(body) {
    return fetch("/api/action", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    }).then(function (r) { return r.json(); }).catch(function () { return { ok: false }; });
  }

  // ---------------- Canvas 尺寸自适应 ----------------

  function resizeCanvas() {
    var rect = canvas.getBoundingClientRect();
    canvasCssW = rect.width || 800;
    canvasCssH = rect.height || 460;
    canvas.width = Math.round(canvasCssW * DPR);
    canvas.height = Math.round(canvasCssH * DPR);
  }
  window.addEventListener("resize", resizeCanvas);

  function nodePos(node) {
    return { x: 190 + node.x * 720, y: 58 + node.y * 350 };
  }

  // ---------------- 粒子 ----------------

  function spawnRouteParticle(ev) {
    if (!nodesById[ev.data.node]) return;
    if (particles.length > 220) particles.shift();
    particles.push({
      node: ev.data.node,
      t: Math.random() * 0.08,
      speed: 0.55 + Math.random() * 0.45,
      outcome: ev.data.outcome,
      trial: !!ev.data.trial
    });
  }

  function spawnBurst(x, y, color, count) {
    for (var i = 0; i < count; i++) {
      var ang = Math.random() * Math.PI * 2;
      var spd = 40 + Math.random() * 90;
      burstParticles.push({
        x: x, y: y,
        vx: Math.cos(ang) * spd,
        vy: Math.sin(ang) * spd,
        life: 0.5 + Math.random() * 0.3,
        age: 0,
        color: color
      });
    }
    if (burstParticles.length > 300) {
      burstParticles.splice(0, burstParticles.length - 300);
    }
  }

  // ---------------- 绘制 ----------------

  function drawBackground() {
    // 网格底纹
    ctx.fillStyle = "#0a1024";
    ctx.fillRect(0, 0, LW, LH);
    ctx.strokeStyle = "rgba(110,130,200,0.07)";
    ctx.lineWidth = 1;
    for (var gx = 0; gx <= LW; gx += 40) {
      ctx.beginPath(); ctx.moveTo(gx, 0); ctx.lineTo(gx, LH); ctx.stroke();
    }
    for (var gy = 0; gy <= LH; gy += 40) {
      ctx.beginPath(); ctx.moveTo(0, gy); ctx.lineTo(LW, gy); ctx.stroke();
    }
  }

  function drawServiceGroups(nodes) {
    var groups = {};
    nodes.forEach(function (n) {
      var p = nodePos(n);
      if (!groups[n.service]) groups[n.service] = { minX: p.x, maxX: p.x, minY: p.y, maxY: p.y, label: n.service };
      var g = groups[n.service];
      g.minX = Math.min(g.minX, p.x); g.maxX = Math.max(g.maxX, p.x);
      g.minY = Math.min(g.minY, p.y); g.maxY = Math.max(g.maxY, p.y);
    });
    Object.keys(groups).forEach(function (key) {
      var g = groups[key];
      var x = g.minX - 62, y = g.minY - 58, w = g.maxX - g.minX + 124, h = g.maxY - g.minY + 110;
      ctx.save();
      ctx.strokeStyle = "rgba(124,156,255,0.28)";
      ctx.setLineDash([6, 6]);
      ctx.lineWidth = 1.2;
      roundRect(x, y, w, h, 16);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "rgba(150,170,255,0.75)";
      ctx.font = "600 11px Segoe UI";
      var label = key === "order-service" ? "订单服务集群" : "支付服务集群";
      ctx.fillText(label, x + 14, y + 16);
      ctx.restore();
    });
  }

  function roundRect(x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x, y + h, r);
    ctx.arcTo(x, y + h, x, y, r);
    ctx.arcTo(x, y, x + w, y, r);
    ctx.closePath();
  }

  function drawEdges(nodes, timeMs) {
    nodes.forEach(function (n) {
      var p = nodePos(n);
      var state = n.breaker.state;
      var color = STATE_COLORS[state] || "#34d399";
      var flash = edgeFlash[n.id] ? Math.max(0, 1 - (timeMs - edgeFlash[n.id]) / 900) : 0;
      var baseAlpha = state === "open" ? 0.5 : 0.28 + flash * 0.5;
      var width = state === "open" ? 1.4 : clamp(0.9 + n.weight / 3.2, 1, 6.5) + flash * 1.6;

      ctx.save();
      ctx.strokeStyle = color;
      ctx.globalAlpha = baseAlpha;
      ctx.lineWidth = width;
      if (state === "open") ctx.setLineDash([7, 7]);
      ctx.beginPath();
      ctx.moveTo(GATE.x + 26, GATE.y);
      ctx.quadraticCurveTo((GATE.x + p.x) / 2, (GATE.y + p.y) / 2 - 26, p.x, p.y);
      ctx.stroke();
      ctx.restore();

      // OPEN 链路中央标注
      if (state === "open") {
        var mx = (GATE.x + 2 * (GATE.x + p.x) / 2 + p.x) / 2;
        var my = (GATE.y + 2 * ((GATE.y + p.y) / 2 - 26) + p.y) / 2;
        ctx.save();
        ctx.fillStyle = "rgba(248,113,113,0.95)";
        ctx.font = "700 10.5px Consolas";
        ctx.fillText("流量切除 ✕", mx - 26, my - 6);
        ctx.font = "10px Consolas";
        ctx.fillStyle = "rgba(255,150,150,0.8)";
        if (n.breaker.cooldown_remaining > 0) {
          ctx.fillText("冷却 " + n.breaker.cooldown_remaining.toFixed(1) + "s", mx - 18, my + 10);
        }
        ctx.restore();
      } else if (state === "half_open") {
        var hx = (GATE.x + p.x) / 2;
        var hy = (GATE.y + p.y) / 2 - 40;
        ctx.save();
        ctx.fillStyle = "rgba(96,165,250,0.95)";
        ctx.font = "700 10.5px Consolas";
        ctx.fillText("半开试探 ↑", hx - 26, hy);
        ctx.restore();
      }
    });
  }

  function drawParticles(dt, nodes) {
    // 链路粒子沿二次贝塞尔曲线移动
    var alive = [];
    particles.forEach(function (pt) {
      pt.t += dt * pt.speed;
      var n = nodesById[pt.node];
      if (!n || pt.t > 1) return;
      var p = nodePos(n);
      var qx = (GATE.x + p.x) / 2;
      var qy = (GATE.y + p.y) / 2 - 26;
      var u = 1 - pt.t;
      var x = u * u * (GATE.x + 26) + 2 * u * pt.t * qx + pt.t * pt.t * p.x;
      var y = u * u * GATE.y + 2 * u * pt.t * qy + pt.t * pt.t * p.y;
      var color = pt.trial ? "#9ec2ff" : OUTCOME_COLORS[pt.outcome];

      ctx.save();
      ctx.shadowColor = color;
      ctx.shadowBlur = pt.trial ? 14 : 8;
      ctx.fillStyle = color;
      if (pt.trial) {
        ctx.beginPath();
        ctx.moveTo(x, y - 6); ctx.lineTo(x + 6, y); ctx.lineTo(x, y + 6); ctx.lineTo(x - 6, y);
        ctx.closePath(); ctx.fill();
      } else {
        ctx.beginPath();
        ctx.arc(x, y, pt.outcome === "success" ? 3.2 : 3.8, 0, Math.PI * 2);
        ctx.fill();
      }
      ctx.restore();
      alive.push(pt);
    });
    particles = alive;

    var aliveBurst = [];
    burstParticles.forEach(function (b) {
      b.age += dt;
      if (b.age >= b.life) return;
      b.x += b.vx * dt;
      b.y += b.vy * dt;
      ctx.save();
      ctx.globalAlpha = 1 - b.age / b.life;
      ctx.strokeStyle = b.color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(b.x - b.vx * 0.03, b.y - b.vy * 0.03);
      ctx.lineTo(b.x, b.y);
      ctx.stroke();
      ctx.restore();
      aliveBurst.push(b);
    });
    burstParticles = aliveBurst;
  }
  function drawGateway(timeMs) {
    var pulsing = Math.max(0, 1 - (timeMs - gateFlash) / 700);
    ctx.save();
    // 网关主体
    ctx.shadowColor = pulsing > 0 ? "#fbbf24" : "#7c9cff";
    ctx.shadowBlur = 16 + pulsing * 26;
    var grad = ctx.createLinearGradient(GATE.x - 26, GATE.y - 26, GATE.x + 26, GATE.y + 26);
    grad.addColorStop(0, pulsing > 0 ? "#8a6d1f" : "#3d5afe");
    grad.addColorStop(1, pulsing > 0 ? "#fbbf24" : "#34d399");
    ctx.fillStyle = grad;
    roundRect(GATE.x - 26, GATE.y - 26, 52, 52, 12);
    ctx.fill();
    ctx.shadowBlur = 0;
    ctx.strokeStyle = "rgba(255,255,255,0.6)";
    ctx.lineWidth = 1.4;
    ctx.stroke();

    ctx.fillStyle = "#fff";
    ctx.font = "700 12px Segoe UI";
    ctx.textAlign = "center";
    ctx.fillText("Mesh", GATE.x, GATE.y - 2);
    ctx.font = "10px Segoe UI";
    ctx.fillText("Gateway", GATE.x, GATE.y + 12);
    ctx.textAlign = "left";

    if (latestSnapshot) {
      ctx.font = "10px Consolas";
      ctx.fillStyle = "rgba(255,224,138,0.95)";
      ctx.fillText("令牌桶 " + latestSnapshot.tokens + "/" + latestSnapshot.burst_capacity,
        GATE.x - 34, GATE.y + 44);
    }
    ctx.restore();
  }

  function drawNode(n, timeMs) {
    var p = nodePos(n);
    var state = n.breaker.state;
    var color = STATE_COLORS[state];
    var hitAge = n.last_hit_age;
    var hitPulse = hitAge < 0.35 ? 1 - hitAge / 0.35 : 0;

    ctx.save();
    // 外圈光晕
    ctx.shadowColor = color;
    ctx.shadowBlur = state === "open" ? 26 : 12 + hitPulse * 18;
    ctx.fillStyle = state === "open" ? "rgba(60,18,24,0.95)" : "rgba(18,28,56,0.95)";
    ctx.beginPath();
    ctx.arc(p.x, p.y, 27, 0, Math.PI * 2);
    ctx.fill();
    ctx.shadowBlur = 0;
    ctx.lineWidth = state === "closed" ? 2 : 2.6;
    ctx.strokeStyle = color;
    ctx.stroke();

    // 健康度环
    ctx.beginPath();
    ctx.arc(p.x, p.y, 27, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * clamp(n.health, 0, 1));
    ctx.strokeStyle = healthColor(n.health);
    ctx.lineWidth = 3.4;
    ctx.stroke();

    // 在途并发小指示
    if (n.active_calls > 0) {
      ctx.fillStyle = "rgba(157,240,207,0.95)";
      ctx.font = "700 11px Consolas";
      ctx.textAlign = "center";
      ctx.fillText(String(n.active_calls), p.x, p.y - 3);
    }

    // 故障标记
    if (n.inject_delay_ms > 0 || n.force_error || n.offline) {
      ctx.fillStyle = "#fbbf24";
      ctx.beginPath();
      ctx.moveTo(p.x + 21, p.y - 21);
      ctx.arc(p.x + 21, p.y - 21, 8, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = "#3a2a05";
      ctx.font = "800 11px Segoe UI";
      ctx.fillText("!", p.x + 21, p.y - 17);
    }

    // 名称与指标
    ctx.textAlign = "center";
    ctx.fillStyle = "#dbe4ff";
    ctx.font = "700 11.5px Segoe UI";
    ctx.fillText(n.name, p.x, p.y + 44);
    ctx.fillStyle = state === "open" ? "#ff9ca2" : (state === "half_open" ? "#9ec2ff" : "#8fa1d4");
    ctx.font = "10px Consolas";
    var sub = state === "open"
      ? "OPEN 熔断中 " + n.breaker.cooldown_remaining.toFixed(1) + "s"
      : state === "half_open"
        ? "HALF-OPEN 探针 " + n.breaker.trials_completed
        : "w=" + n.weight + "  " + n.latency_ms.toFixed(0) + "ms  " + n.ok_rps + " rps";
    ctx.fillText(sub, p.x, p.y + 58);
    ctx.textAlign = "left";
    ctx.restore();
  }

  var lastFrame = nowMs();

  function frame() {
    var timeMs = nowMs();
    var dt = Math.min(0.05, (timeMs - lastFrame) / 1000);
    lastFrame = timeMs;

    ctx.setTransform(DPR * (canvasCssW / LW), 0, 0, DPR * (canvasCssH / LH), 0, 0);
    drawBackground();

    var nodes = latestSnapshot ? latestSnapshot.nodes : [];
    if (nodes.length) {
      drawServiceGroups(nodes);
      drawEdges(nodes, timeMs);
      drawParticles(dt, nodes);
      drawGateway(timeMs);
      nodes.forEach(function (n) { drawNode(n, timeMs); });
    } else {
      drawGateway(timeMs);
      ctx.fillStyle = "rgba(180,195,255,0.7)";
      ctx.font = "14px Segoe UI";
      ctx.fillText("等待内核状态…", LW / 2 - 60, LH / 2);
    }
    requestAnimationFrame(frame);
  }
  // ---------------- 右侧指标 & 节点卡片 ----------------

  var STATE_LABEL = { closed: "CLOSED 正常", half_open: "HALF-OPEN 半开", open: "OPEN 熔断" };

  function renderStats(s) {
    $("statRps").textContent = s.measured_rps;
    $("statOk").textContent = s.ok_rps;
    $("statStatTimeout").textContent = s.timeout_rps;
    $("statThrottle").textContent = s.throttle_rps;
    $("statReject").textContent = s.reject_rps;
    $("statConcurrency").textContent = s.active_calls + "/" + s.max_concurrency;
  }

  var cardRefs = {};

  function buildNodeCards(nodes) {
    var box = $("nodeCards");
    box.innerHTML = "";
    cardRefs = {};
    nodes.forEach(function (n) {
      var card = document.createElement("div");
      card.className = "node-card state-closed";
      card.id = "card-" + n.id;
      card.innerHTML =
        '<div class="node-row1">' +
          '<span><span class="node-name"></span><span class="node-svc"></span></span>' +
          '<span class="badge closed"></span>' +
        "</div>" +
        '<div class="health-bar"><i style="width:100%"></i></div>' +
        '<div class="node-metrics">' +
          '<div class="metric"><b class="m-rps"></b><span>RPS</span></div>' +
          '<div class="metric"><b class="m-lat"></b><span>延迟 ms</span></div>' +
          '<div class="metric"><b class="m-weight"></b><span>动态权重</span></div>' +
          '<div class="metric"><b class="m-health"></b><span>健康度</span></div>' +
        "</div>" +
        '<div class="node-note"></div>';
      box.appendChild(card);
      cardRefs[n.id] = {
        card: card,
        name: card.querySelector(".node-name"),
        svc: card.querySelector(".node-svc"),
        badge: card.querySelector(".badge"),
        bar: card.querySelector(".health-bar i"),
        rps: card.querySelector(".m-rps"),
        lat: card.querySelector(".m-lat"),
        weight: card.querySelector(".m-weight"),
        health: card.querySelector(".m-health"),
        note: card.querySelector(".node-note")
      };
    });
  }

  function renderNodeCards(nodes, timeMs) {
    nodes.forEach(function (n) {
      var refs = cardRefs[n.id];
      if (!refs) return;
      refs.name.textContent = n.name;
      refs.svc.textContent = n.service;
      refs.badge.textContent = STATE_LABEL[n.breaker.state] || n.breaker.state;
      refs.badge.className = "badge " + n.breaker.state;
      refs.card.className = "node-card state-" + n.breaker.state;
      refs.bar.style.width = Math.round(n.health * 100) + "%";
      refs.bar.style.background = healthColor(n.health);
      refs.rps.textContent = n.ok_rps;
      refs.rps.style.color = n.timeout + n.error > 0 ? "#fbbf24" : "";
      refs.lat.textContent = n.latency_ms.toFixed(0);
      refs.weight.textContent = n.weight;
      refs.health.textContent = Math.round(n.health * 100) + "%";

      var notes = [];
      if (n.offline) notes.push("⚠ 节点已离线");
      if (n.force_error) notes.push("⚠ 强制异常注入");
      if (n.inject_delay_ms > 0) notes.push("⚠ 延迟注入 +" + n.inject_delay_ms.toFixed(0) + "ms");
      if (n.breaker.state === "open")
        notes.push("熔断快速失败 · 冷却剩 " + n.breaker.cooldown_remaining.toFixed(1) + "s · 累计熔断 " + n.breaker.open_count + " 次");
      if (n.breaker.state === "half_open")
        notes.push("半开试探中 · 探针 " + n.breaker.consecutive_successes + " 次成功");
      if (n.breaker.error_rate !== null && n.breaker.state === "closed")
        notes.push("窗口错误率 " + (n.breaker.error_rate * 100).toFixed(0) + "%");
      refs.note.textContent = notes.join("　");

      // 命中闪一下
      if (n.last_hit_age < 0.12 && refs.card._lastFlash !== n.last_hit_age) {
        refs.card._lastFlash = n.last_hit_age;
        refs.card.classList.remove("flash");
        void refs.card.offsetWidth;
        refs.card.classList.add("flash");
      }
    });
  }

  function populateNodeSelect(nodes) {
    var sel = $("nodeSelect");
    if (sel.options.length === nodes.length) return;
    sel.innerHTML = "";
    nodes.forEach(function (n, i) {
      var opt = document.createElement("option");
      opt.value = n.id;
      opt.textContent = n.name + " (" + n.id + ")";
      if (n.id === "order-3") opt.selected = true;
      sel.appendChild(opt);
    });
  }

  // ---------------- 日志 ----------------

  var TAG_LABEL = {
    breaker: "断路器",
    throttle: "限流",
    reject: "熔断拦截",
    fault: "故障注入",
    demo: "演示",
    control: "流量控制",
    route: "路由",
    kernel: "内核",
    console: "控制台"
  };

  var REASON_LABEL = {
    consecutive_failures: "连续失败越限",
    error_rate: "错误率越限",
    trial_failed: "半开探针失败",
    cooldown_elapsed: "冷却窗口结束",
    trials_succeeded: "试探连续成功"
  };

  function addLog(ev) {
    if (ev.kind === "route") return;  // 路由事件用粒子表达, 不刷日志
    var box = $("logConsole");
    var line = document.createElement("div");
    line.className = "log-line kind-" + ev.kind;

    var ts = document.createElement("span");
    ts.className = "log-ts";
    var t = ev.ts.toFixed(2);
    ts.textContent = "+" + (t.length < 5 ? "0" + t : t) + "s";

    var tag = document.createElement("span");
    tag.className = "log-tag";
    tag.textContent = TAG_LABEL[ev.kind] || ev.kind;

    var msg = document.createElement("span");
    msg.className = "log-msg";
    msg.textContent = formatEvent(ev);

    line.appendChild(ts);
    line.appendChild(tag);
    line.appendChild(msg);
    box.appendChild(line);
    logLines++;
    if (logLines > 250) {
      box.removeChild(box.firstChild);
      logLines--;
    }
    if ($("followLog").checked) box.scrollTop = box.scrollHeight;
  }

  function nodeName(id) {
    return nodesById[id] ? nodesById[id].name : id;
  }

  function formatEvent(ev) {
    var d = ev.data;
    switch (ev.kind) {
      case "breaker": {
        var stateText = { open: "OPEN 开启熔断", half_open: "HALF_OPEN 进入半开", closed: "CLOSED 恢复关闭" }[d.state] || d.state;
        var reason = String(d.reason || "").split(":")[0];
        var reasonText = REASON_LABEL[reason] || d.reason || "";
        var extra = d.error_rate !== null && d.error_rate !== undefined ? " (错误率 " + (d.error_rate * 100).toFixed(0) + "%)" : "";
        return nodeName(d.node) + "  →  " + stateText + " · " + reasonText + extra;
      }
      case "throttle":
        return "令牌桶耗尽, 请求快速失败 (已限流丢弃 #" + d.count + ")";
      case "reject":
        return "全部节点熔断/离线, 请求被快速失败拦截 (#" + d.count + ") — 防止级联雪崩";
      case "fault": {
        var what = d.offline ? "离线" : d.force_error ? "强制异常" :
          d.inject_delay > 0 ? "延迟 +" + Math.round(d.inject_delay * 1000) + "ms" : "故障解除, 恢复正常";
        return nodeName(d.node) + "  " + what;
      }
      case "control":
        if (d.action === "set_traffic") return "常态流量设置为 " + d.rps + " RPS";
        if (d.action === "burst_start") return "突发洪峰 " + d.rps + " RPS, 持续 " + d.duration + "s — 令牌桶削峰";
        if (d.action === "burst_end") return "洪峰退去, 恢复 " + d.rps + " RPS";
        if (d.action === "reset_breakers") return "全部断路器与健康指标已复位";
        return JSON.stringify(d);
      case "demo":
        return "◆ " + d.phase;
      case "kernel":
      case "console":
        return d.msg || "";
      default:
        return JSON.stringify(d);
    }
  }
  // ---------------- SSE 事件流 ----------------

  function setConn(online) {
    var el = $("connState");
    el.textContent = online ? "● 实时事件流已连接" : "○ 事件流断开, 重连中…";
    el.className = online ? "online" : "offline";
  }

  function connectSSE() {
    var es = new EventSource("/api/events?after=" + lastSeq);
    es.addEventListener("mesh", function (e) {
      var ev;
      try { ev = JSON.parse(e.data); } catch (err) { return; }
      lastSeq = ev.seq;
      handleEvent(ev);
    });
    es.onopen = function () { setConn(true); };
    es.onerror = function () { setConn(false); };
  }

  function handleEvent(ev) {
    addLog(ev);

    if (ev.kind === "route") {
      spawnRouteParticle(ev);
      edgeFlash[ev.data.node] = nowMs();
      if (ev.data.outcome !== "success") {
        var n = nodesById[ev.data.node];
        if (n) {
          var p = nodePos(n);
          spawnBurst(p.x, p.y, OUTCOME_COLORS[ev.data.outcome] || "#f87171", 7);
        }
      }
    } else if (ev.kind === "throttle") {
      gateFlash = nowMs();
      spawnBurst(GATE.x - 10, GATE.y + 34, "#fbbf24", 14);
    } else if (ev.kind === "reject") {
      gateFlash = nowMs();
      spawnBurst(GATE.x - 10, GATE.y + 34, "#f87171", 18);
    } else if (ev.kind === "breaker") {
      var node = nodesById[ev.data.node];
      if (node) {
        var pos = nodePos(node);
        var color = ev.data.state === "closed" ? "#34d399" :
          ev.data.state === "half_open" ? "#60a5fa" : "#f87171";
        spawnBurst(pos.x, pos.y, color, ev.data.state === "open" ? 22 : 14);
      }
    } else if (ev.kind === "demo") {
      var phase = ev.data.phase || "";
      var banner = $("demoBanner");
      if (phase === "full-demo-done" || phase === "circuit-demo-done" ||
          phase === "normal-demo-done" || phase === "cancelled") {
        banner.classList.remove("active");
        banner.textContent = "";
      } else {
        banner.textContent = "演示进行中: " + phase;
        banner.classList.add("active");
      }
    }
  }

  // ---------------- 快照轮询 ----------------

  function refreshState() {
    fetch("/api/state", { cache: "no-store" })
      .then(function (r) { return r.json(); })
      .then(function (s) {
        latestSnapshot = s;
        s.nodes.forEach(function (n) { nodesById[n.id] = n; });
        if (!$("nodeCards").firstChild || Object.keys(cardRefs).length !== s.nodes.length) {
          buildNodeCards(s.nodes);
        }
        renderStats(s);
        renderNodeCards(s.nodes, nowMs());
        populateNodeSelect(s.nodes);
      })
      .catch(function () { /* 下轮重试 */ });
  }

  // ---------------- 控制绑定 ----------------

  function selectedNode() { return $("nodeSelect").value || "order-3"; }

  function bindControls() {
    $("btnDemoFull").addEventListener("click", function () {
      postAction({ action: "demo", name: "full" });
    });
    $("btnDemoNormal").addEventListener("click", function () {
      postAction({ action: "demo", name: "normal" });
    });
    $("btnDemoCircuit").addEventListener("click", function () {
      postAction({ action: "demo", name: "circuit" });
    });

    var slider = $("rpsSlider");
    slider.addEventListener("input", function () {
      $("rpsValue").textContent = slider.value;
    });
    slider.addEventListener("change", function () {
      postAction({ action: "set_traffic", rps: Number(slider.value) });
    });

    $("btnBurst").addEventListener("click", function () {
      postAction({ action: "burst", rps: 320, duration: 2.5 });
    });
    $("btnInjectDelay").addEventListener("click", function () {
      postAction({ action: "inject", node: selectedNode(), delay: 0.35 });
    });
    $("btnInjectError").addEventListener("click", function () {
      postAction({ action: "inject", node: selectedNode(), delay: 0.35, force_error: true });
    });
    $("btnRecover").addEventListener("click", function () {
      postAction({ action: "recover", node: selectedNode() });
    });
    $("btnReset").addEventListener("click", function () {
      postAction({ action: "reset" });
    });
    $("btnClearLog").addEventListener("click", function () {
      $("logConsole").innerHTML = "";
      logLines = 0;
    });
  }

  // ---------------- 启动 ----------------

  resizeCanvas();
  bindControls();
  refreshState();
  setInterval(refreshState, 200);
  connectSSE();
  requestAnimationFrame(frame);
})();