import json, time, urllib.request

BASE = "http://127.0.0.1:8124"

def post(payload):
    req = urllib.request.Request(
        BASE + "/api/action",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    return json.loads(urllib.request.urlopen(req).read())

def state():
    return json.loads(urllib.request.urlopen(BASE + "/api/state").read())

def n3():
    return next(n for n in state()["nodes"] if n["id"] == "order-3")

# 故障保持阶段: 轮询 8s, 应看到 half_open -> 探针失败 -> open (open_count 增长)
seen_half = False
deadline = time.time() + 10
while time.time() < deadline and n3()["breaker"]["open_count"] < 2:
    if n3()["breaker"]["state"] == "half_open":
        seen_half = True
    time.sleep(0.15)

node = n3()
print("fault-persist phase: seen_half =", seen_half,
      "| state =", node["breaker"]["state"],
      "| open_count =", node["breaker"]["open_count"])
assert seen_half and node["breaker"]["open_count"] >= 2

# 恢复 -> 半开探针连续成功 -> CLOSED 自愈
post({"action": "recover", "node": "order-3"})
deadline = time.time() + 15
while time.time() < deadline:
    node = n3()
    if node["breaker"]["state"] == "closed" and node["ok_rps"] > 0:
        break
    time.sleep(0.2)
node = n3()
print("recovered: state =", node["breaker"]["state"],
      "ok_rps =", node["ok_rps"], "health =", node["health"], "w =", node["weight"])
assert node["breaker"]["state"] == "closed" and node["ok_rps"] > 0
print("LIVE CYCLE VERIFIED")