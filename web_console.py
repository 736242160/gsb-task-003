"""
web_console.py
==============

零外部依赖的服务网格可视化控制台服务端。

    - http.server.ThreadingHTTPServer : 提供静态页面 + JSON API + SSE 事件流
    - 前端全部为原生 HTML/CSS/JavaScript (web/ 目录), 无 CDN、无三方库
    - SSE (Server-Sent Events) 长连接实时推送治理事件, 无需轮询
    - 状态快照通过 /api/state 由前端按帧率拉取 (约 5fps, 开销极小)

路由:
    GET  /                      控制台页面
    GET  /app.js /styles.css    静态资源
    GET  /api/state             内核完整快照 JSON
    GET  /api/events?after=N    SSE 事件流 (含最近事件回放 + 实时推送)
    POST /api/action            控制指令 (流量/故障/演示/复位)
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from mesh_kernel import TrafficKernel

WEB_ROOT_FILES = {
    "/": ("web/index.html", "text/html; charset=utf-8"),
    "/index.html": ("web/index.html", "text/html; charset=utf-8"),
    "/styles.css": ("web/styles.css", "text/css; charset=utf-8"),
    "/app.js": ("web/app.js", "application/javascript; charset=utf-8"),
}

SAFE_ACTIONS = {
    "set_traffic", "burst", "inject", "recover", "reset", "demo",
}


class ConsoleHandler(BaseHTTPRequestHandler):
    server_version = "MeshConsole/1.0"

    # 静默标准访问日志 (演示时保持控制台整洁), 仅打印错误
    def log_message(self, fmt, *args):  # noqa: A003
        if args and str(args[1]).startswith(("4", "5")):
            super().log_message(fmt, *args)

    # -- 工具 --------------------------------------------------------------

    def _send_json(self, payload, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0) or 0)
        length = min(length, 1 << 16)  # 64KiB 上限
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    # -- GET ---------------------------------------------------------------

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/api/state",):
            kernel: TrafficKernel = self.server.kernel
            self._send_json(kernel.snapshot())
            return

        if path == "/api/events":
            self._serve_sse(parse_qs(parsed.query))
            return

        static = WEB_ROOT_FILES.get(path)
        if static:
            self._serve_static(*static)
            return

        self._send_json({"error": "not found"}, status=404)

    def _serve_static(self, rel_path: str, content_type: str) -> None:
        try:
            with open(rel_path, "rb") as handle:
                body = handle.read()
        except FileNotFoundError:
            self._send_json({"error": "asset missing"}, status=500)
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _serve_sse(self, query) -> None:
        kernel: TrafficKernel = self.server.kernel
        after = 0
        try:
            after = int(query.get("after", ["0"])[0])
        except (ValueError, IndexError):
            after = 0

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        queue: list = []
        condition = threading.Condition()

        def _push(event):
            with condition:
                queue.append(event)
                condition.notify()

        unsub = kernel.add_listener(_push)

        def _write(payload: dict) -> bool:
            try:
                self.wfile.write(f"id: {payload['seq']}\n".encode("utf-8"))
                self.wfile.write(b"event: mesh\n")
                body = json.dumps(payload, ensure_ascii=False)
                self.wfile.write(f"data: {body}\n\n".encode("utf-8"))
                self.wfile.flush()
                return True
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                return False

        try:
            # 1) 历史事件回放, 让刷新页面后立即补齐上下文
            for event in kernel.recent_events(after):
                if not _write(event):
                    return
            # 2) 实时推送 + 心跳 (SSE 代理保活)
            last_beat = time.monotonic()
            while True:
                with condition:
                    if not queue:
                        condition.wait(timeout=1.0)
                    drained = queue[:]
                    del queue[:]
                for event in drained:
                    if event["seq"] <= after:
                        continue
                    if not _write(event):
                        return
                    after = event["seq"]
                if time.monotonic() - last_beat > 15:
                    try:
                        self.wfile.write(b": heartbeat\n\n")
                        self.wfile.flush()
                    except OSError:
                        return
                    last_beat = time.monotonic()
        finally:
            unsub()
    # -- POST --------------------------------------------------------------

    def do_POST(self):  # noqa: N802
        if urlparse(self.path).path != "/api/action":
            self._send_json({"error": "not found"}, status=404)
            return

        kernel: TrafficKernel = self.server.kernel
        data = self._read_body()
        action = str(data.get("action", ""))
        if action not in SAFE_ACTIONS:
            self._send_json({"ok": False, "error": "unknown action"}, status=400)
            return

        try:
            if action == "set_traffic":
                kernel.set_traffic(int(float(data.get("rps", 70))))
            elif action == "burst":
                duration = min(10.0, float(data.get("duration", 2.5)))
                rps = int(float(data.get("rps", 320)))
                kernel.burst(duration=duration, rps=rps)
            elif action == "inject":
                node_id = str(data.get("node", ""))
                if node_id not in kernel.nodes:
                    raise ValueError("unknown node")
                delay = max(0.0, min(5.0, float(data.get("delay", 0.35))))
                kernel.inject_fault(
                    node_id,
                    delay=delay,
                    force_error=bool(data.get("force_error", False)),
                    offline=bool(data.get("offline", False)),
                )
            elif action == "recover":
                node_id = str(data.get("node", ""))
                if node_id not in kernel.nodes:
                    raise ValueError("unknown node")
                kernel.recover_node(node_id)
            elif action == "reset":
                kernel.reset_breakers()
            elif action == "demo":
                name = str(data.get("name", "full"))
                accepted = kernel.start_demo(name)
                self._send_json({"ok": accepted, "running_demo": kernel.running_demo})
                return
            self._send_json({"ok": True})
        except (ValueError, TypeError) as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=400)


def create_server(host: str, port: int, kernel: TrafficKernel) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), ConsoleHandler)
    server.daemon_threads = True
    server.kernel = kernel
    return server


def serve_forever(host: str, port: int, kernel: TrafficKernel) -> ThreadingHTTPServer:
    """阻塞式启动 (由主入口调用); 返回服务器对象便于测试。"""
    server = create_server(host, port, kernel)
    actual_host, actual_port = server.server_address[:2]
    kernel.publish("console", {"msg": f"http://{actual_host}:{actual_port}"}, force=True)
    try:
        server.serve_forever(poll_interval=0.4)
    finally:
        server.server_close()
    return server