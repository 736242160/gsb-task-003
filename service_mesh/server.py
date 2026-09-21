"""Zero-dependency visualization console built on ``http.server``.

Serves the native HTML/CSS/JS console from ``web/`` and exposes a small
JSON API consumed by the page:

    GET  /api/state               kernel + demo status snapshot
    GET  /api/events?after=<seq>  long-poll governance events
    POST /api/action              {"action": "...", ...} traffic/fault/demo ctl
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .demo import DemoRunner
from .kernel import MeshKernel

WEB_ROOT = "web"
_MIME = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}


class ConsoleServer:
    def __init__(self, kernel=None, host="127.0.0.1", port=8200, web_root=WEB_ROOT):
        self.kernel = kernel or MeshKernel()
        self.host = host
        self.port = port
        self.web_root = web_root
        self.demo = DemoRunner(self.kernel)
        self._httpd = None
        self._thread = None

    # ------------------------------------------------------------------
    def start(self, open_browser=False):
        self.kernel.start()
        handler = self._make_handler()
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        self._httpd.daemon_threads = True
        actual_host, actual_port = self._httpd.server_address
        self.host, self.port = actual_host, actual_port
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name="mesh-http", daemon=True
        )
        self._thread.start()
        if open_browser:
            threading.Thread(
                target=self._open_browser, args=(actual_port,), daemon=True
            ).start()
        return actual_host, actual_port

    @staticmethod
    def _open_browser(port):
        import time
        import webbrowser

        time.sleep(0.4)
        webbrowser.open("http://127.0.0.1:%d/" % port)

    def stop(self):
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        self.kernel.stop()

    @property
    def url(self):
        return "http://%s:%d/" % (self.host, self.port)

    # ------------------------------------------------------------------
    def _make_handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "MeshConsole/1.0"

            def log_message(self, fmt, *args):
                pass

            def _json(self, payload, status=200):
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def _static(self, rel):
                import os

                if rel == "" or rel.endswith("/"):
                    rel = (rel + "index.html").lstrip("/")
                rel = rel.replace("\\", "/").lstrip("/")
                base = os.path.abspath(server.web_root)
                target = os.path.abspath(os.path.join(base, rel))
                if not target.startswith(base + os.sep) and target != base:
                    self.send_error(403)
                    return
                if not os.path.isfile(target):
                    self.send_error(404)
                    return
                ext = os.path.splitext(target)[1].lower()
                ctype = _MIME.get(ext, "application/octet-stream")
                try:
                    with open(target, "rb") as handle:
                        body = handle.read()
                except OSError:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(body)

            # ----------------------------------------------------------
            def do_GET(self):
                parsed = urlparse(self.path)
                path = parsed.path
                if path == "/api/state":
                    try:
                        self._json({"kernel": server.kernel.snapshot(),
                                    "demo": server.demo.status()})
                    except Exception as exc:
                        self._json({"error": str(exc)}, status=500)
                    return
                if path == "/api/events":
                    query = parse_qs(parsed.query)
                    try:
                        after = int(query.get("after", ["0"])[0])
                    except ValueError:
                        after = 0
                    if query.get("wait", ["1"])[0] != "0":
                        server.kernel.events.wait(after, timeout=12.0)
                    events, latest = server.kernel.events.poll(after)
                    self._json({"events": events, "latest": latest,
                                "demo": server.demo.status()})
                    return
                if path == "/api/health":
                    self._json({"ok": True})
                    return
                self._static(path.lstrip("/"))

            # ----------------------------------------------------------
            def do_POST(self):
                parsed = urlparse(self.path)
                if parsed.path != "/api/action":
                    self.send_error(404)
                    return
                length = int(self.headers.get("Content-Length", 0) or 0)
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    data = json.loads(raw.decode("utf-8") or "{}")
                except (ValueError, UnicodeDecodeError):
                    self._json({"error": "invalid json"}, status=400)
                    return
                try:
                    result = self._dispatch(data)
                    self._json({"ok": True, "result": result})
                except Exception as exc:
                    self._json({"ok": False, "error": str(exc)}, status=400)

            def _dispatch(self, data):
                action = data.get("action")
                k = server.kernel
                if action == "traffic":
                    rate = float(data.get("rate", 0))
                    k.set_traffic(rate)
                    return {"traffic_rate": rate}
                if action == "burst":
                    return {"summary": k.burst(int(data.get("count", 120)))}
                if action == "inject_latency":
                    k.inject_latency(data["node"], float(data.get("delay", 0)))
                    return {}
                if action == "inject_fault":
                    k.inject_fault(data["node"], float(data.get("probability", 0)))
                    return {}
                if action == "offline":
                    k.set_offline(data["node"], bool(data.get("offline", True)))
                    return {}
                if action == "reset":
                    k.reset()
                    server.demo.cancel()
                    return {}
                if action == "demo":
                    scenario = data.get("scenario")
                    if server.demo.is_busy():
                        raise RuntimeError("demo already running")
                    server.demo.start(scenario)
                    return {"scenario": scenario}
                if action == "demo_cancel":
                    server.demo.cancel()
                    return {}
                raise ValueError("unknown action: %r" % action)

        return Handler
