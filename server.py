#!/usr/bin/env python3
"""HTTP API + static web UI. Stdlib only.

Run:  python3 server.py [port]     (default 8000, data in ./data)
"""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from catalog.migrator import Migrator
from catalog.seed import gen_batch, seed_demo
from catalog.service import BadRequest, CatalogError, CatalogService

ROOT = os.path.dirname(os.path.abspath(__file__))
WEB = os.path.join(ROOT, "web")

CONTENT_TYPES = {".html": "text/html; charset=utf-8",
                 ".js": "application/javascript; charset=utf-8",
                 ".css": "text/css; charset=utf-8"}


def build_handler(svc, mig):
    class Handler(BaseHTTPRequestHandler):
        server_version = "TenantCatalog/1.0"

        # ---------------- plumbing ----------------
        def _send_json(self, obj, status=200):
            body = json.dumps(obj, ensure_ascii=False, indent=1).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_static(self, path):
            ext = os.path.splitext(path)[1]
            with open(path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type",
                             CONTENT_TYPES.get(ext, "application/octet-stream"))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle(self, method):
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)
            try:
                if method == "GET" and not path.startswith("/api/"):
                    return self._static(path)
                body = {}
                if method == "POST":
                    length = int(self.headers.get("Content-Length") or 0)
                    if length:
                        body = json.loads(self.rfile.read(length).decode())
                result = self._route(method, path, qs, body)
                self._send_json(result if result is not None else {"ok": True})
            except CatalogError as e:
                self._send_json({"error": str(e)}, status=e.status)
            except Exception as e:  # noqa: BLE001
                self._send_json({"error": f"internal: {e}"}, status=500)

        def _static(self, path):
            if path in ("/", "/index.html"):
                return self._send_static(os.path.join(WEB, "index.html"))
            safe = os.path.normpath(path).lstrip("/")
            full = os.path.join(WEB, safe)
            if not os.path.realpath(full).startswith(os.path.realpath(WEB)):
                return self._send_json({"error": "forbidden"}, status=403)
            if os.path.isfile(full):
                return self._send_static(full)
            self._send_json({"error": "not found"}, status=404)

        # ---------------- routes ----------------
        def _route(self, method, path, qs, body):
            if method == "GET" and path == "/api/state":
                return svc.dashboard()
            if method == "GET" and path == "/api/query":
                return svc.query(qs.get("tenant", [""])[0])
            if method == "GET" and path == "/api/compare":
                return svc.compare(qs.get("tenant", [""])[0])
            if method == "GET" and path == "/api/isolation_check":
                return svc.isolation_check()
            if method == "POST" and path == "/api/reset":
                svc.reset()
                return {"ok": True, "state": "empty"}
            if method == "POST" and path == "/api/seed":
                return seed_demo(svc)

            parts = [p for p in path.split("/") if p]
            # /api/tenants/<tenant>/<action>
            if (len(parts) == 4 and parts[0] == "api"
                    and parts[1] == "tenants" and method == "POST"):
                tenant, action = parts[2], parts[3]
                if action == "preview":
                    return mig.preview(tenant)
                if action == "migrate":
                    return mig.start(tenant)
                if action == "catchup":
                    return mig.catchup(tenant)
                if action == "cutover":
                    return mig.cutover(tenant)
                if action == "rollback":
                    return mig.rollback(tenant)
                if action == "batches":
                    return gen_batch(
                        svc, tenant,
                        n=int(body.get("n", 2)),
                        plant_error=bool(body.get("plant_error", False)))
                raise BadRequest(f"unknown tenant action: {action}")
            # /api/shared_dims/<key...>/<action>  (key itself contains '/')
            if (len(parts) >= 4 and parts[0] == "api"
                    and parts[1] == "shared_dims" and method == "POST"):
                key = "/".join(parts[2:-1])
                action = parts[-1]
                if action == "revise":
                    return svc.revise_shared(key, body.get("payload", {}))
                if action == "tombstone":
                    return svc.tombstone_shared(key)
                if action == "acl":
                    return svc.tighten_shared(key, body.get("allowed", []))
                raise BadRequest(f"unknown shared-dim action: {action}")
            raise BadRequest(f"no route: {method} {path}")

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

        def log_message(self, fmt, *args):
            sys.stderr.write("[http] %s\n" % (fmt % args))

    return Handler


def run(port=8000, data_dir=None):
    data_dir = data_dir or os.environ.get("CATALOG_DATA",
                                          os.path.join(ROOT, "data"))
    svc = CatalogService(data_dir)
    mig = Migrator(svc)
    httpd = ThreadingHTTPServer(("127.0.0.1", port), build_handler(svc, mig))
    print(f"catalog server on http://127.0.0.1:{port}  (data: {data_dir})")
    httpd.serve_forever()


if __name__ == "__main__":
    run(port=int(sys.argv[1]) if len(sys.argv) > 1 else 8000)
