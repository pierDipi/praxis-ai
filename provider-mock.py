#!/usr/bin/env python3
"""Local JSON upstream that echoes Praxis request details."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ProviderHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length)
        try:
            request_body = json.loads(raw_body)
        except json.JSONDecodeError:
            request_body = raw_body.decode(errors="replace")

        gateway_model = self.headers.get("X-Gateway-Model-Name")
        print(f"X-Gateway-Model-Name: {gateway_model}", flush=True)
        print("Body:", json.dumps(request_body, indent=2), flush=True)

        response = json.dumps(
            {
                "mock": "provider",
                "x_gateway_model_name": gateway_model,
                "request_body": request_body,
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}", flush=True)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 3000), ProviderHandler)
    print("Provider mock listening on http://127.0.0.1:3000", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
