#!/usr/bin/env python3
"""Small local NeMo guardrail endpoint for manual Praxis testing."""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def response_for(verdict: str) -> dict:
    if verdict == "success":
        return {"status": "success", "rails_status": {"local mock": {"status": "success"}}}
    if verdict == "blocked":
        return {"status": "blocked", "rails_status": {"local mock": {"status": "blocked"}}}
    return {
        "status": "error",
        "guardrails_data": {"error": "Local mock error", "details": "Selected with --verdict error"},
    }


def handler(verdict: str):
    class GuardrailHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length)
            try:
                request = json.loads(raw_body)
                print("Received guardrail request:", json.dumps(request, indent=2), flush=True)
            except json.JSONDecodeError:
                print("Received invalid JSON:", raw_body.decode(errors="replace"), flush=True)

            body = json.dumps(response_for(verdict)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt, *args):
            print(f"{self.address_string()} - {fmt % args}", flush=True)

    return GuardrailHandler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3001)
    parser.add_argument("--verdict", choices=("success", "blocked", "error"), default="success")
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), handler(args.verdict))
    print(f"NeMo mock listening on http://{args.host}:{args.port} with verdict={args.verdict}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
