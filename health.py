import os
from http.server import BaseHTTPRequestHandler, HTTPServer

# Koyeb ???? PORT ????????
PORT = int(os.environ.get("PORT", 8000))

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        # ??? ????? ?? ??? ?? ?????
        return

def run():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    print(f"Health server running on port {PORT}")
    server.serve_forever()

if __name__ == "__main__":
    run()