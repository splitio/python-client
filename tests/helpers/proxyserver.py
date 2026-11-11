import threading
import http.server
import socketserver
import urllib.request
import urllib.error

# Define the port the proxy server will listen on
PORT = 8808

# Upstream requests must never block forever: a stuck handler thread would hang the server close.
REQUEST_TIMEOUT = 10


class ProxyServer(object):
    def start_proxy(self):
        self.server_address = ('', PORT)
        # Daemon: a proxy a test forgot (or failed) to stop must not keep the interpreter alive at exit.
        start_thread = threading.Thread(target=self._run, daemon=True)
        start_thread.start()

    def _run(self):
        with ThreadingHTTPServer(self.server_address, ProxyHTTPRequestHandler) as self.httpd:
            self.httpd.serve_forever()

    def stop(self):
        httpd = getattr(self, 'httpd', None)
        if httpd is not None:
            httpd.shutdown()


class ProxyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Extract the target URL from the client request path
        # If accessing via browser proxy settings, self.path will be the full URL
        target_url = self.path

        try:
            # Re-transmit the request to the destination server
            req = urllib.request.Request(target_url, headers=self.headers)
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                # Send the HTTP status code back to the client
                self.send_response(response.status)

                # Forward response headers
                for header, value in response.getheaders():
                    self.send_header(header, value)
                self.end_headers()

                # Stream the response body back to the client
                self.wfile.write(response.read())

        except urllib.error.HTTPError as e:
            self.send_error(e.code, e.reason)
        except urllib.error.URLError as e:
            self.send_error(502, f"Bad Gateway: {e.reason}")
        except Exception as e:
            self.send_error(500, f"Internal Server Error: {str(e)}")

    def log_message(self, format, *args):
        pass  # keep the test output quiet

    def do_POST(self):
        length = int(self.headers.get('Content-Length') or 0)
        if length:
            self.rfile.read(length)
        self.send_response(200)
        self.send_header('Content-Length', '2')
        self.end_headers()
        self.wfile.write(b"ok")

# ThreadingTCPServer allows handling multiple client connections simultaneously


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    # Handler threads are daemons and are not joined on close: a handler stuck on an upstream
    # request that never finishes would otherwise block both server_close() and interpreter exit.
    daemon_threads = True
    block_on_close = False
