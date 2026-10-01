import threading
import http.server
import socketserver
import urllib.request
import urllib.error

# Define the port the proxy server will listen on
PORT = 8808

class ProxyServer(object):
    def start_proxy(self):
        self.server_address = ('', PORT)
        print(f"[*] Starting proxy server on port {PORT}...")
        start_thread = threading.Thread(target=self._run)
        start_thread.start()
        
    def _run(self):
        with ThreadingHTTPServer(self.server_address, ProxyHTTPRequestHandler) as self.httpd:
            try:
                self.httpd.serve_forever()
            except KeyboardInterrupt:
                print("\n[-] Shutting down proxy server.")
                
    def stop(self):
        print("\n[-] Shutting down proxy server.")
        self.httpd.shutdown()

class ProxyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Extract the target URL from the client request path
        # If accessing via browser proxy settings, self.path will be the full URL
        target_url = self.path
        
        print(f"[+] Proxying request for: {target_url}")
        
        try:
            # Re-transmit the request to the destination server
            req = urllib.request.Request(target_url, headers=self.headers)
            with urllib.request.urlopen(req) as response:
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

    def do_POST(self):
        self.wfile.write("ok")

# ThreadingTCPServer allows handling multiple client connections simultaneously
class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    pass

