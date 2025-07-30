import http.server
import socketserver
import sqlite3
import threading
import unittest

from health import SCHEMA, probe, record


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if self.path == "/health" else 503)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


class HealthTests(unittest.TestCase):
    def test_http_probe_and_incident_lifecycle(self):
        server = socketserver.TCPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            ok, elapsed, detail = probe({"type":"http","host":"127.0.0.1","port":server.server_address[1],"path":"/health"})
            self.assertTrue(ok)
            self.assertGreaterEqual(elapsed, 0)
            self.assertEqual(detail, "HTTP 200")
            self.assertFalse(probe({"type":"http","host":"127.0.0.1","port":server.server_address[1],"path":"/missing"})[0])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
        with sqlite3.connect(":memory:") as db:
            db.executescript(SCHEMA)
            self.assertIsNone(record(db,"api",False,1,"fail","t1"))
            self.assertIsNone(record(db,"api",False,1,"fail","t2"))
            self.assertEqual(record(db,"api",False,1,"fail","t3"),"incident opened")
            self.assertIsNone(record(db,"api",False,1,"fail","t4"))
            self.assertEqual(db.execute("SELECT count(*) FROM incidents").fetchone()[0],1)
            self.assertEqual(record(db,"api",True,1,"ok","t5"),"recovered")
            self.assertEqual(db.execute("SELECT ended_at FROM incidents").fetchone()[0],"t5")


if __name__ == "__main__":
    unittest.main()

