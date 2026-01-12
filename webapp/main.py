# main.py
# simple web app without frameworks: http server + udp socket server + mongodb

import logging
import mimetypes
import os
import socket
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from multiprocessing import Process
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from pymongo import MongoClient
from pymongo.errors import PyMongoError


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "front-init"

HTTP_HOST = "0.0.0.0"
HTTP_PORT = int(os.getenv("HTTP_PORT", "3000"))

SOCKET_HOST = os.getenv("SOCKET_HOST", "127.0.0.1")
SOCKET_PORT = int(os.getenv("SOCKET_PORT", "5000"))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "messages_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "messages")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(processName)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("app.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def file_path_for_route(route_path: str) -> Path:
    if route_path in ("/", "/index.html"):
        return STATIC_DIR / "index.html"

    if route_path in ("/message", "/message.html"):
        return STATIC_DIR / "message.html"

    if route_path == "/error.html":
        return STATIC_DIR / "error.html"

    if route_path == "/style.css":
        return STATIC_DIR / "style.css"

    if route_path == "/logo.png":
        return STATIC_DIR / "logo.png"

    # allow direct access to known static assets in the folder
    safe = route_path.lstrip("/")
    candidate = STATIC_DIR / safe
    try:
        candidate_resolved = candidate.resolve()
        if STATIC_DIR in candidate_resolved.parents and candidate_resolved.is_file():
            return candidate_resolved
    except OSError:
        pass

    return STATIC_DIR / "error.html"


def send_udp(payload: bytes) -> None:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.sendto(payload, (SOCKET_HOST, SOCKET_PORT))
    except OSError:
        logging.exception("failed to send udp message")


class AppHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path in ("/", "/index.html", "/message", "/message.html", "/style.css", "/logo.png"):
            return self._serve_path(path)

        candidate = file_path_for_route(path)
        if candidate.name == "error.html":
            return self._serve_404()

        return self._serve_file(candidate, status=200)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/message":
            return self._serve_404()

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0

        body = self.rfile.read(length) if length > 0 else b""

        # forward raw form data to socket server
        send_udp(body)

        # redirect back to main page
        self.send_response(302)
        self.send_header("Location", "/")
        self.end_headers()

    def _serve_path(self, route_path: str) -> None:
        path = file_path_for_route(route_path)
        return self._serve_file(path, status=200)

    def _serve_404(self) -> None:
        path = STATIC_DIR / "error.html"
        return self._serve_file(path, status=404)

    def _serve_file(self, path: Path, status: int) -> None:
        try:
            data = path.read_bytes()
        except OSError:
            logging.exception("failed to read file: %s", path)
            self.send_response(500)
            self.end_headers()
            return

        content_type, _ = mimetypes.guess_type(str(path))
        if not content_type:
            content_type = "application/octet-stream"

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt: str, *args) -> None:
        # redirect default http.server logging to our logger
        logging.info("%s - %s", self.address_string(), fmt % args)


def run_http_server() -> None:
    setup_logging()
    server = HTTPServer((HTTP_HOST, HTTP_PORT), AppHandler)
    logging.info("http server started on %s:%s", HTTP_HOST, HTTP_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        logging.info("http server stopped")


def parse_form_bytes(payload: bytes) -> dict:
    try:
        decoded = payload.decode("utf-8", errors="ignore")
    except Exception:
        decoded = ""

    data = parse_qs(decoded)
    username = (data.get("username") or data.get("name") or [""])[0]
    message = (data.get("message") or [""])[0]

    return {"username": username, "message": message}


def run_socket_server() -> None:
    setup_logging()
    logging.info("socket server starting on 0.0.0.0:%s (udp)", SOCKET_PORT)

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except Exception:
        logging.exception("cannot connect to mongodb")

    collection = client[MONGO_DB][MONGO_COLLECTION]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("0.0.0.0", SOCKET_PORT))

        while True:
            try:
                payload, _addr = s.recvfrom(65535)
                data = parse_form_bytes(payload)

                doc = {
                    "date": datetime.now().isoformat(sep=" ", timespec="microseconds"),
                    "username": data.get("username", ""),
                    "message": data.get("message", ""),
                }

                collection.insert_one(doc)
                logging.info("saved message for user: %s", doc["username"])

            except PyMongoError:
                logging.exception("mongodb error")
            except OSError:
                logging.exception("socket error")


def main() -> None:
    setup_logging()

    http_proc = Process(target=run_http_server, name="http-server")
    sock_proc = Process(target=run_socket_server, name="socket-server")

    http_proc.start()
    sock_proc.start()

    try:
        http_proc.join()
        sock_proc.join()
    except KeyboardInterrupt:
        logging.info("stopping...")
        http_proc.terminate()
        sock_proc.terminate()
        http_proc.join()
        sock_proc.join()


if __name__ == "__main__":
    main()
