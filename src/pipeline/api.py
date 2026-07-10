import asyncio
import json
import logging
from collections import defaultdict
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from uuid import UUID

import redis
import redis.asyncio as async_redis
import uvicorn
from cassandra.cluster import Cluster, Session
from fastapi import Depends, FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from pipeline.config import CASSANDRA_HOST, KEYSPACE, REDIS_HOST, REDIS_PORT
from pipeline.flink_stats_consumer import (
    LATEST_WINDOW_KEY,
    page_count_key,
    page_window_end_key,
    page_window_start_key,
)
from pipeline.realtime_events import FLINK_WINDOWS_CHANNEL, PAGEVIEWS_CHANNEL

logger = logging.getLogger("api")


class WebSocketConnectionManager:
    def __init__(self) -> None:
        self.active_connections: dict[str, set[WebSocket]] = defaultdict(set)

    async def connect(self, channel: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections[channel].add(websocket)

    def disconnect(self, channel: str, websocket: WebSocket) -> None:
        self.active_connections[channel].discard(websocket)
        if not self.active_connections[channel]:
            del self.active_connections[channel]

    async def broadcast(self, channel: str, message: str) -> None:
        for websocket in list(self.active_connections.get(channel, set())):
            try:
                await websocket.send_text(message)
            except Exception:
                logger.exception("Dropping failed WebSocket connection")
                self.disconnect(channel, websocket)


websocket_manager = WebSocketConnectionManager()


async def close_async_resource(resource) -> None:
    close = getattr(resource, "aclose", None) or getattr(resource, "close")
    result = close()
    if result is not None:
        await result


async def redis_pubsub_bridge(
    redis_client: async_redis.Redis, manager: WebSocketConnectionManager
) -> None:
    channels = [PAGEVIEWS_CHANNEL, FLINK_WINDOWS_CHANNEL]
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(*channels)
    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            channel = message["channel"]
            data = message["data"]
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            await manager.broadcast(channel, data)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Redis pub/sub bridge stopped")
    finally:
        with suppress(Exception):
            await pubsub.unsubscribe(*channels)
        await close_async_resource(pubsub)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    app.state.redis_pubsub_client = async_redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    app.state.redis_pubsub_task = asyncio.create_task(
        redis_pubsub_bridge(app.state.redis_pubsub_client, websocket_manager)
    )
    cluster = Cluster([CASSANDRA_HOST])
    app.state.session = cluster.connect(KEYSPACE)
    try:
        yield
    finally:
        app.state.redis_pubsub_task.cancel()
        with suppress(asyncio.CancelledError):
            await app.state.redis_pubsub_task
        await close_async_resource(app.state.redis_pubsub_client)
        app.state.redis_client.close()
        cluster.shutdown()


app = FastAPI(lifespan=lifespan)


def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis_client


def get_session(request: Request) -> Session:
    return request.app.state.session


RedisClient = Annotated[redis.Redis, Depends(get_redis)]
CassandraSession = Annotated[Session, Depends(get_session)]


def redis_text(redis_client: redis.Redis, key: str) -> str | None:
    value = redis_client.get(key)
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def normalize_page(page: str) -> str:
    if page == "":
        return "/"
    if page.startswith("/"):
        return page
    return f"/{page}"


@dataclass
class PageCount:
    page: str
    count: int


@dataclass
class LastPage:
    user: str
    last_page: str | None


@dataclass
class PageView:
    user_id: str
    event_id: UUID
    event_time: datetime
    page: str


@dataclass
class FlinkPageCount:
    page: str
    count: int
    window_start: str | None
    window_end: str | None


@dataclass
class FlinkWindow:
    page: str
    count: int
    window_start: str
    window_end: str


@dataclass
class Health:
    status: str


REALTIME_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pipeline Realtime</title>
  <style>
    :root {
      color-scheme: light dark;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    body {
      margin: 0;
      background: #f7f7f3;
      color: #202124;
    }
    main {
      max-width: 1120px;
      margin: 0 auto;
      padding: 32px 20px;
    }
    header {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 20px;
    }
    h1 {
      margin: 0;
      font-size: 28px;
      line-height: 1.2;
      font-weight: 700;
    }
    button {
      border: 1px solid #b8b8ad;
      border-radius: 6px;
      background: #ffffff;
      color: #202124;
      cursor: pointer;
      font: inherit;
      padding: 8px 12px;
    }
    .streams {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }
    section {
      min-width: 0;
      border: 1px solid #d4d3c7;
      border-radius: 8px;
      background: #ffffff;
      overflow: hidden;
    }
    .stream-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid #e4e2d8;
    }
    h2 {
      margin: 0;
      font-size: 16px;
      line-height: 1.3;
    }
    .status {
      border-radius: 999px;
      background: #ecebe3;
      color: #525047;
      font-size: 12px;
      line-height: 1;
      padding: 6px 8px;
      white-space: nowrap;
    }
    .status.open {
      background: #d8f0de;
      color: #1d5f32;
    }
    .status.closed {
      background: #f5dddd;
      color: #8a2626;
    }
    pre {
      box-sizing: border-box;
      height: 60vh;
      margin: 0;
      overflow: auto;
      padding: 16px;
      background: #151515;
      color: #f1f1ec;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      font-size: 13px;
      line-height: 1.45;
      white-space: pre-wrap;
      word-break: break-word;
    }
    @media (max-width: 760px) {
      header {
        align-items: flex-start;
        flex-direction: column;
      }
      .streams {
        grid-template-columns: 1fr;
      }
      pre {
        height: 40vh;
      }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Pipeline Realtime</h1>
      <button id="clear" type="button">Clear</button>
    </header>
    <div class="streams">
      <section>
        <div class="stream-header">
          <h2>Pageviews</h2>
          <span id="pageviews-status" class="status">connecting</span>
        </div>
        <pre id="pageviews-log"></pre>
      </section>
      <section>
        <div class="stream-header">
          <h2>Flink Windows</h2>
          <span id="flink-status" class="status">connecting</span>
        </div>
        <pre id="flink-log"></pre>
      </section>
    </div>
  </main>
  <script>
    const socketProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const socketBase = `${socketProtocol}//${window.location.host}`;

    function append(log, message) {
      const timestamp = new Date().toISOString();
      try {
        message = JSON.stringify(JSON.parse(message), null, 2);
      } catch {
      }
      log.textContent += `[${timestamp}] ${message}\\n\\n`;
      log.scrollTop = log.scrollHeight;
    }

    function connect(path, statusId, logId) {
      const status = document.getElementById(statusId);
      const log = document.getElementById(logId);
      const socket = new WebSocket(`${socketBase}${path}`);

      socket.addEventListener("open", () => {
        status.textContent = "connected";
        status.className = "status open";
      });
      socket.addEventListener("message", (event) => append(log, event.data));
      socket.addEventListener("close", () => {
        status.textContent = "closed";
        status.className = "status closed";
      });
      socket.addEventListener("error", () => {
        status.textContent = "error";
        status.className = "status closed";
      });
    }

    document.getElementById("clear").addEventListener("click", () => {
      document.getElementById("pageviews-log").textContent = "";
      document.getElementById("flink-log").textContent = "";
    });

    connect("/ws/pageviews", "pageviews-status", "pageviews-log");
    connect("/ws/flink/windows", "flink-status", "flink-log");
  </script>
</body>
</html>
"""


@app.get("/")
def root() -> Health:
    return Health(status="ok")


@app.get("/health")
def health() -> Health:
    return Health(status="ok")


@app.get("/realtime", response_class=HTMLResponse)
def realtime() -> str:
    return REALTIME_PAGE


async def stream_websocket_channel(channel: str, websocket: WebSocket) -> None:
    await websocket_manager.connect(channel, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        websocket_manager.disconnect(channel, websocket)


@app.websocket("/ws/pageviews")
async def pageview_events(websocket: WebSocket) -> None:
    await stream_websocket_channel(PAGEVIEWS_CHANNEL, websocket)


@app.websocket("/ws/flink/windows")
async def flink_window_events(websocket: WebSocket) -> None:
    await stream_websocket_channel(FLINK_WINDOWS_CHANNEL, websocket)


@app.get("/counts/page/{page}")
def page_count(page: str, redis_client: RedisClient) -> PageCount:
    normalized_page = normalize_page(page)
    value = redis_text(redis_client, f"pageviews:{normalized_page}")
    return PageCount(page=normalized_page, count=int(value) if value else 0)


@app.get("/users/{user_id}/last-page")
def last_page(user_id: str, redis_client: RedisClient) -> LastPage:
    value = redis_text(redis_client, f"user:last_page:{user_id}")
    return LastPage(user=user_id, last_page=value)


@app.get("/events/{user_id}")
def user_events(user_id: str, session: CassandraSession) -> list[PageView]:
    rows = session.execute(
        "SELECT * FROM pageviews WHERE user_id=%s LIMIT 10", (user_id,)
    )
    return [PageView(**r._asdict()) for r in rows]


@app.get("/flink/counts/page/{page}")
def flink_page_count(page: str, redis_client: RedisClient) -> FlinkPageCount:
    normalized_page = normalize_page(page)
    count_value = redis_text(redis_client, page_count_key(normalized_page))
    window_start = redis_text(redis_client, page_window_start_key(normalized_page))
    window_end = redis_text(redis_client, page_window_end_key(normalized_page))
    return FlinkPageCount(
        page=normalized_page,
        count=int(count_value) if count_value else 0,
        window_start=window_start,
        window_end=window_end,
    )


@app.get("/flink/windows/latest")
def flink_latest_window(redis_client: RedisClient) -> FlinkWindow | None:
    value = redis_text(redis_client, LATEST_WINDOW_KEY)
    if not value:
        return None
    data = json.loads(value)
    return FlinkWindow(
        page=data["page"],
        count=int(data["count"]),
        window_start=data["window_start"],
        window_end=data["window_end"],
    )


def main() -> None:
    try:
        uvicorn.run("pipeline.api:app", reload=True)
    except KeyboardInterrupt:
        logger.info("Shutting down API server")


if __name__ == "__main__":
    main()
