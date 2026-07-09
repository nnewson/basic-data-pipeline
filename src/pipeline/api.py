import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated
from uuid import UUID

import redis
import uvicorn
from cassandra.cluster import Cluster, Session
from fastapi import Depends, FastAPI, Request

from pipeline.config import CASSANDRA_HOST, KEYSPACE, REDIS_HOST, REDIS_PORT
from pipeline.flink_stats_consumer import (
    LATEST_WINDOW_KEY,
    page_count_key,
    page_window_end_key,
    page_window_start_key,
)

logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    cluster = Cluster([CASSANDRA_HOST])
    app.state.session = cluster.connect(KEYSPACE)
    yield
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


@app.get("/")
def root() -> Health:
    return Health(status="ok")


@app.get("/health")
def health() -> Health:
    return Health(status="ok")


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
