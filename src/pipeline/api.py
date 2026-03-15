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

logger = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
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


@app.get("/counts/page/{page}")
def page_count(page: str, redis_client: RedisClient) -> PageCount:
    value: str | None = redis_client.get(f"pageviews:{page}")  # type: ignore[assignment]
    return PageCount(page=page, count=int(value) if value else 0)


@app.get("/users/{user_id}/last-page")
def last_page(user_id: str, redis_client: RedisClient) -> LastPage:
    value: str | None = redis_client.get(f"user:last_page:{user_id}")  # type: ignore[assignment]
    return LastPage(user=user_id, last_page=value)


@app.get("/events/{user_id}")
def user_events(user_id: str, session: CassandraSession) -> list[PageView]:
    rows = session.execute(
        "SELECT * FROM pageviews WHERE user_id=%s LIMIT 10", (user_id,)
    )
    return [PageView(**r._asdict()) for r in rows]


def main() -> None:
    try:
        uvicorn.run("pipeline.api:app", reload=True)
    except KeyboardInterrupt:
        logger.info("Shutting down API server")


if __name__ == "__main__":
    main()
