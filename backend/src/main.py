from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI
import uvicorn

from scripts.seed_demo import seed
from src.config.settings import engine
from src.middlewares.request import limit_body, request_headers
from src.models.tables import schema
from src.routes.migrations import router as migration_router
from src.routes.workflow import router
from src.services import topology


@asynccontextmanager
async def lifespan(app):
    topology.init_schema()
    if os.getenv('SEED_DEMO', 'true').lower() == 'true':
        try:
            seed()
        except Exception:
            logging.exception('Demo seeding failed')
    yield


app = FastAPI(title='Database business workflow', lifespan=lifespan)
app.middleware('http')(limit_body)
app.middleware('http')(request_headers)
app.include_router(router)
app.include_router(migration_router)


@app.get('/')
def root():
    return {'service': 'ceramic-shards', 'docs': '/docs', 'health': '/health',
            'active_database': topology.accepting_database()}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
