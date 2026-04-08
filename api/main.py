"""AI Controller — FastAPI application entry point."""

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from api.auth import router as auth_router
from api.routers.alerts import router as alerts_router
from api.routers.analytics import router as analytics_router
from api.routers.fleet import router as fleet_router
from api.routers.ingest import router as ingest_router
from api.routers.miners import router as miners_router
from api.routers.settings import router as settings_router

app = FastAPI(
    title="AI Controller",
    description="Predictive Maintenance for ASIC Mining Fleets",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(auth_router, prefix="/api", tags=["auth"])
app.include_router(fleet_router, prefix="/api", tags=["fleet"])
app.include_router(miners_router, prefix="/api", tags=["miners"])
app.include_router(analytics_router, prefix="/api", tags=["analytics"])
app.include_router(alerts_router, prefix="/api", tags=["alerts"])
app.include_router(settings_router, prefix="/api", tags=["settings"])
app.include_router(ingest_router, prefix="/api", tags=["ingest"])

# Dashboard static files
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")
if os.path.isdir(DASHBOARD_DIR):
    app.mount("/assets", StaticFiles(directory=DASHBOARD_DIR), name="assets")


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "aicontroller-api", "version": "1.0.0"}


@app.get("/{full_path:path}", include_in_schema=False)
async def serve_spa(full_path: str):
    """Serve the SPA for all non-API paths."""
    index = os.path.join(DASHBOARD_DIR, "index.html")
    if os.path.isfile(index):
        return FileResponse(index)
    return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)


@app.get("/", include_in_schema=False)
async def root():
    index = os.path.join(DASHBOARD_DIR, "index.html")
    if os.path.isfile(index):
        return FileResponse(index)
    return HTMLResponse("<h1>AI Controller API Running</h1>")
