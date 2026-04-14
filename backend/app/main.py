from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.db import init_db
from app.webhook import router as webhook_router
from app.shift_api import router as shift_api_router
from app.admin_api import router as admin_api_router

app = FastAPI(title="Shift LINE Backend")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return {"message": "Shift LINE Backend is running"}


static_dir = Path(__file__).resolve().parent.parent / "static"
app.mount("/liff", StaticFiles(directory=static_dir / "liff"), name="liff")
app.mount("/admin", StaticFiles(directory=static_dir / "admin"), name="admin")

app.include_router(webhook_router)
app.include_router(shift_api_router)
app.include_router(admin_api_router)