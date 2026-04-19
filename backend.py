# =============================================================
# backend.py  –  Small Business Insights SaaS
# =============================================================

from fastapi import (
    FastAPI, UploadFile, File, Depends, HTTPException, status, Form, Request, Header
)
from fastapi.responses import JSONResponse, FileResponse, Response
from weasyprint import HTML
from fastapi.security import OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from uuid import uuid4
import hashlib
import secrets

from fastapi import Body
import re

from typing import Dict, Any, Optional, List, Tuple, Generator, Literal
from io import BytesIO
from datetime import datetime, timedelta
import tempfile
import textwrap
import math
import json
import logging
import requests
from svix.webhooks import Webhook, WebhookVerificationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


import os
from dotenv import load_dotenv

load_dotenv()
import unicodedata

import pandas as pd
import numpy as np
import stripe

from jose import jwt, JWTError
from passlib.context import CryptContext

from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime,
    Text, ForeignKey, Boolean
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session

from dateutil import parser as dateparser
from pydantic import BaseModel

# -------------------------------------------------------------
# PDF imports
# -------------------------------------------------------------
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

# =============================================================
# BRAND / STYLE CONSTANTS
# =============================================================

BRAND_BLUE = "#5B8DEF"
BRAND_BLUE_DARK = "#3F6FD8"
ACCENT_GREEN = "#10B981"
ACCENT_PURPLE = "#8B5CF6"
ACCENT_ORANGE = "#F97316"

BACKGROUND_GREY = "#F3F4F6"  # slightly lighter for print
TEXT_PRIMARY = "#111827"
TEXT_SECONDARY = "#4B5563"
TEXT_MUTED = "#6B7280"
ROW_ALT = "#F9FAFB"

# =============================================================
# CONFIG
# =============================================================

SECRET_KEY = os.getenv("SECRET_KEY", "").strip()
ENV = os.getenv("ENV", "development").strip().lower()

if not SECRET_KEY:
    raise RuntimeError("SECRET_KEY must be set in the environment.")

if ENV == "production" and SECRET_KEY == "dev_only_change_me":
    raise RuntimeError("SECRET_KEY cannot use a development placeholder in production.")

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

MAX_FILE_SIZE_MB = 10
MAX_DASHBOARDS_PER_HOUR = 20
FREE_PLAN_DASHBOARD_LIMIT = 1
FREE_PLAN_SAVED_VIEW_LIMIT = 3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{os.path.join(BASE_DIR, 'app.db')}"
)

FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL", "http://localhost:3000").strip()
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_SUCCESS_PATH = os.getenv("STRIPE_SUCCESS_PATH", "/billing/success").strip()
STRIPE_CANCEL_PATH = os.getenv("STRIPE_CANCEL_PATH", "/billing/cancel").strip()
REPORTS_CRON_SECRET = os.getenv("REPORTS_CRON_SECRET", "").strip()
RESEND_WEBHOOK_SECRET = os.getenv("RESEND_WEBHOOK_SECRET", "").strip()

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "").strip()
EMAIL_FROM_ADDRESS = os.getenv("EMAIL_FROM_ADDRESS", "reports@mail.easy-dash.io").strip()
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "Dashboard Reports").strip()
EMAIL_SEND_TIMEOUT_SECONDS = int(os.getenv("EMAIL_SEND_TIMEOUT_SECONDS", "20"))
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "support@easy-dash.io").strip().lower()

CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "").strip()
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "").strip()
CLOUDFLARE_ZONE_ID = os.getenv("CLOUDFLARE_ZONE_ID", "").strip()
CLOUDFLARE_GRAPHQL_URL = "https://api.cloudflare.com/client/v4/graphql"

stripe.api_key = STRIPE_SECRET_KEY or None

# =============================================================
# DATABASE INIT
# =============================================================

engine_kwargs = {}

if DATABASE_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# =============================================================
# MODELS
# =============================================================

class SignupRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    token: str
    password: str


class TestEmailRequest(BaseModel):
    to_email: str

class SendDashboardReportRequest(BaseModel):
    to_email: str
    dashboard_id: int

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    subscription_status = Column(String, default="trial")  # trial, active, canceled
    stripe_customer_id = Column(String, nullable=True)

    email_verified = Column(Boolean, nullable=False, default=False)
    email_verification_token = Column(String, nullable=True, index=True)
    email_verification_expires_at = Column(DateTime, nullable=True)

    password_reset_token = Column(String, nullable=True, index=True)
    password_reset_expires_at = Column(DateTime, nullable=True)

    dashboards = relationship("Dashboard", back_populates="user")


class Dashboard(Base):
    __tablename__ = "dashboards"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    file_name = Column(String, nullable=False)
    mapping_json = Column(Text, nullable=False)    # mapping + filters + detection meta
    dashboard_json = Column(Text, nullable=False)  # full dashboard payload
    source_rows_json = Column(Text, nullable=False, default="[]")
    source_columns_json = Column(Text, nullable=False, default="[]")

    user = relationship("User", back_populates="dashboards")

class SavedMapping(Base):
    __tablename__ = "saved_mappings"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    schema_fingerprint = Column(String, nullable=False, index=True)
    sample_file_name = Column(String, nullable=True)

    date_col = Column(String, nullable=True)
    amount_col = Column(String, nullable=True)
    product_col = Column(String, nullable=True)
    customer_col = Column(String, nullable=True)
    date_parse_mode = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class EmailEvent(Base):
    __tablename__ = "email_events"

    id = Column(Integer, primary_key=True, index=True)
    resend_email_id = Column(String, index=True, nullable=True)
    event_type = Column(String, nullable=False)
    to_email = Column(String, nullable=True)
    subject = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    raw_json = Column(Text, nullable=False)

class SavedView(Base):
    __tablename__ = "saved_views"

    id = Column(Integer, primary_key=True, index=True)
    dashboard_id = Column(Integer, ForeignKey("dashboards.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String, nullable=False)
    filters_json = Column(Text, nullable=False)
    report_enabled = Column(Boolean, nullable=False, default=False)
    report_frequency = Column(String, nullable=False, default="weekly")
    report_recipient = Column(String, nullable=True)
    last_report_sent_at = Column(DateTime, nullable=True)
    last_report_started_at = Column(DateTime, nullable=True)
    last_report_error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class TrafficEvent(Base):
    __tablename__ = "traffic_events"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    path = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

from sqlalchemy import text

def run_migrations():
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT 0"))
        except Exception:
            pass

        try:
            conn.execute(text("ALTER TABLE users ADD COLUMN email_verification_token TEXT"))
        except Exception:
            pass

        try:
            conn.execute(text("ALTER TABLE users ADD COLUMN email_verification_expires_at DATETIME"))
        except Exception:
            pass

        try:
            conn.execute(text("ALTER TABLE users ADD COLUMN password_reset_token TEXT"))
        except Exception:
            pass

        try:
            conn.execute(text("ALTER TABLE users ADD COLUMN password_reset_expires_at DATETIME"))
        except Exception:
            pass

        # No test-account exceptions in production migrations.

run_migrations()

def run_safe_migrations() -> None:
    """
    Lightweight startup migrations for both SQLite and Postgres.
    Adds missing auth/reporting columns without requiring shell access.
    """
    try:
        with engine.connect() as conn:
            if DATABASE_URL.startswith("sqlite"):
                saved_view_columns_result = conn.exec_driver_sql("PRAGMA table_info(saved_views)")
                saved_view_columns = saved_view_columns_result.fetchall()
                existing_saved_view_columns = {row[1] for row in saved_view_columns}

                user_columns_result = conn.exec_driver_sql("PRAGMA table_info(users)")
                user_columns = user_columns_result.fetchall()
                existing_user_columns = {row[1] for row in user_columns}

            else:
                saved_view_columns_result = conn.exec_driver_sql("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'saved_views'
                """)
                existing_saved_view_columns = {row[0] for row in saved_view_columns_result.fetchall()}

                user_columns_result = conn.exec_driver_sql("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'users'
                """)
                existing_user_columns = {row[0] for row in user_columns_result.fetchall()}

            if "last_report_sent_at" not in existing_saved_view_columns:
                conn.exec_driver_sql("ALTER TABLE saved_views ADD COLUMN last_report_sent_at TIMESTAMP")

            if "last_report_started_at" not in existing_saved_view_columns:
                conn.exec_driver_sql("ALTER TABLE saved_views ADD COLUMN last_report_started_at TIMESTAMP")

            if "last_report_error" not in existing_saved_view_columns:
                conn.exec_driver_sql("ALTER TABLE saved_views ADD COLUMN last_report_error TEXT")

            if "email_verified" not in existing_user_columns:
                conn.exec_driver_sql("ALTER TABLE users ADD COLUMN email_verified BOOLEAN NOT NULL DEFAULT FALSE")

            if "email_verification_token" not in existing_user_columns:
                conn.exec_driver_sql("ALTER TABLE users ADD COLUMN email_verification_token TEXT")

            if "email_verification_expires_at" not in existing_user_columns:
                conn.exec_driver_sql("ALTER TABLE users ADD COLUMN email_verification_expires_at TIMESTAMP")

            if "password_reset_token" not in existing_user_columns:
                conn.exec_driver_sql("ALTER TABLE users ADD COLUMN password_reset_token TEXT")

            if "password_reset_expires_at" not in existing_user_columns:
                conn.exec_driver_sql("ALTER TABLE users ADD COLUMN password_reset_expires_at TIMESTAMP")

            # No test-account exceptions in production safe migrations.

            if DATABASE_URL.startswith("sqlite"):
                conn.exec_driver_sql("""
                    UPDATE saved_views
                    SET report_enabled = 1
                    WHERE LOWER(TRIM(CAST(report_enabled AS TEXT))) IN ('true', '1', 'yes', 'on')
                """)

                conn.exec_driver_sql("""
                    UPDATE saved_views
                    SET report_enabled = 0
                    WHERE LOWER(TRIM(CAST(report_enabled AS TEXT))) IN ('false', '0', 'no', 'off', '')
                       OR report_enabled IS NULL
                """)

            conn.commit()
    except Exception:
        logger.exception("Failed running startup migrations.")
        raise


run_safe_migrations()

# =============================================================
# EMAIL
# =============================================================

def is_email_configured() -> bool:
    return bool(RESEND_API_KEY and EMAIL_FROM_ADDRESS)


def build_from_header() -> str:
    if EMAIL_FROM_NAME:
        return f"{EMAIL_FROM_NAME} <{EMAIL_FROM_ADDRESS}>"
    return EMAIL_FROM_ADDRESS


def send_email_message(
    *,
    to_email: str,
    subject: str,
    body_text: str,
    body_html: Optional[str] = None,
    from_address: Optional[str] = None,
    from_name: Optional[str] = None,
    attachments: Optional[List[Dict[str, str]]] = None,
) -> None:
    """
    Send an email using the Resend API.
    """
    import base64

    to_email = (to_email or "").strip()
    subject = (subject or "").strip()
    body_text = (body_text or "").strip()

    if not to_email:
        raise HTTPException(status_code=400, detail="Recipient email is required.")

    if not subject:
        raise HTTPException(status_code=400, detail="Email subject is required.")

    if not body_text:
        raise HTTPException(status_code=400, detail="Email body is required.")

    if not is_email_configured():
        logger.error("Email sending attempted but Resend environment variables are incomplete.")
        raise HTTPException(
            status_code=500,
            detail="Email is not configured on the server.",
        )

    sender_address = (from_address or EMAIL_FROM_ADDRESS).strip()
    sender_name = (from_name or EMAIL_FROM_NAME).strip()

    if sender_name:
        sender_header = f"{sender_name} <{sender_address}>"
    else:
        sender_header = sender_address

    payload = {
        "from": sender_header,
        "to": [to_email],
        "subject": subject,
        "text": body_text,
        "reply_to": ADMIN_EMAIL,
    }

    if body_html:
        payload["html"] = body_html

    if attachments:
        resend_attachments = []
        for attachment in attachments:
            filename = (attachment.get("filename") or "attachment.bin").strip()
            content_bytes = attachment.get("content_bytes")
            content_type = (attachment.get("content_type") or "application/octet-stream").strip()

            if not isinstance(content_bytes, (bytes, bytearray)):
                continue

            resend_attachments.append(
                {
                    "filename": filename,
                    "content": base64.b64encode(content_bytes).decode("utf-8"),
                    "content_type": content_type,
                }
            )

        if resend_attachments:
            payload["attachments"] = resend_attachments

    try:
        response = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=EMAIL_SEND_TIMEOUT_SECONDS,
        )

        if response.status_code >= 400:
            logger.error(
                "Resend API error status=%s body=%s",
                response.status_code,
                response.text,
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to send email: {response.text}",
            )

        response_json = response.json()

        logger.info(
            "Email sent successfully via Resend to=%s subject=%s resend_id=%s",
            to_email,
            subject,
            response_json.get("id"),
        )

        try:
            db = SessionLocal()
            db.add(
                EmailEvent(
                    resend_email_id=response_json.get("id"),
                    event_type="email.sent",
                    to_email=to_email,
                    subject=subject,
                    raw_json=json.dumps(response_json),
                )
            )
            db.commit()
        except Exception:
            logger.exception("Failed to persist sent email event.")
        finally:
            try:
                db.close()
            except Exception:
                pass

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Failed to send email via Resend to=%s subject=%s",
            to_email,
            subject,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send email: {exc}",
        )

def html_escape(value: Any) -> str:
    if value is None:
        return ""
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )

def safe_wrap_text(value: Any, chunk: int = 24) -> str:
    """
    Force-break long unbroken strings so PDFs never overflow.
    """
    text = html_escape(value)

    if not text:
        return ""

    # Insert zero-width space every N chars
    return "&#8203;".join(
        text[i:i+chunk] for i in range(0, len(text), chunk)
    )

def normalize_currency_symbol(symbol: Optional[str]) -> str:
    symbol = (symbol or "").strip()
    if symbol in {"£", "$", "€", "¥"}:
        return symbol
    return "£"


def detect_currency_symbol_from_series(series: pd.Series) -> str:
    counts = {"£": 0, "$": 0, "€": 0, "¥": 0}

    for raw_value in series.dropna().astype(str):
        for symbol in counts:
            if symbol in raw_value:
                counts[symbol] += 1

    most_common_symbol = max(counts, key=counts.get)
    if counts[most_common_symbol] > 0:
        return most_common_symbol

    return "£"


def get_dashboard_currency_symbol(
    df: pd.DataFrame,
    amount_col: Optional[str],
    fallback: Optional[str] = None,
) -> str:
    normalized_fallback = normalize_currency_symbol(fallback)

    if amount_col and amount_col in df.columns:
        detected = detect_currency_symbol_from_series(df[amount_col])
        if detected:
            return detected

    return normalized_fallback

def is_saved_view_report_enabled(saved_view: SavedView) -> bool:
    return bool(saved_view.report_enabled)

def is_report_currently_locked(saved_view: SavedView, now_utc: datetime) -> bool:
    if not saved_view.last_report_started_at:
        return False

    lock_window = timedelta(minutes=10)
    return saved_view.last_report_started_at > (now_utc - lock_window)

def should_send_scheduled_report(
    saved_view: SavedView,
    now_utc: datetime,
) -> bool:
    if not is_saved_view_report_enabled(saved_view):
        return False

    if not saved_view.report_recipient:
        return False

    frequency = (saved_view.report_frequency or "weekly").strip().lower()
    last_sent = saved_view.last_report_sent_at

    if last_sent is None:
        return True

    elapsed = now_utc - last_sent

    if frequency == "daily":
        return elapsed >= timedelta(days=1)
    if frequency == "weekly":
        return elapsed >= timedelta(days=7)
    if frequency == "monthly":
        return elapsed >= timedelta(days=28)

    return False

def get_next_report_due_at(saved_view: SavedView) -> Optional[datetime]:
    if not is_saved_view_report_enabled(saved_view):
        return None

    if not saved_view.report_recipient:
        return None

    frequency = (saved_view.report_frequency or "weekly").strip().lower()
    last_sent = saved_view.last_report_sent_at

    if last_sent is None:
        return datetime.utcnow()

    if frequency == "daily":
        return last_sent + timedelta(days=1)
    if frequency == "weekly":
        return last_sent + timedelta(days=7)
    if frequency == "monthly":
        return last_sent + timedelta(days=28)

    return None

def get_cloudflare_traffic_summary() -> Dict[str, Any]:
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ZONE_ID:
        return {
            "connected": False,
            "source": "cloudflare",
            "message": "Cloudflare traffic metrics not connected yet.",
        }

    now = datetime.utcnow()
    start = now - timedelta(hours=24)

    query = """
    query GetZoneTraffic($zoneTag: string, $start: Time, $end: Time) {
      viewer {
        zones(filter: { zoneTag: $zoneTag }) {
          traffic: httpRequestsAdaptiveGroups(
            limit: 1
            filter: {
              datetime_geq: $start
              datetime_lt: $end
              requestSource: "eyeball"
            }
          ) {
            count
            sum {
              visits
            }
          }
        }
      }
    }
    """

    variables = {
        "zoneTag": CLOUDFLARE_ZONE_ID,
        "start": start.isoformat() + "Z",
        "end": now.isoformat() + "Z",
    }

    try:
        response = requests.post(
            CLOUDFLARE_GRAPHQL_URL,
            headers={
                "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "query": query,
                "variables": variables,
            },
            timeout=20,
        )
        response.raise_for_status()

        payload = response.json()

        if payload.get("errors"):
            logger.warning("Cloudflare GraphQL returned errors: %s", payload.get("errors"))
            return {
                "connected": False,
                "source": "cloudflare",
                "message": "Cloudflare traffic metrics returned an error.",
            }

        zones = (((payload.get("data") or {}).get("viewer") or {}).get("zones") or [])
        if not zones:
            return {
                "connected": False,
                "source": "cloudflare",
                "message": "Cloudflare zone not found in analytics response.",
            }

        traffic_rows = (zones[0].get("traffic") or [])
        if not traffic_rows:
            return {
                "connected": True,
                "source": "cloudflare",
                "requests_last_24h": 0,
                "visits_last_24h": 0,
                "trend_7d": [],
                "message": "No Cloudflare traffic data available for the last 24 hours.",
            }

        traffic = traffic_rows[0]

        trend_points = []

        # First try Cloudflare daily trend
        try:
            trend_query = """
            query GetZoneTrafficTrend($zoneTag: string, $start: Time, $end: Time) {
              viewer {
                zones(filter: { zoneTag: $zoneTag }) {
                  traffic: httpRequests1dGroups(
                    limit: 7
                    orderBy: [date_ASC]
                    filter: {
                      date_geq: $start
                      date_lt: $end
                    }
                  ) {
                    dimensions {
                      date
                    }
                    sum {
                      requests
                      visits
                    }
                  }
                }
              }
            }
            """

            trend_start = (now - timedelta(days=7)).date().isoformat()
            trend_end = now.date().isoformat()

            trend_response = requests.post(
                CLOUDFLARE_GRAPHQL_URL,
                headers={
                    "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={
                    "query": trend_query,
                    "variables": {
                        "zoneTag": CLOUDFLARE_ZONE_ID,
                        "start": trend_start,
                        "end": trend_end,
                    },
                },
                timeout=20,
            )
            trend_response.raise_for_status()
            trend_data = trend_response.json()

            if trend_data.get("errors"):
                logger.warning("Cloudflare trend query returned errors: %s", trend_data.get("errors"))
            else:
                zones_trend = (((trend_data.get("data") or {}).get("viewer") or {}).get("zones") or [])
                if zones_trend:
                    rows = zones_trend[0].get("traffic") or []
                    for r in rows:
                        trend_points.append(
                            {
                                "date": (r.get("dimensions") or {}).get("date"),
                                "requests": int(((r.get("sum") or {}).get("requests")) or 0),
                                "visits": int(((r.get("sum") or {}).get("visits")) or 0),
                            }
                        )
        except Exception:
            logger.exception("Cloudflare daily trend query failed.")

        # If daily trend is empty, try Cloudflare hourly fallback grouped into days
        if not trend_points:
            try:
                hourly_query = """
                query GetHourlyTraffic($zoneTag: string, $start: Time, $end: Time) {
                  viewer {
                    zones(filter: { zoneTag: $zoneTag }) {
                      traffic: httpRequestsAdaptiveGroups(
                        limit: 168
                        orderBy: [datetime_ASC]
                        filter: {
                          datetime_geq: $start
                          datetime_lt: $end
                        }
                      ) {
                        dimensions {
                          datetime
                        }
                        count
                        sum {
                          visits
                        }
                      }
                    }
                  }
                }
                """

                hourly_response = requests.post(
                    CLOUDFLARE_GRAPHQL_URL,
                    headers={
                        "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "query": hourly_query,
                        "variables": {
                            "zoneTag": CLOUDFLARE_ZONE_ID,
                            "start": (now - timedelta(days=7)).isoformat() + "Z",
                            "end": now.isoformat() + "Z",
                        },
                    },
                    timeout=20,
                )
                hourly_response.raise_for_status()
                hourly_data = hourly_response.json()

                if hourly_data.get("errors"):
                    logger.warning("Cloudflare hourly trend query returned errors: %s", hourly_data.get("errors"))
                else:
                    zones_hourly = (((hourly_data.get("data") or {}).get("viewer") or {}).get("zones") or [])
                    if zones_hourly:
                        buckets = zones_hourly[0].get("traffic") or []

                        daily = {}
                        for r in buckets:
                            dt = (r.get("dimensions") or {}).get("datetime")
                            if not dt:
                                continue

                            day = dt[:10]
                            daily.setdefault(day, {"requests": 0, "visits": 0})
                            daily[day]["requests"] += int(r.get("count") or 0)
                            daily[day]["visits"] += int(((r.get("sum") or {}).get("visits")) or 0)

                        # build full 7-day window (fills missing days)
                        full_trend = []
                        for i in range(7):
                            day = (datetime.utcnow() - timedelta(days=6 - i)).strftime("%Y-%m-%d")
                            values = daily.get(day, {"requests": 0, "visits": 0})

                            full_trend.append({
                                "date": day,
                                "requests": values["requests"],
                                "visits": values["visits"],
                            })

                        trend_points = full_trend
            except Exception:
                logger.exception("Cloudflare hourly fallback trend query failed.")

        # Final fallback: use internal tracked traffic events
        if not trend_points:
            try:
                db = SessionLocal()
                seven_days_ago = datetime.utcnow() - timedelta(days=7)

                rows = (
                    db.query(TrafficEvent)
                    .filter(TrafficEvent.created_at >= seven_days_ago)
                    .all()
                )

                daily = {}
                for r in rows:
                    day = r.created_at.strftime("%Y-%m-%d")
                    daily.setdefault(day, {"requests": 0, "visits": 0})
                    daily[day]["requests"] += 1
                    daily[day]["visits"] += 1

                full_trend = []
                for i in range(7):
                    day = (datetime.utcnow() - timedelta(days=6 - i)).strftime("%Y-%m-%d")
                    values = daily.get(day, {"requests": 0, "visits": 0})

                    full_trend.append(
                        {
                            "date": day,
                            "requests": values["requests"],
                            "visits": values["visits"],
                        }
                    )

                trend_points = full_trend
            except Exception:
                logger.exception("Internal traffic fallback failed.")
            finally:
                try:
                    db.close()
                except Exception:
                    pass

        return {
            "connected": True,
            "source": "cloudflare",
            "requests_last_24h": int(traffic.get("count") or 0),
            "visits_last_24h": int(((traffic.get("sum") or {}).get("visits")) or 0),
            "trend_7d": trend_points,
            "message": "Cloudflare traffic metrics loaded successfully.",
        }

    except Exception:
        logger.exception("Failed to load Cloudflare traffic metrics.")
        return {
            "connected": False,
            "source": "cloudflare",
            "message": "Failed to load Cloudflare traffic metrics.",
        }

def send_saved_view_report_email(
    *,
    saved_view: SavedView,
    dashboard: Dashboard,
) -> None:
    try:
        dashboard_payload = json.loads(dashboard.dashboard_json or "{}")
    except Exception:
        raise HTTPException(status_code=500, detail="Saved dashboard data is invalid.")

    try:
        saved_mapping = json.loads(dashboard.mapping_json or "{}")
    except Exception:
        saved_mapping = {}

    pdf_html = build_dashboard_report_email_html(
        file_name=dashboard.file_name,
        dashboard_payload=dashboard_payload,
        applied_filters=saved_mapping.get("filters", {}) or {},
    )
    pdf_bytes = HTML(string=pdf_html).write_pdf()
    pdf_filename = build_safe_pdf_filename(dashboard.file_name)

    email_body_text = (
        f"Hello,\n\n"
        f"Your Easy-dash dashboard report is attached as a PDF.\n\n"
        f"Dashboard: {dashboard.file_name}\n"
        f"Generated by Easy-dash."
    )

    email_body_html = f"""
        <div style="font-family:Arial,Helvetica,sans-serif;background:#020617;padding:32px;color:#e5e7eb;">
          <div style="max-width:620px;margin:0 auto;border:1px solid #1e293b;border-radius:24px;background:#0f172a;padding:32px;">
            <p style="margin:0 0 12px 0;color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">
              Easy-dash
            </p>
            <h1 style="margin:0 0 12px 0;color:#ffffff;font-size:28px;">Your dashboard report is ready</h1>
            <p style="margin:0;color:#cbd5e1;line-height:1.7;">
              We’ve attached your dashboard report as a PDF.
            </p>
            <p style="margin:18px 0 0 0;color:#94a3b8;font-size:14px;line-height:1.7;">
              Dashboard: <span style="color:#ffffff;">{html_escape(dashboard.file_name)}</span>
            </p>
          </div>
        </div>
    """.strip()

    send_email_message(
        to_email=(saved_view.report_recipient or "").strip(),
        subject=f"Your dashboard report: {dashboard.file_name}",
        body_text=email_body_text,
        body_html=email_body_html,
        from_address=os.getenv("REPORTS_FROM_ADDRESS", EMAIL_FROM_ADDRESS).strip(),
        from_name=os.getenv("REPORTS_FROM_NAME", "Dashboard Reports").strip(),
        attachments=[
            {
                "filename": pdf_filename,
                "content_bytes": pdf_bytes,
                "content_type": "application/pdf",
            }
        ],
    )

def get_email_currency_symbol(dashboard_payload: Dict[str, Any]) -> str:
    summary = dashboard_payload.get("summary", {}) or {}
    return normalize_currency_symbol(summary.get("currency_symbol") or "£")


def format_email_money(value: Any, currency_symbol: str) -> str:
    if not isinstance(value, (int, float)):
        return "—"

    decimals = 0 if currency_symbol == "¥" else 2
    return f"{currency_symbol}{value:,.{decimals}f}"


def extract_email_insights(dashboard_payload: Dict[str, Any]) -> List[Dict[str, str]]:
    raw_smart_insights = dashboard_payload.get("smart_insights") or []
    fallback_insights = dashboard_payload.get("insights") or []

    extracted: List[Dict[str, str]] = []

    if isinstance(raw_smart_insights, list) and raw_smart_insights:
        for item in raw_smart_insights[:4]:
            if isinstance(item, dict):
                extracted.append(
                    {
                        "title": str(item.get("title") or "Insight"),
                        "message": str(item.get("message") or ""),
                    }
                )
            elif isinstance(item, str):
                extracted.append(
                    {
                        "title": "Insight",
                        "message": item,
                    }
                )

    if extracted:
        return extracted

    for item in fallback_insights[:4]:
        if isinstance(item, str):
            extracted.append(
                {
                    "title": "Insight",
                    "message": item,
                }
            )

    return extracted

def build_dashboard_report_email_text(
    file_name: str,
    dashboard_payload: Dict[str, Any],
) -> str:
    summary = dashboard_payload.get("summary", {}) or {}
    charts = dashboard_payload.get("charts", {}) or {}
    currency_symbol = get_email_currency_symbol(dashboard_payload)

    total_revenue = summary.get("total_revenue")
    num_sales = summary.get("num_sales")
    average_sale = summary.get("average_sale")
    date_range = summary.get("date_range", {}) or {}

    top_products = (charts.get("top_products") or [])[:5]
    top_customers = (charts.get("top_customers") or [])[:5]
    email_insights = extract_email_insights(dashboard_payload)

    top_customer_name = top_customers[0].get("customer") if top_customers else "—"

    lines = [
        "Hello,",
        "",
        f"Here is your Easy-dash report for: {file_name}",
        "",
        "SUMMARY",
        f"- Total revenue: {format_email_money(total_revenue, currency_symbol)}",
        f"- Orders: {num_sales:,}" if isinstance(num_sales, int) else f"- Orders: {num_sales}" if num_sales is not None else "- Orders: —",
        f"- Average order value: {format_email_money(average_sale, currency_symbol)}",
        f"- Top customer: {top_customer_name or '—'}",
        f"- Date range: {date_range.get('start', '—')} to {date_range.get('end', '—')}",
        "",
        "TOP PRODUCTS",
    ]

    if top_products:
        for item in top_products:
            product_name = item.get("product") or "Unknown product"
            revenue_text = format_email_money(item.get("revenue"), currency_symbol)
            lines.append(f"- {product_name}: {revenue_text}")
    else:
        lines.append("- No product data available")

    lines.extend([
        "",
        "TOP CUSTOMERS",
    ])

    if top_customers:
        for item in top_customers:
            customer_name = item.get("customer") or "Unknown customer"
            revenue_text = format_email_money(item.get("revenue"), currency_symbol)
            lines.append(f"- {customer_name}: {revenue_text}")
    else:
        lines.append("- No customer data available")

    lines.extend([
        "",
        "KEY INSIGHTS",
    ])

    if email_insights:
        for insight in email_insights:
            title = insight.get("title") or "Insight"
            message = insight.get("message") or ""
            lines.append(f"- {title}: {message}")
    else:
        lines.append("- No insights available")

    lines.extend([
        "",
        "Generated by Easy-dash.",
    ])

    return "\n".join(lines)

def build_dashboard_report_email_html(
    file_name: str,
    dashboard_payload: Dict[str, Any],
    applied_filters: Optional[Dict[str, Any]] = None,
) -> str:
    summary = dashboard_payload.get("summary", {}) or {}
    charts = dashboard_payload.get("charts", {}) or {}
    currency_symbol = get_email_currency_symbol(dashboard_payload)

    total_revenue = summary.get("total_revenue")
    num_sales = summary.get("num_sales")
    average_sale = summary.get("average_sale")
    date_range = summary.get("date_range", {}) or {}

    top_products = (charts.get("top_products") or [])[:5]
    top_customers = (charts.get("top_customers") or [])[:5]
    email_insights = extract_email_insights(dashboard_payload)

    revenue_over_time = (
        charts.get("revenue_over_time") or {}
        if isinstance(charts.get("revenue_over_time"), dict)
        else {}
    )
    revenue_points = revenue_over_time.get("points") or []
    revenue_grouping = str(revenue_over_time.get("grouping") or "period").strip().lower()

    revenue_text = format_email_money(total_revenue, currency_symbol)
    orders_text = (
        f"{num_sales:,}" if isinstance(num_sales, int)
        else str(num_sales) if num_sales is not None
        else "—"
    )
    aov_text = format_email_money(average_sale, currency_symbol)
    top_customer_text = html_escape(top_customers[0].get("customer") if top_customers else "—")
    date_range_text = f"{date_range.get('start', '—')} to {date_range.get('end', '—')}"

    top_products_rows = ""
    if top_products:
        for index, item in enumerate(top_products, start=1):
            product_name = safe_wrap_text(item.get("product") or "Unknown product")
            revenue_value = html_escape(format_email_money(item.get("revenue"), currency_symbol))
            top_products_rows += f"""
                <tr>
                    <td class="row-shell" colspan="3">
                        <table role="presentation" class="data-row-table">
                            <tr>
                                <td class="data-rank">{index}</td>
                                <td class="data-label">{product_name}</td>
                                <td class="data-value">{revenue_value}</td>
                            </tr>
                        </table>
                    </td>
                </tr>
            """
    else:
        top_products_rows = """
            <tr>
                <td colspan="3" class="empty-cell">No product data available</td>
            </tr>
        """

    top_customers_rows = ""
    if top_customers:
        for index, item in enumerate(top_customers, start=1):
            customer_name = safe_wrap_text(item.get("customer") or "Unknown customer")
            revenue_value = html_escape(format_email_money(item.get("revenue"), currency_symbol))
            top_customers_rows += f"""
                <tr>
                    <td class="row-shell" colspan="3">
                        <table role="presentation" class="data-row-table">
                            <tr>
                                <td class="data-rank">{index}</td>
                                <td class="data-label">{customer_name}</td>
                                <td class="data-value">{revenue_value}</td>
                            </tr>
                        </table>
                    </td>
                </tr>
            """
    else:
        top_customers_rows = """
            <tr>
                <td colspan="3" class="empty-cell">No customer data available</td>
            </tr>
        """

    insights_html = ""
    if email_insights:
        for index, insight in enumerate(email_insights[:3]):
            title = html_escape(insight.get("title") or "Insight")
            message = html_escape(insight.get("message") or "")
            modifier_class = " insight-primary" if index == 0 else ""
            insights_html += f"""
                <div class="insight-card{modifier_class}">
                    <div class="insight-title">{title}</div>
                    <div class="insight-message">{message}</div>
                </div>
            """
    else:
        insights_html = """
            <div class="insight-card">
                <div class="insight-message">No insights available</div>
            </div>
        """

    def _short_period_label(value: Any, grouping: str) -> str:
        text = str(value or "").strip()
        if not text:
            return "—"

        if grouping == "day":
            try:
                dt = datetime.fromisoformat(text)
                return dt.strftime("%b %d")
            except Exception:
                return text[-5:] if len(text) > 5 else text

        if grouping == "week":
            return text[-7:] if len(text) > 7 else text

        if grouping == "month":
            try:
                dt = datetime.fromisoformat(f"{text}-01")
                return dt.strftime("%b")
            except Exception:
                return text[-7:] if len(text) > 7 else text

        return text[-8:] if len(text) > 8 else text

        chart_panel_html = """
        <div class="empty-chart-state">No revenue trend data available</div>
    """

    if isinstance(revenue_points, list) and revenue_points:
        trimmed_points = revenue_points[-7:]

        values: List[float] = []
        labels: List[str] = []

        for item in trimmed_points:
            if not isinstance(item, dict):
                continue

            revenue_value = item.get("revenue")
            try:
                numeric_value = float(revenue_value or 0)
            except Exception:
                numeric_value = 0.0

            values.append(max(numeric_value, 0.0))
            labels.append(_short_period_label(item.get("period"), revenue_grouping))

        if values:
            max_value = max(values) or 1.0

            chart_width = 520
            chart_height = 180
            left_pad = 42
            right_pad = 12
            top_pad = 16
            bottom_pad = 34
            plot_width = chart_width - left_pad - right_pad
            plot_height = chart_height - top_pad - bottom_pad

            count = len(values)
            gap = 14
            bar_width = max(24, int((plot_width - (gap * max(count - 1, 0))) / max(count, 1)))
            total_bars_width = (count * bar_width) + (gap * max(count - 1, 0))
            start_x = left_pad + max(0, (plot_width - total_bars_width) / 2)

            grid_lines_svg = ""
            y_axis_labels_svg = ""

            y_ticks = 4
            rounded_top = int(math.ceil(max_value / 10.0) * 10)
            if rounded_top <= 0:
                rounded_top = 10

            for step in range(y_ticks + 1):
                y = top_pad + (plot_height * step / y_ticks)
                tick_value = rounded_top - (rounded_top * step / y_ticks)

                grid_lines_svg += f'''
                    <line x1="{left_pad}" y1="{y:.1f}" x2="{chart_width - right_pad}" y2="{y:.1f}"
                          stroke="#17304A" stroke-width="1" opacity="0.65" />
                '''

                y_axis_labels_svg += f'''
                    <text x="{left_pad - 8}" y="{y + 3:.1f}" text-anchor="end"
                          font-size="9" fill="#7C8CA5">{int(round(tick_value))}</text>
                '''

            bars_svg = ""
            labels_svg = ""

            for idx, value in enumerate(values):
                bar_height = 0 if max_value == 0 else (value / max_value) * plot_height
                x = start_x + idx * (bar_width + gap)
                y = top_pad + (plot_height - bar_height)
                label_x = x + (bar_width / 2)

                bars_svg += f'''
                    <rect x="{x:.1f}" y="{y:.1f}" width="{bar_width}" height="{bar_height:.1f}"
                          rx="8" ry="8" fill="url(#barGradient)" />
                '''

                labels_svg += f'''
                    <text x="{label_x:.1f}" y="{chart_height - 10}" text-anchor="middle"
                          font-size="9.5" fill="#94A3B8">{html_escape(labels[idx])}</text>
                '''

            chart_panel_html = f"""
                <div class="svg-chart-wrap">
                  <svg viewBox="0 0 {chart_width} {chart_height}" class="svg-bar-chart" role="img" aria-label="Revenue trend chart">
                    <defs>
                      <linearGradient id="barGradient" x1="0" y1="1" x2="0" y2="0">
                        <stop offset="0%" stop-color="#58B8D4"></stop>
                        <stop offset="100%" stop-color="#76C98D"></stop>
                      </linearGradient>
                    </defs>
                    {grid_lines_svg}
                    {y_axis_labels_svg}
                    <line x1="{left_pad}" y1="{top_pad + plot_height:.1f}" x2="{chart_width - right_pad}" y2="{top_pad + plot_height:.1f}"
                          stroke="#23405D" stroke-width="1.2" />
                    {bars_svg}
                    {labels_svg}
                  </svg>
                </div>
            """

    filters = applied_filters or {}

    def _normalize_list(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [v.strip() for v in value.split("|") if v.strip()]
        return []

    active_filters: List[Tuple[str, str]] = []

    filter_start = str(filters.get("start_date") or filters.get("filter_start_date") or "").strip()
    filter_end = str(filters.get("end_date") or filters.get("filter_end_date") or "").strip()
    if filter_start or filter_end:
        if filter_start and filter_end:
            active_filters.append(("Date", f"{filter_start} → {filter_end}"))
        elif filter_start:
            active_filters.append(("Date", f"From {filter_start}"))
        elif filter_end:
            active_filters.append(("Date", f"Up to {filter_end}"))

    selected_products = _normalize_list(filters.get("product") or filters.get("filter_product"))
    if selected_products:
        active_filters.append(("Product", ", ".join(selected_products[:3])))

    selected_customers = _normalize_list(filters.get("customer") or filters.get("filter_customer"))
    if selected_customers:
        active_filters.append(("Customer", ", ".join(selected_customers[:3])))

    comparison_enabled = bool(filters.get("comparison_enabled"))
    comparison_mode = str(filters.get("comparison_mode") or "").strip()
    comparison_preset = str(filters.get("comparison_preset") or "").strip()
    if comparison_enabled:
        if comparison_preset:
            active_filters.append(("Compare", comparison_preset.replace("_", " ").title()))
        elif comparison_mode and comparison_mode != "none":
            active_filters.append(("Compare", comparison_mode.replace("_", " ").title()))

    applied_filters_html = ""
    if active_filters:
        chips_html = ""
        for label, value in active_filters:
            chips_html += f"""
                <div class="filter-chip">
                  <span class="filter-chip-label">{html_escape(label)}:</span>
                  <span class="filter-chip-value">{html_escape(value)}</span>
                </div>
            """

        applied_filters_html = f"""
          <div class="filters-panel">
            <div class="filters-title">Applied filters</div>
            <div class="filters-row">
              {chips_html}
            </div>
          </div>
        """

    chart_subtitle = f"Recent revenue by {html_escape(revenue_grouping)}"

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Easy-dash Dashboard Report</title>
  <style>
    @page {{
      size: A4;
      margin: 7mm;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      padding: 0;
      background: #020617;
      color: #e5e7eb;
      font-family: Arial, Helvetica, sans-serif;
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }}

    .page {{
      width: 100%;
    }}

    .shell {{
      background: linear-gradient(180deg, #0b1120 0%, #020617 100%);
      border: 1px solid #1e293b;
      border-radius: 18px;
      overflow: hidden;
    }}

    .hero {{
      background: linear-gradient(135deg, #0f172a 0%, #020617 100%);
      padding: 16px 18px 12px 18px;
    }}

    .badge {{
      display: inline-block;
      padding: 5px 9px;
      border-radius: 999px;
      background: rgba(16, 185, 129, 0.10);
      border: 1px solid rgba(16, 185, 129, 0.30);
      color: #a7f3d0;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}

    .hero h1 {{
      margin: 12px 0 6px 0;
      color: #ffffff;
      font-size: 24px;
      line-height: 1.1;
      font-weight: 700;
    }}

    .hero .subtle {{
      margin: 0;
      color: #94a3b8;
      font-size: 13px;
      line-height: 1.35;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .content {{
      padding: 12px;
    }}

    .stats-grid {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 6px;
      table-layout: fixed;
      margin-bottom: 10px;
    }}

    .stats-grid td {{
      vertical-align: top;
      background: #0f172a;
      border: 1px solid #1e293b;
      border-radius: 12px;
      padding: 8px;
      overflow: hidden;
    }}

    .stat-label {{
      color: #94a3b8;
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
      line-height: 1.2;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .stat-value {{
      color: #ffffff;
      font-size: 18px;
      font-weight: 700;
      line-height: 1.15;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .two-col {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      margin-bottom: 10px;
    }}

    .two-col td {{
      width: 50%;
      vertical-align: top;
    }}

    .col-left {{
      padding-right: 6px;
    }}

    .col-right {{
      padding-left: 6px;
    }}

    .panel {{
      background: #0b1220;
      border: 1px solid #1e293b;
      border-radius: 16px;
      overflow: hidden;
      page-break-inside: avoid;
    }}

    .panel-head {{
      padding: 12px 14px 9px 14px;
      border-bottom: 1px solid #1e293b;
      background: #0f172a;
    }}

    .panel-title {{
      margin: 0;
      color: #ffffff;
      font-size: 16px;
      font-weight: 700;
      line-height: 1.15;
    }}

    .panel-subtitle {{
      margin: 4px 0 0 0;
      color: #94a3b8;
      font-size: 11px;
      line-height: 1.3;
    }}

    .table-wrap {{
      padding: 4px 8px 8px 8px;
    }}

    table.data-table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0 4px;
      table-layout: fixed;
    }}

    .data-table td {{
      background: #111827;
      font-size: 11px;
      padding: 6px 8px;
      border-top: 1px solid #1f2937;
      border-bottom: 1px solid #1f2937;
      vertical-align: middle;
      line-height: 1.2;
    }}

    .data-table tr td:first-child {{
      border-left: 1px solid #1f2937;
      border-top-left-radius: 10px;
      border-bottom-left-radius: 10px;
    }}

    .data-table tr td:last-child {{
      border-right: 1px solid #1f2937;
      border-top-right-radius: 10px;
      border-bottom-right-radius: 10px;
    }}

    .rank {{
      width: 12%;
      text-align: center;
      color: #94a3b8;
      font-weight: 700;
      white-space: nowrap;
    }}

    .label-cell {{
      width: 58%;
      color: #e5e7eb;
      font-weight: 600;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}

    .value-cell {{
      width: 30%;
      text-align: right;
      color: #ffffff;
      font-weight: 700;
      white-space: nowrap;
    }}

    .empty-cell {{
      text-align: center;
      color: #94a3b8;
      padding: 14px 10px !important;
    }}

    .bottom-row {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      margin-bottom: 8px;
    }}

    .bottom-row td {{
      width: 50%;
      vertical-align: top;
    }}

    .bottom-left {{
      padding-right: 6px;
    }}

    .bottom-right {{
      padding-left: 6px;
    }}

    .insights-panel {{
      background: #0b1220;
      border: 1px solid #1e293b;
      border-radius: 16px;
      overflow: hidden;
      page-break-inside: avoid;
    }}

    .insights-body {{
      padding: 8px;
    }}

    .insight-card {{
      margin-bottom: 8px;
      padding: 10px 12px;
      border: 1px solid #1f2937;
      border-radius: 12px;
      background: #111827;
      page-break-inside: avoid;
      overflow: hidden;
    }}

    .insight-card:last-child {{
      margin-bottom: 0;
    }}

    .insight-card.insight-primary {{
      background: rgba(245, 158, 11, 0.10);
      border-color: rgba(245, 158, 11, 0.35);
    }}

    .insight-title {{
      margin-bottom: 5px;
      color: #ffffff;
      font-size: 12px;
      font-weight: 700;
      line-height: 1.25;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .insight-message {{
      color: #cbd5e1;
      font-size: 11px;
      line-height: 1.35;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .chart-panel {{
      background: #0b1220;
      border: 1px solid #1e293b;
      border-radius: 16px;
      overflow: hidden;
      page-break-inside: avoid;
      min-height: 100%;
    }}

    .chart-body {{
      padding: 8px 10px 8px 10px;
    }}

    .svg-chart-wrap {{
      width: 100%;
      border: 1px solid #162133;
      border-radius: 12px;
      background: linear-gradient(180deg, rgba(15, 23, 42, 0.78) 0%, rgba(2, 6, 23, 0.98) 100%);
      padding: 6px;
    }}

    .svg-bar-chart {{
      display: block;
      width: 100%;
      height: 188px;
    }}

    .empty-chart-state {{
      padding: 58px 10px;
      text-align: center;
      color: #94a3b8;
      font-size: 11px;
      line-height: 1.4;
    }}

    .filters-panel {{
      margin-top: 2px;
      margin-bottom: 6px;
      padding: 9px 10px;
      border: 1px solid #1e293b;
      border-radius: 14px;
      background: #0b1220;
      overflow: hidden;
    }}

    .filters-title {{
      margin-bottom: 7px;
      color: #94a3b8;
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .filters-row {{
      font-size: 0;
    }}

    .filter-chip {{
      display: inline-block;
      max-width: 100%;
      margin-right: 6px;
      margin-bottom: 6px;
      padding: 6px 9px;
      border-radius: 999px;
      background: #111827;
      border: 1px solid #1f2937;
      color: #cbd5e1;
      font-size: 10px;
      line-height: 1.25;
      vertical-align: top;
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .filter-chip-label {{
      color: #94a3b8;
      font-weight: 700;
      margin-right: 4px;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .filter-chip-value {{
      color: #ffffff;
      font-weight: 600;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .footer-inline {{
      margin-top: 4px;
      padding-top: 7px;
      border-top: 1px solid #1e293b;
      color: #94a3b8;
      font-size: 10px;
      line-height: 1.4;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="shell">
      <div class="hero">
        <div class="badge">Easy-dash</div>
        <h1>Dashboard report</h1>
        <p class="subtle">{html_escape(file_name)}</p>
        <p class="subtle">Date range: {html_escape(date_range_text)}</p>
      </div>

      <div class="content">
        <table class="stats-grid" role="presentation">
          <tr>
            <td>
              <div class="stat-label">Total revenue</div>
              <div class="stat-value">{html_escape(revenue_text)}</div>
            </td>
            <td>
              <div class="stat-label">Orders</div>
              <div class="stat-value">{html_escape(orders_text)}</div>
            </td>
            <td>
              <div class="stat-label">Average order value</div>
              <div class="stat-value">{html_escape(aov_text)}</div>
            </td>
            <td>
              <div class="stat-label">Top customer</div>
              <div class="stat-value">{top_customer_text}</div>
            </td>
          </tr>
        </table>

        <table class="two-col" role="presentation">
          <tr>
            <td class="col-left">
              <div class="panel">
                <div class="panel-head">
                  <h2 class="panel-title">Top products</h2>
                  <p class="panel-subtitle">Highest-performing products by revenue</p>
                </div>
                <div class="table-wrap">
                  <table class="data-table" role="presentation">
                    {top_products_rows}
                  </table>
                </div>
              </div>
            </td>
            <td class="col-right">
              <div class="panel">
                <div class="panel-head">
                  <h2 class="panel-title">Top customers</h2>
                  <p class="panel-subtitle">Customer performance ranking</p>
                </div>
                <div class="table-wrap">
                  <table class="data-table" role="presentation">
                    {top_customers_rows}
                  </table>
                </div>
              </div>
            </td>
          </tr>
        </table>

        <table class="bottom-row" role="presentation">
          <tr>
            <td class="bottom-left">
              <div class="insights-panel">
                <div class="panel-head">
                  <h2 class="panel-title">Key insights</h2>
                  <p class="panel-subtitle">Smart takeaways from your current dashboard view</p>
                </div>
                <div class="insights-body">
                  {insights_html}
                </div>
              </div>
            </td>
            <td class="bottom-right">
              <div class="chart-panel">
                <div class="panel-head">
                  <h2 class="panel-title">Revenue trend</h2>
                  <p class="panel-subtitle">{chart_subtitle}</p>
                </div>
                <div class="chart-body">
                  {chart_panel_html}
                </div>
              </div>
            </td>
          </tr>
        </table>

        {applied_filters_html}

        <div class="footer-inline">
          Generated by Easy-dash.
        </div>
      </div>
    </div>
  </div>
</body>
</html>
    """.strip()

def build_dashboard_report_email_html_for_email(
    file_name: str,
    dashboard_payload: Dict[str, Any],
    applied_filters: Optional[Dict[str, Any]] = None,
) -> str:
    summary = dashboard_payload.get("summary", {}) or {}
    charts = dashboard_payload.get("charts", {}) or {}
    currency_symbol = get_email_currency_symbol(dashboard_payload)

    total_revenue = summary.get("total_revenue")
    num_sales = summary.get("num_sales")
    average_sale = summary.get("average_sale")
    date_range = summary.get("date_range", {}) or {}

    top_products = (charts.get("top_products") or [])[:5]
    top_customers = (charts.get("top_customers") or [])[:5]
    email_insights = extract_email_insights(dashboard_payload)

    revenue_over_time = (
        charts.get("revenue_over_time") or {}
        if isinstance(charts.get("revenue_over_time"), dict)
        else {}
    )
    revenue_points = revenue_over_time.get("points") or []
    revenue_grouping = str(revenue_over_time.get("grouping") or "period").strip().lower()

    revenue_text = format_email_money(total_revenue, currency_symbol)
    orders_text = (
        f"{num_sales:,}" if isinstance(num_sales, int)
        else str(num_sales) if num_sales is not None
        else "—"
    )
    aov_text = format_email_money(average_sale, currency_symbol)
    top_customer_text = html_escape(top_customers[0].get("customer") if top_customers else "—")
    date_range_text = f"{date_range.get('start', '—')} to {date_range.get('end', '—')}"

    top_products_rows = ""
    if top_products:
        for index, item in enumerate(top_products, start=1):
            product_name = html_escape(item.get("product") or "Unknown product")
            revenue_value = html_escape(format_email_money(item.get("revenue"), currency_symbol))
            top_products_rows += f"""
                <tr>
                    <td class="rank">{index}</td>
                    <td class="label-cell">{product_name}</td>
                    <td class="value-cell">{revenue_value}</td>
                </tr>
            """
    else:
        top_products_rows = """
            <tr>
                <td colspan="3" class="empty-cell">No product data available</td>
            </tr>
        """

    top_customers_rows = ""
    if top_customers:
        for index, item in enumerate(top_customers, start=1):
            customer_name = html_escape(item.get("customer") or "Unknown customer")
            revenue_value = html_escape(format_email_money(item.get("revenue"), currency_symbol))
            top_customers_rows += f"""
                <tr>
                    <td class="rank">{index}</td>
                    <td class="label-cell">{customer_name}</td>
                    <td class="value-cell">{revenue_value}</td>
                </tr>
            """
    else:
        top_customers_rows = """
            <tr>
                <td colspan="3" class="empty-cell">No customer data available</td>
            </tr>
        """

    insights_html = ""
    if email_insights:
        for index, insight in enumerate(email_insights[:3]):
            title = html_escape(insight.get("title") or "Insight")
            message = html_escape(insight.get("message") or "")
            modifier_class = " insight-primary" if index == 0 else ""
            insights_html += f"""
                <div class="insight-card{modifier_class}">
                    <div class="insight-title">{title}</div>
                    <div class="insight-message">{message}</div>
                </div>
            """
    else:
        insights_html = """
            <div class="insight-card">
                <div class="insight-message">No insights available</div>
            </div>
        """

    def _short_period_label(value: Any, grouping: str) -> str:
        text = str(value or "").strip()
        if not text:
            return "—"

        if grouping == "day":
            try:
                dt = datetime.fromisoformat(text)
                return dt.strftime("%b %d")
            except Exception:
                return text[-5:] if len(text) > 5 else text

        if grouping == "week":
            return text[-7:] if len(text) > 7 else text

        if grouping == "month":
            try:
                dt = datetime.fromisoformat(f"{text}-01")
                return dt.strftime("%b")
            except Exception:
                return text[-7:] if len(text) > 7 else text

        return text[-8:] if len(text) > 8 else text

    chart_panel_html = """
        <div class="empty-chart-state">No revenue trend data available</div>
    """

    if isinstance(revenue_points, list) and revenue_points:
        trimmed_points = revenue_points[-7:]

        values: List[float] = []
        labels: List[str] = []

        for item in trimmed_points:
            if not isinstance(item, dict):
                continue

            revenue_value = item.get("revenue")
            try:
                numeric_value = float(revenue_value or 0)
            except Exception:
                numeric_value = 0.0

            values.append(max(numeric_value, 0.0))
            labels.append(_short_period_label(item.get("period"), revenue_grouping))

        if values:
            max_value = max(values) or 1.0
            latest_value_text = html_escape(format_email_money(values[-1], currency_symbol))

            rows_html = ""
            for idx, value in enumerate(values):
                width_pct = 0 if max_value == 0 else max(10, round((value / max_value) * 100))
                rows_html += f"""
                    <tr>
                      <td class="email-chart-label">{html_escape(labels[idx])}</td>
                      <td class="email-chart-bar-cell">
                        <table role="presentation" class="email-chart-bar-table">
                          <tr>
                            <td class="email-chart-bar-fill" style="width:{width_pct}%;"></td>
                            <td class="email-chart-bar-rest"></td>
                          </tr>
                        </table>
                      </td>
                      <td class="email-chart-value">{html_escape(format_email_money(value, currency_symbol))}</td>
                    </tr>
                """

            chart_panel_html = f"""
                <div class="chart-summary-row">
                  <div class="chart-summary-label">Latest</div>
                  <div class="chart-summary-value">{latest_value_text}</div>
                </div>
                <table role="presentation" class="email-chart-table">
                  {rows_html}
                </table>
            """

    filters = applied_filters or {}

    def _normalize_list(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [v.strip() for v in value.split("|") if v.strip()]
        return []

    active_filters: List[Tuple[str, str]] = []

    filter_start = str(filters.get("start_date") or filters.get("filter_start_date") or "").strip()
    filter_end = str(filters.get("end_date") or filters.get("filter_end_date") or "").strip()
    if filter_start or filter_end:
        if filter_start and filter_end:
            active_filters.append(("Date", f"{filter_start} → {filter_end}"))
        elif filter_start:
            active_filters.append(("Date", f"From {filter_start}"))
        elif filter_end:
            active_filters.append(("Date", f"Up to {filter_end}"))

    selected_products = _normalize_list(filters.get("product") or filters.get("filter_product"))
    if selected_products:
        active_filters.append(("Product", ", ".join(selected_products[:3])))

    selected_customers = _normalize_list(filters.get("customer") or filters.get("filter_customer"))
    if selected_customers:
        active_filters.append(("Customer", ", ".join(selected_customers[:3])))

    comparison_enabled = bool(filters.get("comparison_enabled"))
    comparison_mode = str(filters.get("comparison_mode") or "").strip()
    comparison_preset = str(filters.get("comparison_preset") or "").strip()
    if comparison_enabled:
        if comparison_preset:
            active_filters.append(("Compare", comparison_preset.replace("_", " ").title()))
        elif comparison_mode and comparison_mode != "none":
            active_filters.append(("Compare", comparison_mode.replace("_", " ").title()))

    applied_filters_html = ""
    if active_filters:
        chips_html = ""
        for label, value in active_filters:
            chips_html += f"""
                <div class="filter-chip">
                  <span class="filter-chip-label">{html_escape(label)}:</span>
                  <span class="filter-chip-value">{html_escape(value)}</span>
                </div>
            """

        applied_filters_html = f"""
          <div class="filters-panel">
            <div class="filters-title">Applied filters</div>
            <div class="filters-row">
              {chips_html}
            </div>
          </div>
        """

    chart_subtitle = f"Recent revenue by {html_escape(revenue_grouping)}"

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <title>Easy-dash Dashboard Report</title>
  <style>
    body {{
      margin: 0;
      padding: 0;
      background: #020617;
      color: #e5e7eb;
      font-family: Arial, Helvetica, sans-serif;
      -webkit-print-color-adjust: exact;
      print-color-adjust: exact;
    }}

    .page {{
      width: 100%;
      background: #020617;
      padding: 20px 0;
    }}

    .shell {{
      max-width: 960px;
      margin: 0 auto;
      background: linear-gradient(180deg, #0b1120 0%, #020617 100%);
      border: 1px solid #1e293b;
      border-radius: 18px;
      overflow: hidden;
    }}

    .hero {{
      background: linear-gradient(135deg, #0f172a 0%, #020617 100%);
      padding: 16px 18px 12px 18px;
    }}

    .badge {{
      display: inline-block;
      padding: 5px 9px;
      border-radius: 999px;
      background: rgba(16, 185, 129, 0.10);
      border: 1px solid rgba(16, 185, 129, 0.30);
      color: #a7f3d0;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}

    .hero h1 {{
      margin: 12px 0 6px 0;
      color: #ffffff;
      font-size: 24px;
      line-height: 1.1;
      font-weight: 700;
    }}

    .hero .subtle {{
      margin: 0;
      color: #94a3b8;
      font-size: 13px;
      line-height: 1.35;
    }}

    .content {{
      padding: 12px;
    }}

    .stats-grid {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 6px;
      table-layout: fixed;
      margin-bottom: 10px;
    }}

    .stats-grid td {{
      vertical-align: top;
      background: #0f172a;
      border: 1px solid #1e293b;
      border-radius: 12px;
      padding: 8px;
    }}

    .stat-label {{
      color: #94a3b8;
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 6px;
      line-height: 1.2;
    }}

    .stat-value {{
      color: #ffffff;
      font-size: 18px;
      font-weight: 700;
      line-height: 1.08;
      word-break: break-word;
    }}

    .two-col {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      margin-bottom: 10px;
    }}

    .two-col td {{
      width: 50%;
      vertical-align: top;
    }}

    .col-left {{
      padding-right: 6px;
    }}

    .col-right {{
      padding-left: 6px;
    }}

    .panel {{
      background: #0b1220;
      border: 1px solid #1e293b;
      border-radius: 16px;
      overflow: hidden;
    }}

    .panel-head {{
      padding: 12px 14px 9px 14px;
      border-bottom: 1px solid #1e293b;
      background: #0f172a;
    }}

    .panel-title {{
      margin: 0;
      color: #ffffff;
      font-size: 16px;
      font-weight: 700;
      line-height: 1.15;
    }}

    .panel-subtitle {{
      margin: 4px 0 0 0;
      color: #94a3b8;
      font-size: 11px;
      line-height: 1.3;
    }}

    .table-wrap {{
      padding: 4px 8px 8px 8px;
    }}

    table.data-table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0 6px;
      table-layout: fixed;
    }}

    .data-table td {{
      font-size: 11px;
      vertical-align: middle;
    }}

    .row-shell {{
      padding: 0 !important;
      background: transparent !important;
      border: 0 !important;
    }}

    .data-row-table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      table-layout: fixed;
      background: #111827;
      border: 1px solid #1f2937;
      border-radius: 10px;
      overflow: hidden;
    }}

    .data-rank {{
      width: 14%;
      padding: 8px 6px;
      text-align: center;
      color: #94a3b8;
      font-weight: 700;
      white-space: nowrap;
    }}

    .data-label {{
      width: 56%;
      padding: 8px 8px;
      color: #e5e7eb;
      font-weight: 600;
      line-height: 1.25;
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .data-value {{
      width: 30%;
      padding: 8px 10px;
      text-align: right;
      color: #ffffff;
      font-weight: 700;
      white-space: nowrap;
    }}

    .empty-cell {{
      text-align: center;
      color: #94a3b8;
      padding: 14px 10px !important;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .bottom-row {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      margin-bottom: 8px;
    }}

    .bottom-row td {{
      width: 50%;
      vertical-align: top;
    }}

    .bottom-left {{
      padding-right: 6px;
    }}

    .bottom-right {{
      padding-left: 6px;
    }}

    .insights-panel {{
      background: #0b1220;
      border: 1px solid #1e293b;
      border-radius: 16px;
      overflow: hidden;
    }}

    .insights-body {{
      padding: 8px;
    }}

    .insight-card {{
      margin-bottom: 8px;
      padding: 10px 12px;
      border: 1px solid #1f2937;
      border-radius: 12px;
      background: #111827;
    }}

    .insight-card:last-child {{
      margin-bottom: 0;
    }}

    .insight-card.insight-primary {{
      background: rgba(245, 158, 11, 0.10);
      border-color: rgba(245, 158, 11, 0.35);
    }}

    .insight-title {{
      margin-bottom: 5px;
      color: #ffffff;
      font-size: 12px;
      font-weight: 700;
      line-height: 1.25;
    }}

    .insight-message {{
      color: #cbd5e1;
      font-size: 11px;
      line-height: 1.3;
    }}

    .chart-panel {{
      background: #0b1220;
      border: 1px solid #1e293b;
      border-radius: 16px;
      overflow: hidden;
    }}

    .chart-body {{
      padding: 10px;
    }}

    .chart-summary-row {{
      display: none;
    }}

    .chart-summary-label {{
      display: none;
    }}

    .chart-summary-value {{
      display: none;
    }}

    .email-chart-table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0 8px;
      table-layout: fixed;
    }}

    .email-chart-label {{
      width: 66px;
      color: #94a3b8;
      font-size: 10px;
      vertical-align: middle;
      white-space: nowrap;
      padding-right: 8px;
    }}

    .email-chart-bar-cell {{
      vertical-align: middle;
    }}

    .email-chart-bar-table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      height: 12px;
      background: #0f172a;
      border: 1px solid #1e293b;
      border-radius: 999px;
      overflow: hidden;
    }}

    .email-chart-bar-fill {{
      background: #10b981;
      height: 12px;
      font-size: 0;
      line-height: 0;
    }}

    .email-chart-bar-rest {{
      background: transparent;
      height: 12px;
      font-size: 0;
      line-height: 0;
    }}

    .email-chart-value {{
      width: 84px;
      color: #ffffff;
      font-size: 10px;
      font-weight: 700;
      text-align: right;
      vertical-align: middle;
      white-space: nowrap;
      padding-left: 8px;
    }}

    .empty-chart-state {{
      padding: 58px 10px;
      text-align: center;
      color: #94a3b8;
      font-size: 11px;
      line-height: 1.4;
    }}

    .filters-panel {{
      margin-top: 2px;
      margin-bottom: 6px;
      padding: 9px 10px;
      border: 1px solid #1e293b;
      border-radius: 14px;
      background: #0b1220;
    }}

    .filters-title {{
      margin-bottom: 7px;
      color: #94a3b8;
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .filters-row {{
      font-size: 0;
    }}

    .filter-chip {{
      display: inline-block;
      margin-right: 6px;
      margin-bottom: 6px;
      padding: 6px 9px;
      border-radius: 999px;
      background: #111827;
      border: 1px solid #1f2937;
      color: #cbd5e1;
      font-size: 10px;
      line-height: 1.2;
      vertical-align: top;
    }}

    .filter-chip-label {{
      color: #94a3b8;
      font-weight: 700;
      margin-right: 4px;
    }}

    .filter-chip-value {{
      color: #ffffff;
      font-weight: 600;
    }}

    .footer-inline {{
      margin-top: 4px;
      padding-top: 7px;
      border-top: 1px solid #1e293b;
      color: #94a3b8;
      font-size: 10px;
      line-height: 1.4;
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="shell">
      <div class="hero">
        <div class="badge">Easy-dash</div>
        <h1>Dashboard report</h1>
        <p class="subtle">{html_escape(file_name)}</p>
        <p class="subtle">Date range: {html_escape(date_range_text)}</p>
      </div>

      <div class="content">
        <table class="stats-grid" role="presentation">
          <tr>
            <td>
              <div class="stat-label">Total revenue</div>
              <div class="stat-value">{html_escape(revenue_text)}</div>
            </td>
            <td>
              <div class="stat-label">Orders</div>
              <div class="stat-value">{html_escape(orders_text)}</div>
            </td>
            <td>
              <div class="stat-label">Average order value</div>
              <div class="stat-value">{html_escape(aov_text)}</div>
            </td>
            <td>
              <div class="stat-label">Top customer</div>
              <div class="stat-value">{top_customer_text}</div>
            </td>
          </tr>
        </table>

        <table class="two-col" role="presentation">
          <tr>
            <td class="col-left">
              <div class="panel">
                <div class="panel-head">
                  <h2 class="panel-title">Top products</h2>
                  <p class="panel-subtitle">Highest-performing products by revenue</p>
                </div>
                <div class="table-wrap">
                  <table class="data-table" role="presentation">
                    {top_products_rows}
                  </table>
                </div>
              </div>
            </td>
            <td class="col-right">
              <div class="panel">
                <div class="panel-head">
                  <h2 class="panel-title">Top customers</h2>
                  <p class="panel-subtitle">Customer performance ranking</p>
                </div>
                <div class="table-wrap">
                  <table class="data-table" role="presentation">
                    {top_customers_rows}
                  </table>
                </div>
              </div>
            </td>
          </tr>
        </table>

        <table class="bottom-row" role="presentation">
          <tr>
            <td class="bottom-left">
              <div class="insights-panel">
                <div class="panel-head">
                  <h2 class="panel-title">Key insights</h2>
                  <p class="panel-subtitle">Smart takeaways from your current dashboard view</p>
                </div>
                <div class="insights-body">
                  {insights_html}
                </div>
              </div>
            </td>
            <td class="bottom-right">
              <div class="chart-panel">
                <div class="panel-head">
                  <h2 class="panel-title">Revenue trend</h2>
                  <p class="panel-subtitle">{chart_subtitle}</p>
                </div>
                <div class="chart-body">
                  {chart_panel_html}
                </div>
              </div>
            </td>
          </tr>
        </table>

        {applied_filters_html}

        <div class="footer-inline">
          Generated by Easy-dash.
        </div>
      </div>
    </div>
  </div>
</body>
</html>
    """.strip()

# =============================================================
# AUTH / SECURITY
# =============================================================

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")
pwd_ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def hash_password(password: str) -> str:
    return pwd_ctx.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)


def create_access_token(data: dict, expires: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_user_by_email(db: Session, email: str) -> Optional[User]:
    return db.query(User).filter(User.email == email).first()


def generate_secure_token() -> str:
    return secrets.token_urlsafe(32)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def get_email_verification_link(token: str) -> str:
    return f"{FRONTEND_BASE_URL}/verify-email?token={token}"


def get_password_reset_link(token: str) -> str:
    return f"{FRONTEND_BASE_URL}/reset-password?token={token}"


def send_verification_email(user: User, raw_token: str) -> None:
    verification_link = get_email_verification_link(raw_token)

    send_email_message(
        to_email=user.email,
        subject="Verify your Easy-dash email",
        body_text=(
            f"Hello,\n\n"
            f"Please verify your email address for Easy-dash by opening the link below:\n\n"
            f"{verification_link}\n\n"
            f"This link will expire in 24 hours."
        ),
        body_html=f"""
            <div style="font-family:Arial,Helvetica,sans-serif;background:#020617;padding:32px;color:#e5e7eb;">
              <div style="max-width:620px;margin:0 auto;border:1px solid #1e293b;border-radius:24px;background:#0f172a;padding:32px;">
                <p style="margin:0 0 12px 0;color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">
                  Easy-dash
                </p>
                <h1 style="margin:0 0 12px 0;color:#ffffff;font-size:28px;">Verify your email</h1>
                <p style="margin:0 0 24px 0;color:#cbd5e1;line-height:1.7;">
                  Please confirm your email address to finish setting up your Easy-dash account.
                </p>
                <a href="{verification_link}" style="display:inline-block;padding:12px 18px;border-radius:12px;background:#ffffff;color:#0f172a;text-decoration:none;font-weight:600;">
                  Verify email
                </a>
                <p style="margin:24px 0 0 0;color:#94a3b8;font-size:14px;line-height:1.7;">
                  Or open this link:<br />
                  <a href="{verification_link}" style="color:#34d399;">{verification_link}</a>
                </p>
              </div>
            </div>
        """.strip(),
    )


def send_password_reset_email(user: User, raw_token: str) -> None:
    reset_link = get_password_reset_link(raw_token)

    send_email_message(
        to_email=user.email,
        subject="Reset your Easy-dash password",
        body_text=(
            f"Hello,\n\n"
            f"You requested a password reset for Easy-dash.\n\n"
            f"Open the link below to choose a new password:\n\n"
            f"{reset_link}\n\n"
            f"If you did not request this, you can ignore this email.\n\n"
            f"This link will expire in 1 hour."
        ),
        body_html=f"""
            <div style="font-family:Arial,Helvetica,sans-serif;background:#020617;padding:32px;color:#e5e7eb;">
              <div style="max-width:620px;margin:0 auto;border:1px solid #1e293b;border-radius:24px;background:#0f172a;padding:32px;">
                <p style="margin:0 0 12px 0;color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">
                  Easy-dash
                </p>
                <h1 style="margin:0 0 12px 0;color:#ffffff;font-size:28px;">Reset your password</h1>
                <p style="margin:0 0 24px 0;color:#cbd5e1;line-height:1.7;">
                  We received a request to reset your password. Use the button below to choose a new one.
                </p>
                <a href="{reset_link}" style="display:inline-block;padding:12px 18px;border-radius:12px;background:#ffffff;color:#0f172a;text-decoration:none;font-weight:600;">
                  Reset password
                </a>
                <p style="margin:24px 0 0 0;color:#94a3b8;font-size:14px;line-height:1.7;">
                  Or open this link:<br />
                  <a href="{reset_link}" style="color:#34d399;">{reset_link}</a>
                </p>
              </div>
            </div>
        """.strip(),
    )


def user_has_active_subscription(user: User) -> bool:
    status_value = (user.subscription_status or "").strip().lower()
    return status_value in {"active", "trialing"}


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    cred_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        if not email:
            raise cred_exc
    except JWTError:
        raise cred_exc

    user = get_user_by_email(db, email)
    if not user:
        raise cred_exc
    return user

# =============================================================
# RATE LIMITING (simple in-memory)
# =============================================================

RATE_LIMIT: Dict[int, List[datetime]] = {}
PDF_RATE_LIMIT: Dict[int, List[datetime]] = {}
AUTH_RATE_LIMIT: Dict[str, List[datetime]] = {}

def require_admin(current_user: User):
    if (current_user.email or "").strip().lower() != ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Admin only")


def check_rate_limit(user_id: int) -> None:
    now = datetime.utcnow()
    window_start = now - timedelta(hours=1)
    timestamps = [t for t in RATE_LIMIT.get(user_id, []) if t > window_start]
    if len(timestamps) >= MAX_DASHBOARDS_PER_HOUR:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please try again later.",
        )
    timestamps.append(now)
    RATE_LIMIT[user_id] = timestamps


def get_client_ip(request: Optional[Request]) -> str:
    if not request:
        return "unknown"

    forwarded_for = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if forwarded_for:
        return forwarded_for

    if request.client and request.client.host:
        return request.client.host

    return "unknown"


def auth_rate_limit_key(action: str, request: Optional[Request], email: Optional[str] = None) -> str:
    normalized_email = (email or "").strip().lower() or "anonymous"
    return f"{action}:{get_client_ip(request)}:{normalized_email}"


def check_auth_rate_limit(
    key: str,
    *,
    max_attempts: int,
    window_seconds: int,
    detail: str,
) -> None:
    now = datetime.utcnow()
    window_start = now - timedelta(seconds=window_seconds)
    timestamps = [t for t in AUTH_RATE_LIMIT.get(key, []) if t > window_start]

    if len(timestamps) >= max_attempts:
        raise HTTPException(status_code=429, detail=detail)

    timestamps.append(now)
    AUTH_RATE_LIMIT[key] = timestamps


def log_auth_event(
    event: str,
    *,
    outcome: str,
    request: Optional[Request] = None,
    email: Optional[str] = None,
    user_id: Optional[int] = None,
    detail: Optional[str] = None,
) -> None:
    log_fn = logger.info if outcome == "success" else logger.warning
    log_fn(
        "auth_event event=%s outcome=%s email=%s user_id=%s ip=%s detail=%s",
        event,
        outcome,
        (email or "").strip().lower() or None,
        user_id,
        get_client_ip(request),
        detail,
    )


def check_pdf_rate_limit(user_id: int, max_per_hour: int = 60) -> None:
    """
    Very simple per-user rate limit for PDF downloads.
    Prevents a single user from hammering PDF generation.
    """
    now = datetime.utcnow()
    window_start = now - timedelta(hours=1)
    timestamps = [t for t in PDF_RATE_LIMIT.get(user_id, []) if t > window_start]
    if len(timestamps) >= max_per_hour:
        logger.warning(
            "User %s hit PDF rate limit (%s per hour).",
            user_id,
            max_per_hour,
        )
        raise HTTPException(
            status_code=429,
            detail="You’ve reached the download limit. Please try again in a little while.",
        )
    timestamps.append(now)
    PDF_RATE_LIMIT[user_id] = timestamps

TEMP_UPLOADS: Dict[str, Dict[str, Any]] = {}

def build_schema_fingerprint(columns: List[str]) -> str:
    """
    Create a stable fingerprint from cleaned column names.
    This is our V1 way to recognise familiar file structures.
    """
    normalized = [
        unicodedata.normalize("NFKD", str(col)).strip().lower()
        for col in columns
        if str(col).strip()
    ]
    normalized.sort()
    return "|".join(normalized)

def validate_uploaded_filename(filename: str) -> str:
    cleaned = (filename or "").strip()

    if not cleaned:
        raise HTTPException(status_code=400, detail="Uploaded file has no name.")

    if len(cleaned) > 200:
        raise HTTPException(status_code=400, detail="Uploaded file name is too long.")

    if "/" in cleaned or "\\" in cleaned or "\x00" in cleaned:
        raise HTTPException(status_code=400, detail="Invalid uploaded file name.")

    return cleaned

# =============================================================
# APP INIT
# =============================================================

app = FastAPI(title="Small Business Insights SaaS API - REANALYZE LIVE")

@app.middleware("http")
async def track_traffic(request: Request, call_next):
    response = await call_next(request)

    try:
        db = SessionLocal()
        db.add(TrafficEvent(
            path=request.url.path
        ))
        db.commit()
    except Exception:
        pass
    finally:
        try:
            db.close()
        except:
            pass

    return response

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=[
        "dashboard-backend-frc2.onrender.com",
        "localhost",
        "127.0.0.1",
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://easy-dash.io",
        "https://www.easy-dash.io",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/test-email")
async def send_test_email(
    data: TestEmailRequest,
    current_user: User = Depends(get_current_user),
):
    require_admin(current_user)

    recipient = (data.to_email or "").strip()

    if not recipient:
        raise HTTPException(status_code=400, detail="Recipient email is required.")

    send_email_message(
        to_email=recipient,
        subject="Dashboard Reports - Test Email",
        body_text=(
            f"Hello,\n\n"
            f"This is a test email from Dashboard Reports.\n\n"
            f"Sent at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
            f"If you received this, your email sending is working."
        ),
        from_address=os.getenv("REPORTS_FROM_ADDRESS", EMAIL_FROM_ADDRESS).strip(),
        from_name=os.getenv("REPORTS_FROM_NAME", "Dashboard Reports").strip(),
    )

    return {
        "status": "success",
        "message": f"Test email sent successfully to {recipient}",
    }

@app.post("/send-dashboard-report")
async def send_dashboard_report(
    data: SendDashboardReportRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not user_has_active_subscription(current_user):
        raise HTTPException(
            status_code=403,
            detail="Email Dashboard is available on Pro only.",
        )

    check_pdf_rate_limit(current_user.id, max_per_hour=20)

    recipient = (data.to_email or "").strip()

    if not recipient:
        raise HTTPException(status_code=400, detail="Recipient email is required.")

    dashboard = (
        db.query(Dashboard)
        .filter(
            Dashboard.id == data.dashboard_id,
            Dashboard.user_id == current_user.id,
        )
        .first()
    )

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found.")

    try:
        dashboard_payload = json.loads(dashboard.dashboard_json or "{}")
    except Exception:
        raise HTTPException(status_code=500, detail="Saved dashboard data is invalid.")

    try:
        saved_mapping = json.loads(dashboard.mapping_json or "{}")
    except Exception:
        saved_mapping = {}

    pdf_html = build_dashboard_report_email_html(
        file_name=dashboard.file_name,
        dashboard_payload=dashboard_payload,
        applied_filters=saved_mapping.get("filters", {}) or {},
    )
    pdf_bytes = HTML(string=pdf_html).write_pdf()
    pdf_filename = build_safe_pdf_filename(dashboard.file_name)

    email_body_text = (
        f"Hello,\n\n"
        f"Your Easy-dash dashboard report is attached as a PDF.\n\n"
        f"Dashboard: {dashboard.file_name}\n"
        f"Generated by Easy-dash."
    )

    email_body_html = f"""
        <div style="font-family:Arial,Helvetica,sans-serif;background:#020617;padding:32px;color:#e5e7eb;">
          <div style="max-width:620px;margin:0 auto;border:1px solid #1e293b;border-radius:24px;background:#0f172a;padding:32px;">
            <p style="margin:0 0 12px 0;color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.08em;">
              Easy-dash
            </p>
            <h1 style="margin:0 0 12px 0;color:#ffffff;font-size:28px;">Your dashboard report is ready</h1>
            <p style="margin:0;color:#cbd5e1;line-height:1.7;">
              We’ve attached your dashboard report as a PDF.
            </p>
            <p style="margin:18px 0 0 0;color:#94a3b8;font-size:14px;line-height:1.7;">
              Dashboard: <span style="color:#ffffff;">{html_escape(dashboard.file_name)}</span>
            </p>
          </div>
        </div>
    """.strip()

    send_email_message(
        to_email=recipient,
        subject=f"Your dashboard report: {dashboard.file_name}",
        body_text=email_body_text,
        body_html=email_body_html,
        from_address=os.getenv("REPORTS_FROM_ADDRESS", EMAIL_FROM_ADDRESS).strip(),
        from_name=os.getenv("REPORTS_FROM_NAME", "Dashboard Reports").strip(),
        attachments=[
            {
                "filename": pdf_filename,
                "content_bytes": pdf_bytes,
                "content_type": "application/pdf",
            }
        ],
    )

    return {
        "status": "success",
        "message": f"Dashboard report sent successfully to {recipient}",
        "dashboard_id": dashboard.id,
        "file_name": dashboard.file_name,
    }

@app.post("/reports/run-scheduled")
async def run_scheduled_reports(
    db: Session = Depends(get_db),
    x_cron_secret: str | None = Header(default=None, alias="X-Cron-Secret"),
):
    if not REPORTS_CRON_SECRET:
        raise HTTPException(
            status_code=500,
            detail="REPORTS_CRON_SECRET is not configured on the server.",
        )

    if x_cron_secret != REPORTS_CRON_SECRET:
        raise HTTPException(
            status_code=401,
            detail="Invalid cron secret.",
        )

    now_utc = datetime.utcnow()

    saved_views = (
        db.query(SavedView)
        .filter(SavedView.report_enabled.is_(True))
        .all()
    )

    checked_count = 0
    due_count = 0
    sent_count = 0
    failed_count = 0
    skipped_locked_count = 0
    failures = []

    for saved_view in saved_views:
        checked_count += 1

        if is_report_currently_locked(saved_view, now_utc):
            skipped_locked_count += 1
            continue

        if not should_send_scheduled_report(saved_view, now_utc):
            continue

        due_count += 1

        saved_view.last_report_started_at = now_utc
        saved_view.last_report_error = None
        db.add(saved_view)
        db.commit()
        db.refresh(saved_view)

        dashboard = (
            db.query(Dashboard)
            .filter(Dashboard.id == saved_view.dashboard_id)
            .first()
        )

        if not dashboard:
            failed_count += 1
            saved_view.last_report_started_at = None
            saved_view.last_report_error = "Dashboard not found."
            db.add(saved_view)
            db.commit()

            failures.append(
                {
                    "saved_view_id": saved_view.id,
                    "view_name": saved_view.name,
                    "error": "Dashboard not found.",
                }
            )
            continue

        try:
            send_saved_view_report_email(
                saved_view=saved_view,
                dashboard=dashboard,
            )
            saved_view.last_report_sent_at = now_utc
            saved_view.last_report_started_at = None
            saved_view.last_report_error = None
            db.add(saved_view)
            db.commit()
            sent_count += 1
        except Exception as e:
            failed_count += 1
            saved_view.last_report_started_at = None
            saved_view.last_report_error = str(e)
            db.add(saved_view)
            db.commit()

            failures.append(
                {
                    "saved_view_id": saved_view.id,
                    "view_name": saved_view.name,
                    "error": str(e),
                }
            )
            logger.exception(
                "Failed scheduled report send for saved_view_id=%s dashboard_id=%s",
                saved_view.id,
                saved_view.dashboard_id,
            )

    return {
        "status": "success",
        "checked": checked_count,
        "due": due_count,
        "sent": sent_count,
        "failed": failed_count,
        "skipped_locked": skipped_locked_count,
        "failures": failures,
        "ran_at_utc": now_utc.isoformat(),
    }

@app.post("/webhooks/resend")
async def resend_webhook(
    request: Request,
    db: Session = Depends(get_db),
):
    if not RESEND_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="RESEND_WEBHOOK_SECRET is not configured.")

    payload_bytes = await request.body()
    payload_text = payload_bytes.decode("utf-8")

    headers = {
        "svix-id": request.headers.get("svix-id", ""),
        "svix-timestamp": request.headers.get("svix-timestamp", ""),
        "svix-signature": request.headers.get("svix-signature", ""),
    }

    try:
        wh = Webhook(RESEND_WEBHOOK_SECRET)
        payload = wh.verify(payload_text, headers)
    except WebhookVerificationError:
        raise HTTPException(status_code=401, detail="Invalid webhook signature.")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid webhook payload.")

    event_type = (payload.get("type") or "").strip()
    data = payload.get("data") or {}

    resend_email_id = (
        data.get("email_id")
        or data.get("id")
        or payload.get("created", {}).get("id")
    )

    to_email = None
    subject = None

    to_value = data.get("to")
    if isinstance(to_value, list) and to_value:
        to_email = str(to_value[0])
    elif isinstance(to_value, str):
        to_email = to_value

    subject = data.get("subject")

    email_event = EmailEvent(
        resend_email_id=resend_email_id,
        event_type=event_type or "unknown",
        to_email=to_email,
        subject=subject,
        raw_json=json.dumps(payload),
    )

    db.add(email_event)
    db.commit()

    return {"ok": True}

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
):
    check_rate_limit(current_user.id)

    if hasattr(file, "size") and file.size and file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large.")

    if not file.filename or not file.filename.endswith((".csv", ".xlsx")):
        return JSONResponse(
            status_code=400,
            content={"error": "Please upload a .csv or .xlsx file."},
        )

    content = await file.read()

    if not content:
        return JSONResponse(
            status_code=400,
            content={"error": "The uploaded file is empty."},
        )

    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(content))
        else:
            df = pd.read_excel(BytesIO(content))
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": f"Could not read file: {str(e)}"},
        )

    if df.empty:
        return JSONResponse(
            status_code=400,
            content={"error": "The file contains no data."},
        )

    df_clean = sanitise_dataframe(df)
    detection = detect_columns_ml(df_clean)
    detected = detection.get("results", {})

    preview_rows = df_clean.head(5).replace({np.nan: None}).to_dict(orient="records")

    upload_id = str(uuid4())
    columns = list(df_clean.columns)
    schema_fingerprint = build_schema_fingerprint(columns)

    saved_mapping = None
    try:
        db = SessionLocal()
        existing_saved_mapping = (
    db.query(SavedMapping)
    .filter(
        SavedMapping.user_id == current_user.id,
        SavedMapping.schema_fingerprint == schema_fingerprint,
    )
    .first()
)

        if existing_saved_mapping:
            saved_mapping = {
                "date": existing_saved_mapping.date_col,
                "amount": existing_saved_mapping.amount_col,
                "product": existing_saved_mapping.product_col,
                "customer": existing_saved_mapping.customer_col,
                "date_parse_mode": existing_saved_mapping.date_parse_mode,
                "currency_symbol": "£",
            }
    except Exception:
        logger.exception("Failed to look up saved mapping for upload.")
    finally:
        try:
            db.close()
        except Exception:
            pass

    detected_date_col = (saved_mapping or {}).get("date") or (detected.get("date") or {}).get("column")
    detected_date_parse_mode = (saved_mapping or {}).get("date_parse_mode")

    adaptive_time_grouping = "month"
    valid_dates = pd.Series(dtype="datetime64[ns]")
    chosen_date_parse_mode = detected_date_parse_mode or "month_first"

    if detected_date_col and detected_date_col in df_clean.columns:
        parsed_dates, chosen_date_parse_mode = parse_dates_three_tier(
            df_clean[detected_date_col],
            forced_mode=detected_date_parse_mode,
        )
        valid_dates = parsed_dates.dropna()

        if not valid_dates.empty:
            adaptive_time_grouping = get_adaptive_time_grouping(
                valid_dates.min(),
                valid_dates.max(),
            )

    resolved_amount_col = (saved_mapping or {}).get("amount") or (detected.get("amount") or {}).get("column")
    resolved_currency_symbol = get_dashboard_currency_symbol(
        df_clean,
        resolved_amount_col,
        (saved_mapping or {}).get("currency_symbol"),
    )

    mapping = {
        "date": detected_date_col,
        "amount": resolved_amount_col,
        "product": (saved_mapping or {}).get("product") or (detected.get("product") or {}).get("column"),
        "customer": (saved_mapping or {}).get("customer") or (detected.get("customer") or {}).get("column"),
        "filter_start_date": "",
        "filter_end_date": "",
        "filter_product": "",
        "filter_customer": "",
        "currency_symbol": resolved_currency_symbol,
        "time_grouping": adaptive_time_grouping,
        "date_parse_mode": chosen_date_parse_mode,
        "detection_confidence": detection.get("overall_confidence", 0.0),
        "detection_warnings": detection.get("warnings", []),
    }

    logger.warning(
        "UPLOAD ROUTE | detected_date_col=%s | date_parse_mode=%s | adaptive_time_grouping=%s | min_date=%s | max_date=%s",
        detected_date_col,
        chosen_date_parse_mode,
        adaptive_time_grouping,
        valid_dates.min().date().isoformat() if detected_date_col and detected_date_col in df_clean.columns and not valid_dates.empty else None,
        valid_dates.max().date().isoformat() if detected_date_col and detected_date_col in df_clean.columns and not valid_dates.empty else None,
    )

    dashboard = analyze_sales(
        df_clean,
        mapping,
        extra_dimension_col=None,
        currency_symbol=resolved_currency_symbol,
        time_grouping=adaptive_time_grouping,
    )

    

    TEMP_UPLOADS[upload_id] = {
    "file_name": file.filename,
    "columns": columns,
    "rows": df_clean.replace({np.nan: None}).to_dict(orient="records"),
    "schema_fingerprint": schema_fingerprint,
    "detected_mapping": detection.get("results", {}),
    "overall_confidence": detection.get("overall_confidence", 0.0),
    "date_parse_mode": chosen_date_parse_mode,
}

    return {
        "message": "File uploaded successfully",
        "upload_id": upload_id,
        "filename": file.filename,
        "num_rows": len(df_clean),
        "num_columns": len(df_clean.columns),
        "columns": columns,
        "preview_rows": preview_rows,
        "detected_mapping": detection.get("results", {}),
        "saved_mapping": saved_mapping,
        "overall_confidence": detection.get("overall_confidence", 0.0),
        "warnings": detection.get("warnings", []),
        "mapping_used": mapping,
        "dashboard": dashboard,
    }

@app.post("/save-dashboard")
async def save_dashboard(
    data: dict = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    dashboard_payload = data.get("dashboard")
    mapping_payload = data.get("mapping")
    filters_payload = data.get("filters", {})
    upload_id = data.get("upload_id")

    if not dashboard_payload:
        raise HTTPException(status_code=400, detail="Missing dashboard data.")

    if not mapping_payload:
        raise HTTPException(status_code=400, detail="Missing mapping data.")

    if not upload_id:
        raise HTTPException(status_code=400, detail="Missing upload id.")

    if not user_has_active_subscription(current_user):
        existing_dashboard_count = (
            db.query(Dashboard)
            .filter(Dashboard.user_id == current_user.id)
            .count()
        )

        if existing_dashboard_count >= FREE_PLAN_DASHBOARD_LIMIT:
            raise HTTPException(
                status_code=403,
                detail=f"Free plans can save up to {FREE_PLAN_DASHBOARD_LIMIT} dashboard. Upgrade to Pro for unlimited dashboards.",
            )

    stored_upload = TEMP_UPLOADS.get(upload_id)
    if not stored_upload:
        raise HTTPException(
            status_code=400,
            detail="Upload session not found. Please upload the file again before saving."
        )

    file_name = data.get("file_name") or "Untitled dashboard"

    combined_mapping = {
        **mapping_payload,
        "filters": filters_payload,
    }

    dash_record = Dashboard(
        user_id=current_user.id,
        file_name=file_name,
        mapping_json=json.dumps(combined_mapping),
        dashboard_json=json.dumps(dashboard_payload),
        source_rows_json=json.dumps(stored_upload.get("rows", [])),
        source_columns_json=json.dumps(stored_upload.get("columns", [])),
    )
    db.add(dash_record)
    db.commit()
    db.refresh(dash_record)

    return {
        "status": "success",
        "message": "Dashboard saved successfully",
        "dashboard_id": dash_record.id,
        "file_name": dash_record.file_name,
    }

@app.post("/dashboard/{dashboard_id}/reanalyze")
async def reanalyze_saved_dashboard(
    dashboard_id: int,
    data: dict = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    dashboard_record = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .first()
    )

    if not dashboard_record:
        raise HTTPException(status_code=404, detail="Dashboard not found.")

    try:
        source_rows = json.loads(dashboard_record.source_rows_json or "[]")
        source_columns = json.loads(dashboard_record.source_columns_json or "[]")
        saved_mapping = json.loads(dashboard_record.mapping_json or "{}")
    except Exception:
        raise HTTPException(status_code=500, detail="Saved dashboard data is invalid.")

    if not source_rows:
        raise HTTPException(
            status_code=400,
            detail="This saved dashboard does not contain source data for re-analysis."
        )

    df_clean = pd.DataFrame(source_rows)
    if source_columns:
        df_clean = df_clean.reindex(columns=source_columns)

    date_col = data.get("date_col") or saved_mapping.get("date")
    amount_col = data.get("amount_col") or saved_mapping.get("amount")
    product_col = data.get("product_col") or saved_mapping.get("product")
    customer_col = data.get("customer_col") or saved_mapping.get("customer")
    saved_date_parse_mode = data.get("date_parse_mode") or saved_mapping.get("date_parse_mode")

    filter_start_date = (data.get("filter_start_date") or "").strip()
    filter_end_date = (data.get("filter_end_date") or "").strip()
    time_grouping = (data.get("time_grouping") or "month").strip().lower()

    if time_grouping not in ["day", "week", "month", "year"]:
        time_grouping = "month"

    raw_filter_product = data.get("filter_product") or ""
    raw_filter_customer = data.get("filter_customer") or ""

    parsed_filter_product = (
        [v.strip() for v in raw_filter_product.split("|") if v.strip()]
        if isinstance(raw_filter_product, str) and raw_filter_product
        else raw_filter_product if isinstance(raw_filter_product, list)
        else []
    )

    parsed_filter_customer = (
        [v.strip() for v in raw_filter_customer.split("|") if v.strip()]
        if isinstance(raw_filter_customer, str) and raw_filter_customer
        else raw_filter_customer if isinstance(raw_filter_customer, list)
        else []
    )

    comparison_enabled = bool(data.get("comparison_enabled", False))
    comparison_style = (data.get("comparison_style") or "manual").strip().lower()
    comparison_mode = (data.get("comparison_mode") or "previous_period").strip().lower()
    comparison_preset = (data.get("comparison_preset") or "").strip().lower()

    if comparison_style not in ("manual", "preset"):
        comparison_style = "manual"

    if not comparison_enabled:
        comparison_mode = "none"
        comparison_preset = ""
        comparison_style = "manual"

    parsed_dates = pd.Series(dtype="datetime64[ns]")
    chosen_date_parse_mode = saved_date_parse_mode or "month_first"

    if date_col and date_col in df_clean.columns:
        parsed_dates, chosen_date_parse_mode = parse_dates_three_tier(
            df_clean[date_col],
            forced_mode=saved_date_parse_mode,
        )

    filter_options = build_dependent_filter_options(
        df_clean,
        date_col=date_col,
        product_col=product_col,
        customer_col=customer_col,
        filter_start_date=filter_start_date,
        filter_end_date=filter_end_date,
        selected_products=parsed_filter_product,
        selected_customers=parsed_filter_customer,
    )

    effective_time_grouping = time_grouping

    adaptive_start = filter_start_date or filter_options["min_date"]
    adaptive_end = filter_end_date or filter_options["max_date"]

    if adaptive_start and adaptive_end:
        effective_time_grouping = get_adaptive_time_grouping(adaptive_start, adaptive_end)

    resolved_currency_symbol = get_dashboard_currency_symbol(
        df_clean,
        amount_col,
        saved_mapping.get("currency_symbol"),
    )

    mapping = {
        "date": date_col or None,
        "amount": amount_col or None,
        "product": product_col or None,
        "customer": customer_col or None,
        "filter_start_date": filter_start_date,
        "filter_end_date": filter_end_date,
        "filter_product": parsed_filter_product,
        "filter_customer": parsed_filter_customer,
        "currency_symbol": resolved_currency_symbol,
        "time_grouping": effective_time_grouping,
        "date_parse_mode": chosen_date_parse_mode,
        "comparison_enabled": comparison_enabled,
        "comparison_style": comparison_style,
        "comparison_mode": comparison_mode,
        "comparison_preset": comparison_preset,
        "detection_confidence": 1.0,
        "detection_warnings": [],
    }

    logger.warning(
        "REANALYZE | date_col=%s | date_parse_mode=%s | start=%s | end=%s | min_date=%s | max_date=%s | effective_time_grouping=%s",
        date_col,
        chosen_date_parse_mode,
        filter_start_date,
        filter_end_date,
        filter_options["min_date"],
        filter_options["max_date"],
        effective_time_grouping,
    )

    dashboard = analyze_sales(
        df_clean,
        mapping,
        extra_dimension_col=None,
        currency_symbol=resolved_currency_symbol,
        time_grouping=effective_time_grouping,
    )

    updated_mapping = {
        "date": date_col or None,
        "amount": amount_col or None,
        "product": product_col or None,
        "customer": customer_col or None,
        "filters": {
            "start_date": filter_start_date,
            "end_date": filter_end_date,
            "product": parsed_filter_product,
            "customer": parsed_filter_customer,
            "time_grouping": effective_time_grouping,
            "date_parse_mode": chosen_date_parse_mode,
            "comparison_enabled": comparison_enabled,
            "comparison_style": comparison_style,
            "comparison_mode": comparison_mode,
            "comparison_preset": comparison_preset,
        },
    }

    dashboard_record.mapping_json = json.dumps(updated_mapping)
    dashboard_record.dashboard_json = json.dumps(dashboard)
    db.commit()

    return {
        "dashboard_id": dashboard_record.id,
        "filename": dashboard_record.file_name,
        "dashboard": dashboard,
        "mapping_used": {
            "date": date_col or None,
            "amount": amount_col or None,
            "product": product_col or None,
            "customer": customer_col or None,
            "date_parse_mode": chosen_date_parse_mode,
            "currency_symbol": resolved_currency_symbol,
        },
        "filters_used": {
            "start_date": filter_start_date,
            "end_date": filter_end_date,
            "product": parsed_filter_product,
            "customer": parsed_filter_customer,
            "time_grouping": time_grouping,
            "comparison_enabled": comparison_enabled,
            "comparison_style": comparison_style,
            "comparison_mode": comparison_mode,
            "comparison_preset": comparison_preset,
        },
        "filter_options": filter_options,
    }

@app.patch("/dashboard/{dashboard_id}")
async def rename_dashboard(
    dashboard_id: int,
    data: dict = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    dashboard = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .first()
    )

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found.")

    new_name = (data.get("file_name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="File name cannot be empty.")

    dashboard.file_name = new_name
    db.commit()
    db.refresh(dashboard)

    return {
        "message": "Dashboard renamed successfully",
        "dashboard_id": dashboard.id,
        "file_name": dashboard.file_name,
    }

@app.delete("/mapping/{schema_fingerprint}")
def delete_saved_mapping(
    schema_fingerprint: str,
    current_user: User = Depends(get_current_user),
):
    db = SessionLocal()

    mapping = (
        db.query(SavedMapping)
        .filter(
            SavedMapping.user_id == current_user.id,
            SavedMapping.schema_fingerprint == schema_fingerprint,
        )
        .first()
    )

    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")

    db.delete(mapping)
    db.commit()

    return {"success": True}

@app.delete("/dashboard/{dashboard_id}")
async def delete_dashboard(
    dashboard_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    dashboard = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .first()
    )

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found.")

    saved_views = (
        db.query(SavedView)
        .filter(
            SavedView.dashboard_id == dashboard_id,
            SavedView.user_id == current_user.id,
        )
        .all()
    )

    for saved_view in saved_views:
        db.delete(saved_view)

    db.delete(dashboard)
    db.commit()

    return {"message": "Dashboard deleted successfully"}

@app.get("/dashboards")
async def list_dashboards(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    dashboards = (
        db.query(Dashboard)
        .filter(Dashboard.user_id == current_user.id)
        .order_by(Dashboard.created_at.desc())
        .all()
    )

    return {
        "dashboards": [
            {
                "id": dashboard.id,
                "file_name": dashboard.file_name,
                "created_at": dashboard.created_at.isoformat() if dashboard.created_at else None,
            }
            for dashboard in dashboards
        ]
    }

@app.get("/dashboard/{dashboard_id}/views")
async def get_saved_views(
    dashboard_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    dashboard = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .first()
    )

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found.")

    saved_views = (
        db.query(SavedView)
        .filter(
            SavedView.dashboard_id == dashboard_id,
            SavedView.user_id == current_user.id,
        )
        .order_by(SavedView.created_at.desc())
        .all()
    )

    return {
        "views": [
            {
                "id": view.id,
                "name": view.name,
                "filters": json.loads(view.filters_json),
                "report_enabled": bool(view.report_enabled),
                "report_frequency": view.report_frequency or "weekly",
                "report_recipient": view.report_recipient,
                "last_report_sent_at": view.last_report_sent_at.isoformat() if view.last_report_sent_at else None,
                "last_report_started_at": view.last_report_started_at.isoformat() if view.last_report_started_at else None,
                "last_report_error": view.last_report_error,
                "next_report_due_at": (
                    get_next_report_due_at(view).isoformat()
                    if get_next_report_due_at(view)
                    else None
                ),
                "created_at": view.created_at.isoformat() if view.created_at else None,
            }
            for view in saved_views
        ]
    }


@app.post("/dashboard/{dashboard_id}/views")
async def create_saved_view(
    dashboard_id: int,
    data: dict = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    dashboard = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .first()
    )

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found.")

    name = (data.get("name") or "").strip()
    filters = data.get("filters", {})

    if not name:
        raise HTTPException(status_code=400, detail="View name is required.")

    if not user_has_active_subscription(current_user):
        existing_saved_view_count = (
            db.query(SavedView)
            .filter(SavedView.user_id == current_user.id)
            .count()
        )

        if existing_saved_view_count >= FREE_PLAN_SAVED_VIEW_LIMIT:
            raise HTTPException(
                status_code=403,
                detail=f"Free plans can save up to {FREE_PLAN_SAVED_VIEW_LIMIT} views. Upgrade to Pro for unlimited saved views.",
            )

    existing_view = (
        db.query(SavedView)
        .filter(
            SavedView.dashboard_id == dashboard_id,
            SavedView.user_id == current_user.id,
            SavedView.name == name,
        )
        .first()
    )

    if existing_view:
        raise HTTPException(status_code=400, detail="A saved view with this name already exists.")

    new_view = SavedView(
        dashboard_id=dashboard_id,
        user_id=current_user.id,
        name=name,
        filters_json=json.dumps(filters),
    )

    db.add(new_view)
    db.commit()
    db.refresh(new_view)

    return {
        "status": "success",
        "view": {
            "id": new_view.id,
            "name": new_view.name,
            "filters": filters,
            "report_enabled": bool(new_view.report_enabled),
            "report_frequency": new_view.report_frequency or "weekly",
            "report_recipient": new_view.report_recipient,
            "last_report_sent_at": new_view.last_report_sent_at.isoformat() if new_view.last_report_sent_at else None,
            "last_report_started_at": new_view.last_report_started_at.isoformat() if new_view.last_report_started_at else None,
            "last_report_error": new_view.last_report_error,
            "next_report_due_at": (
                get_next_report_due_at(new_view).isoformat()
                if get_next_report_due_at(new_view)
                else None
            ),
            "created_at": new_view.created_at.isoformat() if new_view.created_at else None,
        },
    }


@app.patch("/dashboard/{dashboard_id}/views/{view_id}")
async def rename_saved_view(
    dashboard_id: int,
    view_id: int,
    data: dict = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    saved_view = (
        db.query(SavedView)
        .filter(
            SavedView.id == view_id,
            SavedView.dashboard_id == dashboard_id,
            SavedView.user_id == current_user.id,
        )
        .first()
    )

    if not saved_view:
        raise HTTPException(status_code=404, detail="Saved view not found.")

    new_name = (data.get("name") or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="View name cannot be empty.")

    duplicate_view = (
        db.query(SavedView)
        .filter(
            SavedView.dashboard_id == dashboard_id,
            SavedView.user_id == current_user.id,
            SavedView.name == new_name,
            SavedView.id != view_id,
        )
        .first()
    )

    if duplicate_view:
        raise HTTPException(status_code=400, detail="A saved view with this name already exists.")

    saved_view.name = new_name
    db.commit()
    db.refresh(saved_view)

    return {
        "status": "success",
        "view": {
            "id": saved_view.id,
            "name": saved_view.name,
            "filters": json.loads(saved_view.filters_json),
            "report_enabled": bool(saved_view.report_enabled),
            "report_frequency": saved_view.report_frequency or "weekly",
            "report_recipient": saved_view.report_recipient,
            "last_report_sent_at": saved_view.last_report_sent_at.isoformat() if saved_view.last_report_sent_at else None,
            "last_report_started_at": saved_view.last_report_started_at.isoformat() if saved_view.last_report_started_at else None,
            "last_report_error": saved_view.last_report_error,
            "next_report_due_at": (
                get_next_report_due_at(saved_view).isoformat()
                if get_next_report_due_at(saved_view)
                else None
            ),
            "created_at": saved_view.created_at.isoformat() if saved_view.created_at else None,
        },
    }


@app.put("/dashboard/{dashboard_id}/views/{view_id}")
async def update_saved_view(
    dashboard_id: int,
    view_id: int,
    data: dict = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    saved_view = (
        db.query(SavedView)
        .filter(
            SavedView.id == view_id,
            SavedView.dashboard_id == dashboard_id,
            SavedView.user_id == current_user.id,
        )
        .first()
    )

    if not saved_view:
        raise HTTPException(status_code=404, detail="Saved view not found.")

    filters = data.get("filters", {})
    saved_view.filters_json = json.dumps(filters)

    db.commit()
    db.refresh(saved_view)

    return {
        "status": "success",
        "view": {
            "id": saved_view.id,
            "name": saved_view.name,
            "filters": filters,
            "report_enabled": bool(saved_view.report_enabled),
            "report_frequency": saved_view.report_frequency or "weekly",
            "report_recipient": saved_view.report_recipient,
            "created_at": saved_view.created_at.isoformat() if saved_view.created_at else None,
        },
    }

@app.patch("/dashboard/{dashboard_id}/views/{view_id}/report")
async def update_saved_view_report_settings(
    dashboard_id: int,
    view_id: int,
    data: dict = Body(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    saved_view = (
        db.query(SavedView)
        .filter(
            SavedView.id == view_id,
            SavedView.dashboard_id == dashboard_id,
            SavedView.user_id == current_user.id,
        )
        .first()
    )

    if not saved_view:
        raise HTTPException(status_code=404, detail="Saved view not found.")

    report_enabled = bool(data.get("report_enabled", False))
    report_frequency = (data.get("report_frequency") or "weekly").strip().lower()
    report_recipient = (data.get("report_recipient") or "").strip()

    if report_frequency not in {"daily", "weekly", "monthly"}:
        raise HTTPException(status_code=400, detail="Invalid report frequency.")

    if report_enabled and not user_has_active_subscription(current_user):
        raise HTTPException(
            status_code=403,
            detail="Scheduled reports are available on paid plans only.",
        )

    if report_enabled and not report_recipient:
        raise HTTPException(status_code=400, detail="Report recipient is required when reports are enabled.")

    saved_view.report_enabled = report_enabled
    saved_view.report_frequency = report_frequency
    saved_view.report_recipient = report_recipient or None

    db.commit()
    db.refresh(saved_view)

    return {
        "status": "success",
        "view": {
            "id": saved_view.id,
            "name": saved_view.name,
            "filters": json.loads(saved_view.filters_json),
            "report_enabled": bool(saved_view.report_enabled),
            "report_frequency": saved_view.report_frequency or "weekly",
            "report_recipient": saved_view.report_recipient,
            "created_at": saved_view.created_at.isoformat() if saved_view.created_at else None,
        },
    }

@app.delete("/dashboard/{dashboard_id}/views/{view_id}")
async def delete_saved_view(
    dashboard_id: int,
    view_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    saved_view = (
        db.query(SavedView)
        .filter(
            SavedView.id == view_id,
            SavedView.dashboard_id == dashboard_id,
            SavedView.user_id == current_user.id,
        )
        .first()
    )

    if not saved_view:
        raise HTTPException(status_code=404, detail="Saved view not found.")

    db.delete(saved_view)
    db.commit()

    return {"status": "success", "message": "Saved view deleted successfully"}

@app.get("/admin/user-by-email")
def admin_get_user_by_email(
    email: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_admin(current_user)

    user = db.query(User).filter(User.email == email).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "id": user.id,
        "email": user.email,
        "subscription_status": user.subscription_status,
        "stripe_customer_id": user.stripe_customer_id,
        "created_at": user.created_at.isoformat() if user.created_at else None,
    }


@app.post("/admin/set-subscription")
def admin_set_subscription(
    email: str,
    subscription_status: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_admin(current_user)

    allowed_statuses = {"trial", "active", "canceled"}
    normalized_status = (subscription_status or "").strip().lower()

    if normalized_status not in allowed_statuses:
        raise HTTPException(
            status_code=400,
            detail="subscription_status must be one of: trial, active, canceled",
        )

    user = db.query(User).filter(User.email == email).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.subscription_status = normalized_status
    db.commit()
    db.refresh(user)

    return {
        "message": f"{user.email} subscription updated",
        "email": user.email,
        "subscription_status": user.subscription_status,
    }


@app.get("/admin/user-dashboards")
def admin_get_user_dashboards(
    email: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_admin(current_user)

    user = db.query(User).filter(User.email == email).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    dashboards = (
        db.query(Dashboard)
        .filter(Dashboard.user_id == user.id)
        .order_by(Dashboard.created_at.desc())
        .all()
    )

    return {
        "user": {
            "id": user.id,
            "email": user.email,
        },
        "dashboards": [
            {
                "id": d.id,
                "file_name": d.file_name,
                "created_at": d.created_at.isoformat() if d.created_at else None,
            }
            for d in dashboards
        ],
    }

@app.get("/admin/stats")
def admin_get_stats(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if (current_user.email or "").strip().lower() != ADMIN_EMAIL:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Admin only",
                "current_user_email": (current_user.email or "").strip().lower(),
                "admin_email": ADMIN_EMAIL,
            },
        )

    now = datetime.utcnow()
    require_admin(current_user)
    last_24h = now - timedelta(hours=24)
    last_7d = now - timedelta(days=7)
    last_30d = now - timedelta(days=30)

    total_users = db.query(User).count()
    verified_users = db.query(User).filter(User.email_verified == True).count()
    active_paid_users = db.query(User).filter(User.subscription_status == "active").count()
    trial_users = db.query(User).filter(User.subscription_status == "trial").count()
    canceled_users = db.query(User).filter(User.subscription_status == "canceled").count()

    total_dashboards = db.query(Dashboard).count()
    total_saved_views = db.query(SavedView).count()

    new_users_24h = db.query(User).filter(User.created_at >= last_24h).count()
    new_users_7d = db.query(User).filter(User.created_at >= last_7d).count()
    new_users_30d = db.query(User).filter(User.created_at >= last_30d).count()

    dashboards_24h = db.query(Dashboard).filter(Dashboard.created_at >= last_24h).count()
    dashboards_7d = db.query(Dashboard).filter(Dashboard.created_at >= last_7d).count()
    dashboards_30d = db.query(Dashboard).filter(Dashboard.created_at >= last_30d).count()

    return {
        "generated_at": now.isoformat(),
        "users": {
            "total": total_users,
            "verified": verified_users,
            "active_paid": active_paid_users,
            "trial": trial_users,
            "canceled": canceled_users,
            "new_last_24h": new_users_24h,
            "new_last_7d": new_users_7d,
            "new_last_30d": new_users_30d,
        },
        "dashboards": {
            "total": total_dashboards,
            "created_last_24h": dashboards_24h,
            "created_last_7d": dashboards_7d,
            "created_last_30d": dashboards_30d,
        },
        "saved_views": {
            "total": total_saved_views,
        },
    }

@app.get("/admin/overview")
def admin_get_overview(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    require_admin(current_user)

    now = datetime.utcnow()
    last_24h = now - timedelta(hours=24)
    last_7d = now - timedelta(days=7)
    last_30d = now - timedelta(days=30)

    pro_price_gbp = 14.99

    total_users = db.query(User).count()
    verified_users = db.query(User).filter(User.email_verified == True).count()
    active_paid_users = db.query(User).filter(User.subscription_status == "active").count()
    trial_users = db.query(User).filter(User.subscription_status == "trial").count()
    canceled_users = db.query(User).filter(User.subscription_status == "canceled").count()

    total_dashboards = db.query(Dashboard).count()
    total_saved_views = db.query(SavedView).count()

    new_users_24h = db.query(User).filter(User.created_at >= last_24h).count()
    new_users_7d = db.query(User).filter(User.created_at >= last_7d).count()
    new_users_30d = db.query(User).filter(User.created_at >= last_30d).count()

    dashboards_24h = db.query(Dashboard).filter(Dashboard.created_at >= last_24h).count()
    dashboards_7d = db.query(Dashboard).filter(Dashboard.created_at >= last_7d).count()
    dashboards_30d = db.query(Dashboard).filter(Dashboard.created_at >= last_30d).count()

    estimated_mrr_gbp = round(active_paid_users * pro_price_gbp, 2)
    traffic_summary = get_cloudflare_traffic_summary()

    visits_last_24h = int(traffic_summary.get("visits_last_24h") or 0)
    signup_conversion_rate_24h = round((new_users_24h / visits_last_24h) * 100, 2) if visits_last_24h > 0 else 0.0

    visits_last_7d = sum((p.get("visits") or 0) for p in (traffic_summary.get("trend_7d") or []))

    signup_conversion_rate_7d = (
        round((new_users_7d / visits_last_7d) * 100, 2)
        if visits_last_7d > 0
        else 0.0
    )
    verification_rate_7d = round((verified_users / total_users) * 100, 2) if total_users > 0 else 0.0
    paid_conversion_rate_7d = round((active_paid_users / verified_users) * 100, 2) if verified_users > 0 else 0.0

    top_pages_rows = (
        db.query(TrafficEvent.path)
        .filter(TrafficEvent.created_at >= last_7d)
        .filter(TrafficEvent.path.isnot(None))
        .all()
    )

    excluded_prefixes = (
        "/admin",
        "/health",
        "/docs",
        "/openapi.json",
        "/favicon",
        "/_next",
        "/auth",
        "/reports",
        "/api",
    )

    page_counts: Dict[str, int] = {}
    for (raw_path,) in top_pages_rows:
        path = (raw_path or "").strip()
        if not path:
            continue

        if any(path.startswith(prefix) for prefix in excluded_prefixes):
            continue

        page_counts[path] = page_counts.get(path, 0) + 1

    top_pages = [
        {"path": path, "hits": hits}
        for path, hits in sorted(page_counts.items(), key=lambda item: item[1], reverse=True)[:8]
    ]

    return {
        "generated_at": now.isoformat(),
        "users": {
            "total": total_users,
            "verified": verified_users,
            "active_paid": active_paid_users,
            "trial": trial_users,
            "canceled": canceled_users,
            "new_last_24h": new_users_24h,
            "new_last_7d": new_users_7d,
            "new_last_30d": new_users_30d,
        },
        "product_usage": {
            "total_dashboards": total_dashboards,
            "dashboards_created_last_24h": dashboards_24h,
            "dashboards_created_last_7d": dashboards_7d,
            "dashboards_created_last_30d": dashboards_30d,
            "total_saved_views": total_saved_views,
        },
        "revenue": {
            "currency": "GBP",
            "plan_price_gbp": pro_price_gbp,
            "active_paid_users": active_paid_users,
            "estimated_mrr_gbp": estimated_mrr_gbp,
            "source": "database_estimate",
        },
        "traffic": {
            **traffic_summary,
            "signup_conversion_rate_24h": signup_conversion_rate_24h,
            "top_pages_7d": top_pages,
        },
        "funnel": {
            "visits_last_24h": visits_last_24h,
            "signups_last_24h": new_users_24h,
            "signups_last_7d": new_users_7d,
            "verified_users_total": verified_users,
            "paid_users_total": active_paid_users,
            "visit_to_signup_rate_24h": signup_conversion_rate_24h,
            "visit_to_signup_rate_7d": signup_conversion_rate_7d,
            "signup_to_verified_rate_total": verification_rate_7d,
            "verified_to_paid_rate_total": paid_conversion_rate_7d,
        },
    }

def apply_dimension_filters(
    df,
    product_col=None,
    customer_col=None,
    product=None,
    customer=None,
):
    """
    Apply all non-date filters first, using the REAL mapped column names.
    Supports either a single selected value or a list of selected values.
    """
    filtered = df.copy()

    if product is not None and product_col and product_col in filtered.columns:
        if isinstance(product, list):
            product_values = [str(v).strip() for v in product if str(v).strip()]
            if product_values:
                filtered = filtered[
                    filtered[product_col].astype(str).str.strip().isin(product_values)
                ]
        else:
            product_value = str(product).strip()
            if product_value:
                filtered = filtered[
                    filtered[product_col].astype(str).str.strip() == product_value
                ]

    if customer is not None and customer_col and customer_col in filtered.columns:
        if isinstance(customer, list):
            customer_values = [str(v).strip() for v in customer if str(v).strip()]
            if customer_values:
                filtered = filtered[
                    filtered[customer_col].astype(str).str.strip().isin(customer_values)
                ]
        else:
            customer_value = str(customer).strip()
            if customer_value:
                filtered = filtered[
                    filtered[customer_col].astype(str).str.strip() == customer_value
                ]

    return filtered


def build_dependent_filter_options(
    df: pd.DataFrame,
    *,
    date_col: Optional[str] = None,
    product_col: Optional[str] = None,
    customer_col: Optional[str] = None,
    filter_start_date: Optional[str] = None,
    filter_end_date: Optional[str] = None,
    selected_products: Optional[List[str]] = None,
    selected_customers: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Build dropdown options that only show valid combinations.

    Rules:
    - Product options are narrowed by current date filters + selected customers
    - Customer options are narrowed by current date filters + selected products

    Also returns compatibility maps so the frontend can narrow options instantly
    before the user clicks Apply filters.
    """
    working_df = df.copy()

    if date_col and date_col in working_df.columns:
        parsed_dates, _ = parse_dates_three_tier(working_df[date_col])
        working_df[date_col] = parsed_dates
        working_df = working_df.dropna(subset=[date_col])

        if filter_start_date:
            try:
                start_dt = pd.Timestamp(filter_start_date).normalize()
                working_df = working_df[working_df[date_col] >= start_dt]
            except Exception:
                pass

        if filter_end_date:
            try:
                end_dt = pd.Timestamp(filter_end_date).normalize()
                working_df = working_df[working_df[date_col] <= end_dt]
            except Exception:
                pass

    products: List[str] = []
    customers: List[str] = []
    customer_to_products: Dict[str, List[str]] = {}
    product_to_customers: Dict[str, List[str]] = {}

    if product_col and customer_col and product_col in working_df.columns and customer_col in working_df.columns:
        relation_df = working_df[[product_col, customer_col]].dropna().copy()
        relation_df[product_col] = relation_df[product_col].astype(str).str.strip()
        relation_df[customer_col] = relation_df[customer_col].astype(str).str.strip()
        relation_df = relation_df[
            (relation_df[product_col] != "") & (relation_df[customer_col] != "")
        ]

        if not relation_df.empty:
            for customer_name, group in relation_df.groupby(customer_col):
                customer_to_products[str(customer_name)] = sorted(
                    group[product_col].dropna().astype(str).unique().tolist()
                )

            for product_name, group in relation_df.groupby(product_col):
                product_to_customers[str(product_name)] = sorted(
                    group[customer_col].dropna().astype(str).unique().tolist()
                )

    product_df = working_df.copy()
    if selected_customers and customer_col and customer_col in product_df.columns:
        product_df = product_df[
            product_df[customer_col].astype(str).isin([str(v) for v in selected_customers])
        ]

    if product_col and product_col in product_df.columns:
        products = sorted(
            [
                str(v)
                for v in product_df[product_col].dropna().astype(str).unique().tolist()
                if str(v).strip()
            ]
        )

    customer_df = working_df.copy()
    if selected_products and product_col and product_col in customer_df.columns:
        customer_df = customer_df[
            customer_df[product_col].astype(str).isin([str(v) for v in selected_products])
        ]

    if customer_col and customer_col in customer_df.columns:
        customers = sorted(
            [
                str(v)
                for v in customer_df[customer_col].dropna().astype(str).unique().tolist()
                if str(v).strip()
            ]
        )

    min_date = ""
    max_date = ""

    if date_col and date_col in working_df.columns:
        valid_dates = working_df[date_col].dropna()
        if not valid_dates.empty:
            min_date = valid_dates.min().date().isoformat()
            max_date = valid_dates.max().date().isoformat()

    return {
        "products": products,
        "customers": customers,
        "min_date": min_date,
        "max_date": max_date,
        "customer_to_products": customer_to_products,
        "product_to_customers": product_to_customers,
    }

    """
    Apply all non-date filters first, using the REAL mapped column names.
    Supports either a single selected value or a list of selected values.
    """
    filtered = df.copy()

    if product is not None and product_col and product_col in filtered.columns:
        if isinstance(product, list):
            product_values = [str(v).strip() for v in product if str(v).strip()]
            if product_values:
                filtered = filtered[
                    filtered[product_col].astype(str).str.strip().isin(product_values)
                ]
        else:
            product_value = str(product).strip()
            if product_value:
                filtered = filtered[
                    filtered[product_col].astype(str).str.strip() == product_value
                ]

    if customer is not None and customer_col and customer_col in filtered.columns:
        if isinstance(customer, list):
            customer_values = [str(v).strip() for v in customer if str(v).strip()]
            if customer_values:
                filtered = filtered[
                    filtered[customer_col].astype(str).str.strip().isin(customer_values)
                ]
        else:
            customer_value = str(customer).strip()
            if customer_value:
                filtered = filtered[
                    filtered[customer_col].astype(str).str.strip() == customer_value
                ]

    return filtered


def get_comparison_date_range(start_date, end_date, comparison_mode):
    """
    Returns:
        previous_start, previous_end, comparison_label
    """
    if not comparison_mode or comparison_mode == "none":
        return None, None, None

    start_date = pd.Timestamp(start_date).normalize()
    end_date = pd.Timestamp(end_date).normalize()

    current_days = (end_date - start_date).days + 1

    if comparison_mode == "previous_period":
        previous_end = start_date - pd.Timedelta(days=1)
        previous_start = previous_end - pd.Timedelta(days=current_days - 1)
        return previous_start, previous_end, "vs previous period"

    if comparison_mode == "previous_week":
        previous_start = start_date - pd.Timedelta(weeks=1)
        previous_end = end_date - pd.Timedelta(weeks=1)
        return previous_start, previous_end, "vs previous week"

    if comparison_mode == "previous_month":
        previous_start = start_date - pd.DateOffset(months=1)
        previous_end = end_date - pd.DateOffset(months=1)
        return previous_start, previous_end, "vs previous month"

    if comparison_mode == "previous_quarter":
        previous_start = start_date - pd.DateOffset(months=3)
        previous_end = end_date - pd.DateOffset(months=3)
        return previous_start, previous_end, "vs previous quarter"

    if comparison_mode == "previous_year":
        previous_start = start_date - pd.DateOffset(years=1)
        previous_end = end_date - pd.DateOffset(years=1)
        return previous_start, previous_end, "vs previous year"

    if comparison_mode == "same_period_last_year":
        previous_start = start_date - pd.DateOffset(years=1)
        previous_end = end_date - pd.DateOffset(years=1)
        return previous_start, previous_end, "vs same period last year"

    return None, None, None


def build_period_dfs(
    df,
    date_col,
    amount_col,
    start_date,
    end_date,
    comparison_mode="none",
    product_col=None,
    customer_col=None,
    product=None,
    customer=None,
):
    """
    Correct order:
    1. Clean dates + amount
    2. Apply non-date filters once
    3. Split into current and previous periods
    """
    working_df = df.copy()

    parsed_dates, _ = parse_dates_three_tier(working_df[date_col])
    working_df[date_col] = parsed_dates
    working_df[amount_col] = pd.to_numeric(working_df[amount_col], errors="coerce")
    working_df = working_df.dropna(subset=[date_col, amount_col])

    filtered_df = apply_dimension_filters(
        working_df,
        product_col=product_col,
        customer_col=customer_col,
        product=product,
        customer=customer,
    )

    start_date = pd.Timestamp(start_date).normalize()
    end_date = pd.Timestamp(end_date).normalize()

    current_mask = (
        (filtered_df[date_col] >= start_date)
        & (filtered_df[date_col] <= end_date)
    )
    current_df = filtered_df.loc[current_mask].copy()

    previous_start, previous_end, comparison_label = get_comparison_date_range(
        start_date,
        end_date,
        comparison_mode,
    )

    if previous_start is None or previous_end is None:
        return current_df, None, None

    previous_mask = (
        (filtered_df[date_col] >= previous_start)
        & (filtered_df[date_col] <= previous_end)
    )
    previous_df = filtered_df.loc[previous_mask].copy()

    return current_df, previous_df, comparison_label

def parse_bool_form_value(value: Optional[str]) -> bool:
    """
    Parse common truthy form values from the frontend.
    """
    if value is None:
        return False

    return str(value).strip().lower() in {"true", "1", "yes", "on"}

def get_latest_available_date(
    df: pd.DataFrame,
    date_col: Optional[str],
) -> Optional[pd.Timestamp]:
    if not date_col or date_col not in df.columns:
        return None

    parsed_dates, _ = parse_dates_three_tier(df[date_col])
    parsed_dates = parsed_dates.dropna()
    if parsed_dates.empty:
        return None

    return parsed_dates.max().normalize()


def get_quick_preset_date_ranges(
    anchor_date: pd.Timestamp,
    preset_key: str,
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], Optional[pd.Timestamp], Optional[pd.Timestamp], Optional[str]]:
    """
    Returns:
        current_start, current_end, previous_start, previous_end, comparison_label
    """
    if not preset_key:
        return None, None, None, None, None

    anchor_date = pd.Timestamp(anchor_date).normalize()

    if preset_key == "last_7_days":
        current_end = anchor_date
        current_start = current_end - pd.Timedelta(days=6)
        previous_end = current_start - pd.Timedelta(days=1)
        previous_start = previous_end - pd.Timedelta(days=6)
        return current_start, current_end, previous_start, previous_end, "vs previous 7 days"

    if preset_key == "last_30_days":
        current_end = anchor_date
        current_start = current_end - pd.Timedelta(days=29)
        previous_end = current_start - pd.Timedelta(days=1)
        previous_start = previous_end - pd.Timedelta(days=29)
        return current_start, current_end, previous_start, previous_end, "vs previous 30 days"

    if preset_key == "this_month":
        current_start = anchor_date.replace(day=1)
        current_end = current_start + pd.offsets.MonthEnd(1)
        previous_end = current_start - pd.Timedelta(days=1)
        previous_start = previous_end.replace(day=1)
        return current_start, current_end, previous_start, previous_end, "vs previous month"

    if preset_key == "this_quarter":
        quarter = ((anchor_date.month - 1) // 3) + 1
        quarter_start_month = ((quarter - 1) * 3) + 1
        current_start = pd.Timestamp(year=anchor_date.year, month=quarter_start_month, day=1)
        current_end = current_start + pd.DateOffset(months=3) - pd.Timedelta(days=1)
        previous_end = current_start - pd.Timedelta(days=1)
        previous_start = current_start - pd.DateOffset(months=3)
        return current_start, current_end, previous_start, previous_end, "vs previous quarter"

    return None, None, None, None, None

def safe_pct_change(current, previous):
    if previous is None or previous == 0:
        return None
    return ((current - previous) / previous) * 100

@app.post("/analyze-with-mapping")
async def analyze_with_mapping(
    upload_id: str = Form(...),
    date_col: Optional[str] = Form(None),
    amount_col: Optional[str] = Form(None),
    product_col: Optional[str] = Form(None),
    customer_col: Optional[str] = Form(None),
    filter_start_date: Optional[str] = Form(None),
    filter_end_date: Optional[str] = Form(None),
    filter_product: Optional[str] = Form(None),
    filter_customer: Optional[str] = Form(None),
    comparison_style: Optional[str] = Form("manual"),
    comparison_mode: Optional[str] = Form("previous_period"),
    comparison_preset: Optional[str] = Form(None),
    comparison_enabled: Optional[str] = Form("false"),
    time_grouping: str = Form("month"),
    current_user: User = Depends(get_current_user),
):
    stored = TEMP_UPLOADS.get(upload_id)

    if not stored:
        return JSONResponse(
            status_code=404,
            content={"error": "Upload session not found. Please upload the file again."},
        )

    df_clean = pd.DataFrame(stored["rows"])
    stored_date_parse_mode = stored.get("date_parse_mode")

    if time_grouping not in ["day", "week", "month", "year"]:
        time_grouping = "month"

    comparison_is_enabled = parse_bool_form_value(comparison_enabled)
    comparison_style = (comparison_style or "manual").strip().lower()
    comparison_preset = (comparison_preset or "").strip().lower()
    comparison_mode = (comparison_mode or "previous_period").strip().lower()

    if comparison_style not in ("manual", "preset"):
        comparison_style = "manual"

    if not comparison_is_enabled:
        comparison_mode = "none"
        comparison_preset = ""
        comparison_style = "manual"

    parsed_dates = pd.Series(dtype="datetime64[ns]")
    chosen_date_parse_mode = stored_date_parse_mode or "month_first"

    if date_col and date_col in df_clean.columns:
        parsed_dates, chosen_date_parse_mode = parse_dates_three_tier(
            df_clean[date_col],
            forced_mode=stored_date_parse_mode,
        )

    parsed_filter_product = (
        [v.strip() for v in filter_product.split("|") if v.strip()]
        if filter_product
        else []
    )

    parsed_filter_customer = (
        [v.strip() for v in filter_customer.split("|") if v.strip()]
        if filter_customer
        else []
    )

    filter_options = build_dependent_filter_options(
        df_clean,
        date_col=date_col,
        product_col=product_col,
        customer_col=customer_col,
        filter_start_date=filter_start_date,
        filter_end_date=filter_end_date,
        selected_products=parsed_filter_product,
        selected_customers=parsed_filter_customer,
    )

    effective_filter_start_date = (filter_start_date or "").strip()
    effective_filter_end_date = (filter_end_date or "").strip()

    effective_time_grouping = time_grouping

    adaptive_start = effective_filter_start_date or filter_options["min_date"]
    adaptive_end = effective_filter_end_date or filter_options["max_date"]

    if adaptive_start and adaptive_end:
        effective_time_grouping = get_adaptive_time_grouping(adaptive_start, adaptive_end)

    resolved_amount_col = amount_col or None
    resolved_currency_symbol = get_dashboard_currency_symbol(
        df_clean,
        resolved_amount_col,
        "£",
    )

    mapping = {
        "date": date_col or None,
        "amount": resolved_amount_col,
        "product": product_col or None,
        "customer": customer_col or None,
        "filter_start_date": effective_filter_start_date,
        "filter_end_date": effective_filter_end_date,
        "filter_product": parsed_filter_product,
        "filter_customer": parsed_filter_customer,
        "currency_symbol": resolved_currency_symbol,
        "time_grouping": effective_time_grouping,
        "date_parse_mode": chosen_date_parse_mode,
        "comparison_enabled": comparison_is_enabled,
        "comparison_style": comparison_style,
        "comparison_mode": comparison_mode,
        "comparison_preset": comparison_preset,
        "detection_confidence": 1.0,
        "detection_warnings": [],
    }

    logger.warning(
        "ANALYZE-WITH-MAPPING | date_col=%s | date_parse_mode=%s | start=%s | end=%s | min_date=%s | max_date=%s | effective_time_grouping=%s",
        date_col,
        chosen_date_parse_mode,
        effective_filter_start_date,
        effective_filter_end_date,
        filter_options["min_date"],
        filter_options["max_date"],
        effective_time_grouping,
    )

    dashboard = analyze_sales(
        df_clean,
        mapping,
        extra_dimension_col=None,
        currency_symbol=resolved_currency_symbol,
        time_grouping=effective_time_grouping,
    )

    schema_fingerprint = build_schema_fingerprint(stored["columns"])

    existing_saved_mapping = None
    try:
        db = SessionLocal()
        existing_saved_mapping = (
            db.query(SavedMapping)
            .filter(
                SavedMapping.user_id == current_user.id,
                SavedMapping.schema_fingerprint == schema_fingerprint,
            )
            .first()
        )

        if existing_saved_mapping:
            existing_saved_mapping.sample_file_name = stored["file_name"]
            existing_saved_mapping.date_col = date_col or None
            existing_saved_mapping.amount_col = amount_col or None
            existing_saved_mapping.product_col = product_col or None
            existing_saved_mapping.customer_col = customer_col or None
            existing_saved_mapping.date_parse_mode = chosen_date_parse_mode
        else:
            existing_saved_mapping = SavedMapping(
            user_id=current_user.id,
            schema_fingerprint=schema_fingerprint,
            sample_file_name=stored["file_name"],
            date_col=date_col or None,
            amount_col=amount_col or None,
            product_col=product_col or None,
            customer_col=customer_col or None,
            date_parse_mode=chosen_date_parse_mode,
        )
            db.add(existing_saved_mapping)

        db.commit()
    except Exception:
        logger.exception("Failed to save mapping for schema fingerprint.")
    finally:
        try:
            db.close()
        except Exception:
            pass

    return {
    "upload_id": upload_id,
    "filename": stored["file_name"],
    "columns": stored["columns"],
    "dashboard": dashboard,
    "detected_mapping": stored.get("detected_mapping", {}),
    "overall_confidence": stored.get("overall_confidence", 0.0),
    "mapping_used": {
        "date": date_col or None,
        "amount": resolved_amount_col,
        "product": product_col or None,
        "customer": customer_col or None,
        "date_parse_mode": chosen_date_parse_mode,
        "currency_symbol": resolved_currency_symbol,
    },
    "filters_used": {
        "start_date": effective_filter_start_date,
        "end_date": effective_filter_end_date,
        "product": parsed_filter_product,
        "customer": parsed_filter_customer,
        "time_grouping": time_grouping,
        "comparison_enabled": comparison_is_enabled,
        "comparison_style": comparison_style,
        "comparison_mode": comparison_mode,
        "comparison_preset": comparison_preset,
    },
    "filter_options": filter_options,
}

# =============================================================
# SANITISATION & ML-STYLE COLUMN DETECTION
# =============================================================

def normalize_text(val: str) -> str:
    if not isinstance(val, str):
        return val
    return unicodedata.normalize("NFKD", val).strip().lower()


TEXT_NUMBERS = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
    "hundred": 100, "thousand": 1000,
}


def textnum_to_int(s: str) -> Optional[float]:
    if not isinstance(s, str):
        return None
    words = normalize_text(s).split()
    if not words:
        return None

    total = 0
    current = 0
    matched = False

    for w in words:
        if w in TEXT_NUMBERS:
            matched = True
            val = TEXT_NUMBERS[w]
            if val in (100, 1000):
                if current == 0:
                    current = 1
                current *= val
            else:
                current += val

    if matched:
        return float(total + current)
    return None


CURRENCY_SYMBOLS = ["£", "$", "€", "¥"]


def detect_mixed_currency(series: pd.Series) -> Tuple[bool, List[str]]:
    symbols = set()
    for v in series.dropna().astype(str):
        for sym in CURRENCY_SYMBOLS:
            if sym in v:
                symbols.add(sym)
    return (len(symbols) > 1, list(symbols))


def convert_numeric_with_confidence(series: pd.Series) -> Tuple[pd.Series, float]:
    cleaned_vals = []
    success = 0
    n = len(series)

    for v in series:
        if pd.isna(v):
            cleaned_vals.append(np.nan)
            continue

        s = str(v).strip()

        # Written numbers ("five", "twelve", etc.)
        t = textnum_to_int(s)
        if t is not None:
            cleaned_vals.append(t)
            success += 1
            continue

        # Strip currency symbols and commas
        for sym in CURRENCY_SYMBOLS:
            s = s.replace(sym, "")
        s = s.replace(",", "")

        try:
            num = float(s)
            cleaned_vals.append(num)
            success += 1
        except Exception:
            cleaned_vals.append(np.nan)

    confidence = success / n if n > 0 else 0.0
    return pd.Series(cleaned_vals), float(confidence)


def convert_dates_with_confidence(series: pd.Series) -> Tuple[pd.Series, float]:
    parsed = []
    success = 0
    n = len(series)

    for v in series:
        if pd.isna(v):
            parsed.append(pd.NaT)
            continue
        s = str(v)
        parsed_dt = None

        for dayfirst in (False, True):
            try:
                dt = dateparser.parse(s, dayfirst=dayfirst, fuzzy=True)
                parsed_dt = dt
                break
            except Exception:
                continue

        if parsed_dt is not None:
            parsed.append(parsed_dt)
            success += 1
        else:
            parsed.append(pd.NaT)

    confidence = success / n if n > 0 else 0.0
    return pd.Series(parsed), float(confidence)


def score_column(df: pd.DataFrame, col: str) -> Dict[str, float]:
    series = df[col]
    text_col = normalize_text(col)
    text_col_spaced = text_col.replace("_", " ").replace("-", " ")

    scores = {
        "keyword_amount": 0.0,
        "keyword_date": 0.0,
        "keyword_product": 0.0,
        "keyword_customer": 0.0,
        "numeric_ratio": 0.0,
        "date_ratio": 0.0,
        "unique_ratio": 0.0,
    }

    AMOUNT_KW_STRONG = [
        "amount", "revenue", "sales", "sale value", "order value",
        "line total", "total sales", "net sales", "gross sales",
        "income", "turnover", "value", "price", "subtotal"
    ]
    AMOUNT_KW_WEAK = [
        "total", "money", "paid", "charge", "charges", "payment"
    ]

    DATE_KW_STRONG = [
        "date", "order date", "invoice date", "transaction date",
        "created at", "created on", "sale date", "purchase date"
    ]
    DATE_KW_WEAK = [
        "created", "transaction", "invoice", "ordered", "timestamp"
    ]

    PRODUCT_KW_STRONG = [
        "product", "product name", "item", "item name", "sku",
        "stock code", "product code", "service", "service name"
    ]
    PRODUCT_KW_WEAK = [
        "category", "description", "product type", "item type"
    ]

    CUSTOMER_KW_STRONG = [
        "customer", "customer name", "client", "client name",
        "buyer", "account", "account name", "company", "company name"
    ]
    CUSTOMER_KW_WEAK = [
        "contact", "organisation", "organization", "business"
    ]

    def contains_any(keywords: List[str]) -> bool:
        return any(k in text_col_spaced for k in keywords)

    # Strong keyword matches
    if contains_any(AMOUNT_KW_STRONG):
        scores["keyword_amount"] += 1.2
    if contains_any(DATE_KW_STRONG):
        scores["keyword_date"] += 1.2
    if contains_any(PRODUCT_KW_STRONG):
        scores["keyword_product"] += 1.2
    if contains_any(CUSTOMER_KW_STRONG):
        scores["keyword_customer"] += 1.2

    # Weak keyword matches
    if contains_any(AMOUNT_KW_WEAK):
        scores["keyword_amount"] += 0.45
    if contains_any(DATE_KW_WEAK):
        scores["keyword_date"] += 0.45
    if contains_any(PRODUCT_KW_WEAK):
        scores["keyword_product"] += 0.45
    if contains_any(CUSTOMER_KW_WEAK):
        scores["keyword_customer"] += 0.45

    # Avoid over-scoring vague column names
    if text_col_spaced in {"name", "title", "label"}:
        scores["keyword_customer"] -= 0.25
        scores["keyword_product"] -= 0.25

    if text_col_spaced in {"order", "order id", "invoice number", "invoice id", "id"}:
        scores["keyword_date"] -= 0.5

    if text_col_spaced in {"total"}:
        scores["keyword_amount"] += 0.2
        scores["keyword_date"] -= 0.2
        scores["keyword_product"] -= 0.2
        scores["keyword_customer"] -= 0.2

    numeric_series, num_conf = convert_numeric_with_confidence(series)
    scores["numeric_ratio"] = num_conf

    _, date_conf = convert_dates_with_confidence(series)
    scores["date_ratio"] = date_conf

    unique_ratio = series.nunique(dropna=True) / len(series) if len(series) else 0.0
    scores["unique_ratio"] = float(unique_ratio)

    # Type-aware nudges
    if num_conf > 0.8:
        scores["keyword_amount"] += 0.35

    if date_conf > 0.8:
        scores["keyword_date"] += 0.35

    if unique_ratio > 0.7 and num_conf < 0.4 and date_conf < 0.4:
        scores["keyword_product"] += 0.15
        scores["keyword_customer"] += 0.15

    return scores


def detect_columns_ml(df: pd.DataFrame) -> Dict[str, Any]:
    all_scores: Dict[str, Dict[str, float]] = {}
    warnings: List[str] = []

    for col in df.columns:
        all_scores[col] = score_column(df, col)

    def pick_best(score_fn) -> Tuple[Optional[str], float]:
        best_col = None
        best_score = 0.0
        for col, sc in all_scores.items():
            score = score_fn(sc)
            if score > best_score:
                best_score = score
                best_col = col
        return best_col, float(best_score)

    amount_col, amount_conf = pick_best(
        lambda sc: sc["keyword_amount"] + sc["numeric_ratio"]
    )
    date_col, date_conf = pick_best(
        lambda sc: sc["keyword_date"] + sc["date_ratio"]
    )
    prod_col, prod_conf = pick_best(
        lambda sc: sc["keyword_product"] + sc["unique_ratio"] * 0.5
    )
    cust_col, cust_conf = pick_best(
        lambda sc: sc["keyword_customer"] + sc["unique_ratio"] * 0.5
    )

    def normalize_conf(score: float) -> float:
        # raw scores can exceed 1 because keyword + type signals are combined
        return float(max(0.0, min(score / 2.0, 1.0)))

    results = {
        "date": {"column": date_col, "confidence": round(normalize_conf(date_conf), 3)},
        "amount": {"column": amount_col, "confidence": round(normalize_conf(amount_conf), 3)},
        "product": {"column": prod_col, "confidence": round(normalize_conf(prod_conf), 3)},
        "customer": {"column": cust_col, "confidence": round(normalize_conf(cust_conf), 3)},
    }

    if amount_col:
        mixed, symbols = detect_mixed_currency(df[amount_col].astype(str))
        if mixed:
            warnings.append(
                f"Multiple currency symbols detected in amount column: {', '.join(symbols)}"
            )

    for role, det in results.items():
        if not det["column"] or det["confidence"] < 0.35:
            warnings.append(
                f"Low confidence detecting {role} column – please double-check in the UI."
            )

    overall_conf = float(
        np.mean(
            [
                results["date"]["confidence"],
                results["amount"]["confidence"],
                results["product"]["confidence"],
                results["customer"]["confidence"],
            ]
        )
    )

    return {
        "results": results,
        "overall_confidence": round(overall_conf, 3),
        "warnings": warnings,
    }


def sanitise_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # clean column names
    new_cols = []
    for c in df.columns:
        c2 = unicodedata.normalize("NFKD", str(c))
        c2 = " ".join(c2.split())  # collapse whitespace
        c2 = c2.strip()
        new_cols.append(c2)
    df.columns = new_cols

    # strip string values
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda v: unicodedata.normalize("NFKD", v).strip()
                if isinstance(v, str)
                else v
            )
    return df

def safe_pct_change(current: float, previous: float):
    if previous is None or previous == 0:
        return None
    return ((current - previous) / previous) * 100.0


def get_day_diff(start_date, end_date):
    if start_date is None or end_date is None:
        return None

    start_ts = pd.Timestamp(start_date).normalize()
    end_ts = pd.Timestamp(end_date).normalize()
    return abs((end_ts - start_ts).days) + 1


def get_adaptive_time_grouping(start_date, end_date):
    day_diff = get_day_diff(start_date, end_date)

    if day_diff is None:
        return "month"
    if day_diff <= 14:
        return "day"
    if day_diff <= 90:
        return "week"
    return "month"


def score_parsed_dates(parsed_series: pd.Series) -> tuple[int, float]:
    """
    Score a parsed date series.
    Higher is better.

    Score by:
    - number of valid parsed values
    - sensible overall span (prefer tighter, realistic ranges)
    """
    valid = parsed_series.dropna()
    valid_count = int(valid.shape[0])

    if valid_count == 0:
        return (0, float("inf"))

    span_days = float((valid.max() - valid.min()).days)
    return (valid_count, span_days)


def parse_dates_three_tier(
    series: pd.Series,
    forced_mode: Optional[str] = None,
) -> tuple[pd.Series, str]:
    """
    Three-tier parsing strategy:

    1. If forced_mode is set, obey it.
    2. Otherwise try month-first and day-first.
    3. Pick the more plausible parse:
       - more valid rows wins
       - if tied, smaller overall date span wins
       - if still tied, default to month_first

    Returns:
        parsed_series, parse_mode
    parse_mode is one of:
        - "month_first"
        - "day_first"
        - "iso"
    """
    clean_series = series.copy()

    if forced_mode == "month_first":
        parsed = pd.to_datetime(clean_series, errors="coerce", dayfirst=False)
        return parsed, "month_first"

    if forced_mode == "day_first":
        parsed = pd.to_datetime(clean_series, errors="coerce", dayfirst=True)
        return parsed, "day_first"

    # First pass: standard/month-first
    parsed_month = pd.to_datetime(clean_series, errors="coerce", dayfirst=False)

    # Second pass: day-first
    parsed_day = pd.to_datetime(clean_series, errors="coerce", dayfirst=True)

    month_score = score_parsed_dates(parsed_month)
    day_score = score_parsed_dates(parsed_day)

    # More valid rows wins
    if month_score[0] > day_score[0]:
        chosen = parsed_month
        mode = "month_first"
    elif day_score[0] > month_score[0]:
        chosen = parsed_day
        mode = "day_first"
    else:
        # Tie-breaker: smaller span wins
        if month_score[1] <= day_score[1]:
            chosen = parsed_month
            mode = "month_first"
        else:
            chosen = parsed_day
            mode = "day_first"

    # Third pass rescue for remaining NaT rows
    rescue_values = []
    for raw_val, parsed_val in zip(clean_series, chosen):
        if pd.isna(parsed_val) and isinstance(raw_val, str) and raw_val.strip():
            try:
                parsed = dateparser.parse(raw_val)
            except Exception:
                parsed = None
            rescue_values.append(parsed if parsed is not None else parsed_val)
        else:
            rescue_values.append(parsed_val)

    final_series = pd.to_datetime(rescue_values, errors="coerce")
    return final_series, mode

# =============================================================
# CORE ANALYSIS
# =============================================================

def analyze_sales(
    df: pd.DataFrame,
    mapping: Dict[str, Optional[str]],
    extra_dimension_col: Optional[str] = None,
    currency_symbol: str = "£",
    time_grouping: str = "month",
) -> Dict[str, Any]:
    quality: Dict[str, int] = {
        "rows_total": int(len(df)),
        "rows_removed_invalid_amount": 0,
        "rows_removed_invalid_date": 0,
        "rows_used_for_insights": 0,
    }

    df_work = df.copy()
    missing_sections: List[str] = []
    summary: Dict[str, Any] = {}
    charts: Dict[str, Any] = {}
    insights: List[str] = []

    amount_col = mapping.get("amount")
    date_col = mapping.get("date")
    product_col = mapping.get("product")
    customer_col = mapping.get("customer")
    comparison_enabled = bool(mapping.get("comparison_enabled", False))
    comparison_style = (mapping.get("comparison_style") or "manual").lower()
    comparison_mode = (mapping.get("comparison_mode") or "none").lower()
    comparison_preset = (mapping.get("comparison_preset") or "").lower()

    if not comparison_enabled:
        comparison_mode = "none"
        comparison_preset = ""
        comparison_style = "manual"
    # ---------- AMOUNT CLEANING ----------
    if not amount_col or amount_col not in df_work.columns:
        missing_sections += [
            "summary",
            "trend",
            "top_products",
            "top_customers",
            "weekday_pattern",
        ]
        quality["rows_used_for_insights"] = 0
        return {
            "summary": {},
            "charts": {},
            "insights": [
                "We couldn't find a usable Amount/Revenue column in your file. Please check the column mapping and try again."
            ],
            "missing_sections": missing_sections,
            "data_quality_report": quality,
        }

    comparison = {
        "enabled": comparison_enabled,
        "comparison_style": comparison_style,
        "comparison_preset": comparison_preset,
        "current_revenue": 0.0,
        "previous_revenue": 0.0,
        "revenue_change_pct": None,
        "current_orders": 0,
        "previous_orders": 0,
        "orders_change_pct": None,
        "current_average_order_value": 0.0,
        "previous_average_order_value": 0.0,
        "average_order_value_change_pct": None,
        "comparison_label": "none",
        "comparison_mode": comparison_mode,
        "has_previous_data": False,
        "current_period_start": None,
        "current_period_end": None,
        "previous_period_start": None,
        "previous_period_end": None,
    }

    if date_col and date_col in df.columns and amount_col and amount_col in df.columns:
        filter_start_date = (mapping.get("filter_start_date") or "").strip()
        filter_end_date = (mapping.get("filter_end_date") or "").strip()

        filter_product = mapping.get("filter_product")
        if filter_product in ("", [], None):
            filter_product = None

        filter_customer = mapping.get("filter_customer")
        if filter_customer in ("", [], None):
            filter_customer = None

        df_compare = df.copy()
        comparison_date_parse_mode = mapping.get("date_parse_mode")

        parsed_compare_dates, _ = parse_dates_three_tier(
            df_compare[date_col],
            forced_mode=comparison_date_parse_mode,
        )
        df_compare[date_col] = parsed_compare_dates

        df_compare[amount_col] = pd.to_numeric(df_compare[amount_col], errors="coerce")
        df_compare = df_compare.dropna(subset=[date_col, amount_col])

        if not df_compare.empty:
            df_compare_filtered = apply_dimension_filters(
                df_compare,
                product_col=product_col,
                customer_col=customer_col,
                product=filter_product,
                customer=filter_customer,
            )

            current_start = None
            current_end = None
            previous_start = None
            previous_end = None
            comparison_label = "none"

            if comparison_enabled:
                if comparison_style == "preset":
                    anchor_date = get_latest_available_date(df_compare_filtered, date_col)

                    if anchor_date is not None:
                        (
                            current_start,
                            current_end,
                            previous_start,
                            previous_end,
                            comparison_label,
                        ) = get_quick_preset_date_ranges(anchor_date, comparison_preset)

                elif comparison_style == "manual":
                    if filter_start_date and filter_end_date:
                        current_start = pd.Timestamp(filter_start_date).normalize()
                        current_end = pd.Timestamp(filter_end_date).normalize()
                        previous_start, previous_end, comparison_label = get_comparison_date_range(
                            current_start,
                            current_end,
                            comparison_mode,
                        )

            if current_start is not None and current_end is not None:
                current_mask = (
                    (df_compare_filtered[date_col] >= current_start) &
                    (df_compare_filtered[date_col] <= current_end)
                )
                current_df = df_compare_filtered.loc[current_mask].copy()
            else:
                current_df = df_compare_filtered.copy()

            if (
                comparison_enabled
                and current_start is not None
                and current_end is not None
                and previous_start is not None
                and previous_end is not None
            ):
                previous_mask = (
                    (df_compare_filtered[date_col] >= previous_start) &
                    (df_compare_filtered[date_col] <= previous_end)
                )
                previous_df = df_compare_filtered.loc[previous_mask].copy()
            else:
                previous_df = None

            current_revenue = float(current_df[amount_col].sum()) if not current_df.empty else 0.0
            current_orders = int(len(current_df))
            current_aov = current_revenue / current_orders if current_orders > 0 else 0.0

            previous_revenue = None
            previous_orders = None
            previous_aov = None

            if previous_df is not None and not previous_df.empty:
                previous_revenue = float(previous_df[amount_col].sum())
                previous_orders = int(len(previous_df))
                previous_aov = previous_revenue / previous_orders if previous_orders > 0 else 0.0

            revenue_change_pct = safe_pct_change(current_revenue, previous_revenue)
            orders_change_pct = safe_pct_change(current_orders, previous_orders)
            aov_change_pct = safe_pct_change(current_aov, previous_aov)

            comparison = {
                "enabled": comparison_enabled,
                "comparison_style": comparison_style,
                "comparison_preset": comparison_preset,
                "current_revenue": round(current_revenue, 2),
                "previous_revenue": round(previous_revenue, 2) if previous_revenue is not None else 0.0,
                "revenue_change_pct": None if not comparison_enabled else (
                    round(revenue_change_pct, 2) if revenue_change_pct is not None else None
                ),
                "current_orders": current_orders,
                "previous_orders": previous_orders if previous_orders is not None else 0,
                "orders_change_pct": None if not comparison_enabled else (
                    round(orders_change_pct, 2) if orders_change_pct is not None else None
                ),
                "current_average_order_value": round(current_aov, 2),
                "previous_average_order_value": round(previous_aov, 2) if previous_aov is not None else 0.0,
                "average_order_value_change_pct": None if not comparison_enabled else (
                    round(aov_change_pct, 2) if aov_change_pct is not None else None
                ),
                "comparison_label": comparison_label or "none",
                "comparison_mode": comparison_mode,
                "has_previous_data": previous_df is not None and not previous_df.empty,
                "current_period_start": current_start.date().isoformat() if current_start is not None else None,
                "current_period_end": current_end.date().isoformat() if current_end is not None else None,
                "previous_period_start": previous_start.date().isoformat() if previous_start is not None else None,
                "previous_period_end": previous_end.date().isoformat() if previous_end is not None else None,
            }
    


    before_amount = len(df_work)
    numeric_series, _ = convert_numeric_with_confidence(df_work[amount_col])
    df_work[amount_col] = numeric_series
    df_work = df_work.dropna(subset=[amount_col])
    after_amount = len(df_work)
    quality["rows_removed_invalid_amount"] = int(before_amount - after_amount)

    if df_work.empty:
        quality["rows_used_for_insights"] = 0
        return {
            "summary": {},
            "charts": {},
            "insights": ["We couldn’t generate a report because there was no valid data after cleaning (all rows in the Amount column were invalid)."],
            "missing_sections": [
                "summary",
                "trend",
                "top_products",
                "top_customers",
                "weekday_pattern",
            ],
            "data_quality_report": quality,
        }

        # ---------- DATE HANDLING & FILTER SETUP ----------
    has_date = False
    # Filters passed via mapping (string values or empty)
    filter_start_date = (mapping.get("filter_start_date") or "") or None
    filter_end_date = (mapping.get("filter_end_date") or "") or None

    filter_product = mapping.get("filter_product")
    if filter_product in ("", [], None):
        filter_product = None

    filter_customer = mapping.get("filter_customer")
    if filter_customer in ("", [], None):
        filter_customer = None

    if date_col and date_col in df_work.columns:
        before_date = len(df_work)

        forced_date_parse_mode = mapping.get("date_parse_mode")
        parsed_dates, chosen_date_parse_mode = parse_dates_three_tier(
            df_work[date_col],
            forced_mode=forced_date_parse_mode,
        )

        df_work[date_col] = parsed_dates

        df_work = df_work.dropna(subset=[date_col])
        after_date = len(df_work)
        quality["rows_removed_invalid_date"] = int(before_date - after_date)

        if not df_work.empty and pd.api.types.is_datetime64_any_dtype(df_work[date_col]):
            has_date = True

    if date_col and date_col in df_work.columns and not has_date:
        # We had a date column but could not parse it – so time-based sections are missing.
        missing_sections.extend(["date_range", "trend", "weekday_pattern"])

    # ---------- APPLY FILTERS (DATE / PRODUCT / CUSTOMER) ----------
    if has_date:
        # Date range filters are applied AFTER cleaning, but do not affect data-quality counts.
        mask = pd.Series(True, index=df_work.index)
        if filter_start_date:
            try:
                start_dt = datetime.fromisoformat(filter_start_date)
                mask &= df_work[date_col] >= start_dt
            except ValueError:
                pass
        if filter_end_date:
            try:
                end_dt = datetime.fromisoformat(filter_end_date)
                mask &= df_work[date_col] <= end_dt
            except ValueError:
                pass
        df_work = df_work[mask]

    # Product and customer filters
    if filter_product and product_col and product_col in df_work.columns:
     if isinstance(filter_product, list):
        df_work = df_work[df_work[product_col].isin(filter_product)]
     else:
        df_work = df_work[df_work[product_col] == filter_product]

    if filter_customer and customer_col and customer_col in df_work.columns:
     if isinstance(filter_customer, list):
        df_work = df_work[df_work[customer_col].isin(filter_customer)]
     else:
        df_work = df_work[df_work[customer_col] == filter_customer]

    # If filters removed all rows, return a friendly empty-dashboard payload.
    if df_work.empty:
        quality["rows_used_for_insights"] = 0
        return {
            "summary": {},
            "charts": {},
            "insights": [
                "No rows matched the selected filters. Try clearing or adjusting your filters."
            ],
            "missing_sections": [
                "summary",
                "trend",
                "top_products",
                "top_customers",
                "weekday_pattern",
            ],
            "data_quality_report": quality,
        }

    total_revenue = float(df_work[amount_col].sum())
    num_sales = int(len(df_work))
    avg_sale = float(total_revenue / num_sales) if num_sales > 0 else 0.0

    summary["total_revenue"] = total_revenue
    summary["num_sales"] = num_sales
    summary["average_sale"] = avg_sale
    summary["comparison"] = comparison
    quality["rows_used_for_insights"] = num_sales

    if has_date:
        start_date = df_work[date_col].min()
        end_date = df_work[date_col].max()
        summary["date_range"] = {
            "start": start_date.date().isoformat(),
            "end": end_date.date().isoformat(),
        }
    else:
        missing_sections.append("date_range")

    # ---------- TREND ----------
    if has_date:
        # Decide grouping based on explicit time_grouping (day / week / month / year)
        tg = (mapping.get("time_grouping") or time_grouping or "month").lower()
        if tg not in {"day", "week", "month", "year"}:
            tg = "month"

        trend_df = df_work.copy()

        if tg == "day":
            trend_df["_day"] = trend_df[date_col].dt.date
            grouped = (
                trend_df.groupby("_day", as_index=False)[amount_col]
                .sum()
                .sort_values("_day")
            )
            grouped["period"] = grouped["_day"].astype(str)
            grouping = "day"

        elif tg == "week":
            iso = trend_df[date_col].dt.isocalendar()
            trend_df["_year"] = iso["year"]
            trend_df["_week"] = iso["week"]
            grouped = (
                trend_df.groupby(["_year", "_week"], as_index=False)[amount_col]
                .sum()
                .sort_values(["_year", "_week"])
            )
            grouped["period"] = (
                grouped["_year"].astype(str)
                + "-W"
                + grouped["_week"].astype(str).str.zfill(2)
            )
            grouping = "week"

        elif tg == "year":
            trend_df["_year"] = trend_df[date_col].dt.year
            grouped = (
                trend_df.groupby("_year", as_index=False)[amount_col]
                .sum()
                .sort_values("_year")
            )
            grouped["period"] = grouped["_year"].astype(str)
            grouping = "year"

        else:  # month (default)
            trend_df["_period"] = trend_df[date_col].dt.to_period("M")
            grouped = (
                trend_df.groupby("_period", as_index=False)[amount_col]
                .sum()
                .sort_values("_period")
            )
            grouped["period"] = grouped["_period"].astype(str)
            grouping = "month"

        charts["revenue_over_time"] = {
            "grouping": grouping,
            "points": [
                {
                    "period": str(row["period"]),
                    "revenue": float(row[amount_col]),
                }
                for _, row in grouped.iterrows()
            ],
        }
    else:
        missing_sections.append("trend")




    # ---------- TOP PRODUCTS ----------
    if product_col and product_col in df_work.columns:
        prod_group = (
            df_work.groupby(product_col)[amount_col].sum().sort_values(ascending=False)
        )
        top_products = prod_group.head(5)
        charts["top_products"] = [
            {
                "product": name,
                "revenue": float(value),
                "share_of_total": float(
                    (value / total_revenue) * 100
                ) if total_revenue > 0 else 0.0,
            }
            for name, value in top_products.items()
        ]
    else:
        missing_sections.append("top_products")

    # ---------- TOP CUSTOMERS ----------
    if customer_col and customer_col in df_work.columns:
        cust_group = (
            df_work.groupby(customer_col)[amount_col]
            .agg(["sum", "count"])
            .sort_values("sum", ascending=False)
        )
        top_customers = cust_group.head(5)
        charts["top_customers"] = [
            {
                "customer": idx,
                "revenue": float(row["sum"]),
                "num_purchases": int(row["count"]),
                "share_of_total": float(
                    (row["sum"] / total_revenue) * 100
                ) if total_revenue > 0 else 0.0,
            }
            for idx, row in top_customers.iterrows()
        ]
    else:
        missing_sections.append("top_customers")

    # ---------- WEEKDAY PATTERN ----------
    if has_date:
        weekday_group = df_work.groupby(df_work[date_col].dt.day_name())[amount_col].sum()
        order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        weekday_points = []
        for day in order:
            if day in weekday_group.index:
                weekday_points.append(
                    {"day": day, "revenue": float(weekday_group.loc[day])}
                )
        charts["revenue_by_weekday"] = weekday_points
    else:
        missing_sections.append("weekday_pattern")

    # ---------- EXTRA DIMENSION ----------
    if extra_dimension_col and extra_dimension_col in df_work.columns:
        dim_group = (
            df_work.groupby(extra_dimension_col)[amount_col]
            .sum()
            .sort_values(ascending=False)
        )
        top_dim = dim_group.head(5)
        charts["by_dimension"] = [
            {
                "label": name,
                "revenue": float(value),
                "share_of_total": float(
                    (value / total_revenue) * 100
                ) if total_revenue > 0 else 0.0,
            }
            for name, value in top_dim.items()
        ]
    elif extra_dimension_col:
        missing_sections.append("by_dimension")

    # ---------- NATURAL LANGUAGE INSIGHTS ----------
    if total_revenue > 0:
        insights.append(
            f"Your total revenue in this period is {currency_symbol}{total_revenue:,.2f} across {num_sales} sales."
        )
        insights.append(
            f"Your average sale value is {currency_symbol}{avg_sale:,.2f}."
        )

    if "revenue_over_time" in charts and charts["revenue_over_time"].get("points"):
        pts = charts["revenue_over_time"]["points"]
        if len(pts) >= 2:
            first = pts[-2]["revenue"]
            last = pts[-1]["revenue"]
            if first > 0:
                change = ((last - first) / first) * 100
                direction = "higher" if change > 0 else "lower"
                grouping = charts["revenue_over_time"].get("grouping", "period")
                if grouping == "day":
                    period_word = "day"
                elif grouping == "week":
                    period_word = "week"
                elif grouping == "year":
                    period_word = "year"
                elif grouping == "month":
                    period_word = "month"
                else:
                    period_word = "period"
                insights.append(
                    f"Revenue in the latest {period_word} is {abs(change):.1f}% {direction} than the previous one."
                )
        if "top_products" in charts and charts["top_products"]:
            tp = charts["top_products"][0]
            insights.append(
                f"Your top product is {tp['product']} contributing {tp['share_of_total']:.1f}% of total revenue."
            )

    if "top_customers" in charts and charts["top_customers"]:
        tc = charts["top_customers"][0]
        insights.append(
            f"Your top customer is {tc['customer']} accounting for {tc['share_of_total']:.1f}% of revenue."
        )

    if "by_dimension" in charts and charts["by_dimension"]:
        bd = charts["by_dimension"][0]
        insights.append(
            f"In your chosen dimension, {bd['label']} is the top contributor with {bd['share_of_total']:.1f}% of revenue."
        )

                        # ---------- SMART INSIGHTS (LAUNCH READY) ----------
    smart_insights = []

    def add_smart_insight(
        title: str,
        message: str,
        severity: str = "info",
        priority: int = 50,
        category: str = "general",
        why_it_matters: Optional[str] = None,
        suggested_action: Optional[str] = None,
    ) -> None:
        if not message:
            return

        duplicate = any(
            isinstance(s, dict)
            and s.get("title") == title
            and s.get("message") == message
            for s in smart_insights
        )
        if duplicate:
            return

        smart_insights.append(
            {
                "title": title,
                "message": message,
                "severity": severity,   # positive | warning | neutral | info
                "priority": priority,
                "category": category,   # change | concentration | trend | opportunity | highlight | summary
                "why_it_matters": why_it_matters,
                "suggested_action": suggested_action,
            }
        )

    comp = summary.get("comparison", {}) or {}

    # --- 1. Comparison-driven performance insights ---
    if comp.get("enabled"):
        revenue_change = comp.get("revenue_change_pct")
        orders_change = comp.get("orders_change_pct")
        aov_change = comp.get("average_order_value_change_pct")
        comparison_label = comp.get("comparison_label") or "the previous period"

        if revenue_change is not None and abs(revenue_change) >= 5:
            if revenue_change > 0:
                add_smart_insight(
                    title="Revenue growth",
                    message=f"Revenue increased by {abs(revenue_change):.1f}% compared with {comparison_label}, showing positive momentum.",
                    severity="positive",
                    priority=95,
                    category="change",
                    why_it_matters="Sustained revenue growth is the clearest signal that overall business performance is improving.",
                    suggested_action="Identify what drove the uplift and look for ways to repeat it in the next period.",
                )
            else:
                add_smart_insight(
                    title="Revenue decline",
                    message=f"Revenue decreased by {abs(revenue_change):.1f}% compared with {comparison_label}, which may need attention.",
                    severity="warning",
                    priority=95,
                    category="change",
                    why_it_matters="A revenue drop can point to weaker demand, smaller orders, or customer loss.",
                    suggested_action="Check whether the drop came from fewer orders, lower basket size, or a single customer/product slipping.",
                )

        if orders_change is not None and abs(orders_change) >= 5:
            if orders_change > 0:
                add_smart_insight(
                    title="Order volume up",
                    message=f"Order volume is up {abs(orders_change):.1f}% versus {comparison_label}, suggesting stronger demand.",
                    severity="positive",
                    priority=82,
                    category="change",
                    why_it_matters="More orders usually means more customer activity and stronger market pull.",
                    suggested_action="Look at which products or customers contributed most to the increase.",
                )
            else:
                add_smart_insight(
                    title="Order volume down",
                    message=f"Order volume is down {abs(orders_change):.1f}% versus {comparison_label}, suggesting softer demand.",
                    severity="warning",
                    priority=82,
                    category="change",
                    why_it_matters="Fewer orders can quickly reduce revenue even if pricing stays stable.",
                    suggested_action="Review whether specific products, channels, or customers drove the drop in order count.",
                )

        if aov_change is not None and abs(aov_change) >= 5:
            if aov_change > 0:
                add_smart_insight(
                    title="Higher basket size",
                    message=f"Average order value increased by {abs(aov_change):.1f}% versus {comparison_label}, meaning customers are spending more per order.",
                    severity="positive",
                    priority=76,
                    category="opportunity",
                    why_it_matters="Higher basket size improves revenue efficiency without needing as many additional orders.",
                    suggested_action="Double down on the products or bundles that are lifting average order value.",
                )
            else:
                add_smart_insight(
                    title="Lower basket size",
                    message=f"Average order value fell by {abs(aov_change):.1f}% versus {comparison_label}, which may point to smaller baskets or lower-value sales.",
                    severity="warning",
                    priority=76,
                    category="opportunity",
                    why_it_matters="Lower basket size can drag revenue even when order count holds up.",
                    suggested_action="Review pricing, bundling, and upsell opportunities to lift value per order.",
                )

    selected_products = mapping.get("filter_product") or []
    selected_customers = mapping.get("filter_customer") or []
    is_filtered_view = bool(
        (mapping.get("filter_start_date") or "").strip()
        or (mapping.get("filter_end_date") or "").strip()
        or selected_products
        or selected_customers
    )

    def scoped_message(default_message: str, filtered_message: str) -> str:
        return filtered_message if is_filtered_view else default_message

    # --- 2. Product concentration / standout performance ---
    top_products = charts.get("top_products", []) or []

    raw_selected_products = mapping.get("filter_product")
    if isinstance(raw_selected_products, list):
        product_filter_values = [str(v).strip() for v in raw_selected_products if str(v).strip()]
    elif isinstance(raw_selected_products, str) and raw_selected_products.strip():
        product_filter_values = [v.strip() for v in raw_selected_products.split("|") if v.strip()]
    else:
        product_filter_values = []

    is_single_product_view = len(product_filter_values) == 1

    if top_products and not is_single_product_view:
        top_product = top_products[0]
        top_product_name = top_product.get("product", "Top product")
        top_product_share = float(top_product.get("share_of_total", 0) or 0)

        if top_product_share >= 40:
            add_smart_insight(
                title="Product concentration risk",
                message=scoped_message(
                    f"{top_product_name} generates {top_product_share:.1f}% of revenue, indicating strong product concentration risk.",
                    f"Within this filtered view, {top_product_name} generates {top_product_share:.1f}% of revenue.",
                ),
                severity="warning",
                priority=90,
                category="concentration",
                why_it_matters="Heavy dependency on one product increases risk if demand changes or supply is disrupted.",
                suggested_action="Consider broadening revenue across more products or strengthening backup winners.",
            )
        elif top_product_share >= 25:
            add_smart_insight(
                title="Leading product",
                message=scoped_message(
                    f"{top_product_name} is your strongest product, contributing {top_product_share:.1f}% of revenue.",
                    f"Within this filtered view, {top_product_name} contributes {top_product_share:.1f}% of revenue.",
                ),
                severity="positive",
                priority=68,
                category="highlight",
                why_it_matters="This product is currently a key commercial driver in the business.",
                suggested_action="Use it as a benchmark when evaluating pricing, promotions, or product mix.",
            )

        if len(top_products) >= 2:
            second_product_share = float(top_products[1].get("share_of_total", 0) or 0)
            if top_product_share - second_product_share >= 15:
                add_smart_insight(
                    title="Clear revenue driver",
                    message=scoped_message(
                        f"{top_product_name} is materially ahead of the next best product, making it your clearest revenue driver.",
                        f"Within this filtered view, {top_product_name} is materially ahead of the next best product.",
                    ),
                    severity="info",
                    priority=64,
                    category="highlight",
                    why_it_matters="A standout product often explains a large share of overall performance.",
                    suggested_action="Understand what makes it win and whether those patterns can be applied elsewhere.",
                )
    elif is_single_product_view:
        focused_product_name = product_filter_values[0]
        add_smart_insight(
            title="Focused product view",
            message=f"You are currently viewing a single product: {focused_product_name}. Product share insights are hidden in this focused view.",
            severity="info",
            priority=63,
            category="summary",
            why_it_matters="Share-of-revenue insights become less useful when the view is already narrowed to one product.",
            suggested_action="Clear the product filter to compare this product against the wider product mix.",
        )

    # --- 3. Customer concentration / dependency ---
    top_customers = charts.get("top_customers", []) or []

    raw_selected_customers = mapping.get("filter_customer")
    if isinstance(raw_selected_customers, list):
        customer_filter_values = [str(v).strip() for v in raw_selected_customers if str(v).strip()]
    elif isinstance(raw_selected_customers, str) and raw_selected_customers.strip():
        customer_filter_values = [v.strip() for v in raw_selected_customers.split("|") if v.strip()]
    else:
        customer_filter_values = []

    is_single_customer_view = len(customer_filter_values) == 1

    if top_customers and not is_single_customer_view:
        top_customer = top_customers[0]
        top_customer_name = top_customer.get("customer", "Top customer")
        top_customer_share = float(top_customer.get("share_of_total", 0) or 0)

        if top_customer_share >= 50:
            add_smart_insight(
                title="Customer concentration risk",
                message=scoped_message(
                    f"{top_customer_name} accounts for {top_customer_share:.1f}% of revenue, creating significant customer concentration risk.",
                    f"Within this filtered view, {top_customer_name} accounts for {top_customer_share:.1f}% of revenue.",
                ),
                severity="warning",
                priority=92,
                category="concentration",
                why_it_matters="Reliance on one customer can create revenue volatility if that account slows or churns.",
                suggested_action="Try to diversify revenue across more customers or strengthen retention planning for this account.",
            )
        elif top_customer_share >= 30:
            add_smart_insight(
                title="Largest customer",
                message=scoped_message(
                    f"{top_customer_name} is your most important customer, contributing {top_customer_share:.1f}% of total revenue.",
                    f"Within this filtered view, {top_customer_name} contributes {top_customer_share:.1f}% of total revenue.",
                ),
                severity="info",
                priority=70,
                category="highlight",
                why_it_matters="This customer has an outsized influence on business performance.",
                suggested_action="Monitor this account closely and look for ways to reduce over-reliance over time.",
            )
    elif is_single_customer_view:
        focused_customer_name = customer_filter_values[0]
        add_smart_insight(
            title="Focused customer view",
            message=f"You are currently viewing a single customer: {focused_customer_name}. Customer share insights are hidden in this focused view.",
            severity="info",
            priority=65,
            category="summary",
            why_it_matters="Share-of-revenue insights become less useful when the view is already narrowed to one customer.",
            suggested_action="Clear the customer filter to compare this customer against the wider customer base.",
        )

    # --- 4. Recent trend direction ---
    revenue_over_time = charts.get("revenue_over_time", {}) or {}
    points = revenue_over_time.get("points", []) or []
    grouping = revenue_over_time.get("grouping", "period")

    if len(points) >= 3:
        recent_values = [float(p.get("revenue", 0) or 0) for p in points[-3:]]

        if recent_values[2] > recent_values[1] > recent_values[0]:
            add_smart_insight(
                title="Upward trend",
                message=f"Revenue is trending upward across the last 3 {grouping}s, which suggests improving performance.",
                severity="positive",
                priority=74,
                category="trend",
                why_it_matters="Consistent upward movement is usually a healthier signal than a one-off spike.",
                suggested_action="Keep an eye on whether the same products or customers are driving the trend.",
            )
        elif recent_values[2] < recent_values[1] < recent_values[0]:
            add_smart_insight(
                title="Downward trend",
                message=f"Revenue is trending downward across the last 3 {grouping}s, which may indicate slowing momentum.",
                severity="warning",
                priority=74,
                category="trend",
                why_it_matters="A steady decline over several periods can point to weakening sales momentum.",
                suggested_action="Review recent periods for drop-offs in order count, AOV, products, or major customers.",
            )

        best_point = max(points, key=lambda p: float(p.get("revenue", 0) or 0))
        if best_point.get("period"):
            add_smart_insight(
                title="Best recent period",
                message=f"Your strongest {grouping} in the current view was {best_point['period']}.",
                severity="info",
                priority=58,
                category="highlight",
                why_it_matters="The best recent period gives a useful benchmark for what strong performance looks like.",
                suggested_action="Compare that period with weaker ones to identify what changed.",
            )

    # --- 5. Basket size / order value opportunity ---
    if num_sales > 0:
        if avg_sale < 20:
            add_smart_insight(
                title="Basket size opportunity",
                message="Average order value is relatively low, so there may be room to improve basket size through bundles, upsells, or pricing changes.",
                severity="neutral",
                priority=72,
                category="opportunity",
                why_it_matters="Increasing value per order is one of the fastest ways to grow revenue efficiently.",
                suggested_action="Test bundles, minimum-order incentives, or premium product positioning.",
            )
        elif avg_sale > 100:
            add_smart_insight(
                title="Strong order value",
                message="Average order value is strong, which suggests customers are already placing relatively high-value orders.",
                severity="positive",
                priority=60,
                category="highlight",
                why_it_matters="Higher-value orders usually improve revenue efficiency and reduce dependence on volume alone.",
                suggested_action="Protect the mix or pricing that is supporting these stronger order values.",
            )

    # --- 6. Fallback business highlight if smart insights are still sparse ---
    if not smart_insights:
        if total_revenue > 0:
            add_smart_insight(
                title="Revenue summary",
                message=f"The dashboard shows {currency_symbol}{total_revenue:,.2f} in revenue across {num_sales} sales for the current view.",
                severity="info",
                priority=50,
                category="summary",
                why_it_matters="Even where patterns are limited, this still gives a reliable picture of current business scale.",
                suggested_action="Use filters and comparisons to dig into what is driving the current period.",
            )

        if top_products:
            add_smart_insight(
                title="Top product",
                message=f"{top_products[0].get('product', 'Your top product')} is currently the biggest product contributor.",
                severity="info",
                priority=48,
                category="highlight",
                why_it_matters="Top contributors usually explain a large share of performance.",
                suggested_action="Review whether this reflects a one-off spike or a repeatable pattern.",
            )

        if top_customers:
            add_smart_insight(
                title="Top customer",
                message=f"{top_customers[0].get('customer', 'Your top customer')} is currently your largest customer by revenue.",
                severity="info",
                priority=48,
                category="highlight",
                why_it_matters="Largest customers often have an outsized effect on revenue stability.",
                suggested_action="Track whether performance is broad-based or dependent on a small number of accounts.",
            )

    smart_insights = sorted(
        smart_insights,
        key=lambda x: x.get("priority", 0),
        reverse=True,
    )[:5]

    return {
        "summary": summary,
        "charts": charts,
        "insights": insights,
        "smart_insights": smart_insights,
        "missing_sections": list(dict.fromkeys(missing_sections)),
        "data_quality_report": quality,
    }

# =============================================================
# AUTH ROUTES
# =============================================================

@app.post("/auth/signup")
def signup(
    request: Request,
    payload: SignupRequest = Body(...),
    db: Session = Depends(get_db),
):
    email = payload.email.lower().strip()

    check_auth_rate_limit(
        auth_rate_limit_key("signup", request, email),
        max_attempts=5,
        window_seconds=15 * 60,
        detail="Too many signup attempts. Please try again in a few minutes.",
    )

    existing_user = get_user_by_email(db, email)
    if existing_user:
        log_auth_event("signup", outcome="rejected", request=request, email=email, detail="email_already_registered")
        raise HTTPException(status_code=400, detail="Email already registered.")

    password = payload.password

    if len(password) < 8:
        log_auth_event("signup", outcome="rejected", request=request, email=email, detail="password_too_short")
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters long.")

    if not re.search(r"[A-Z]", password):
        log_auth_event("signup", outcome="rejected", request=request, email=email, detail="missing_uppercase")
        raise HTTPException(status_code=400, detail="Password must include an uppercase letter.")

    if not re.search(r"[a-z]", password):
        log_auth_event("signup", outcome="rejected", request=request, email=email, detail="missing_lowercase")
        raise HTTPException(status_code=400, detail="Password must include a lowercase letter.")

    if not re.search(r"\d", password):
        log_auth_event("signup", outcome="rejected", request=request, email=email, detail="missing_number")
        raise HTTPException(status_code=400, detail="Password must include a number.")

    raw_verification_token = generate_secure_token()

    user = User(
        email=email,
        hashed_password=hash_password(payload.password),
        email_verified=False,
        email_verification_token=hash_token(raw_verification_token),
        email_verification_expires_at=datetime.utcnow() + timedelta(hours=24),
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    try:
        send_verification_email(user, raw_verification_token)
    except Exception:
        logger.exception("Failed sending verification email to user_id=%s", user.id)

    log_auth_event("signup", outcome="success", request=request, email=user.email, user_id=user.id)

    token = create_access_token({"sub": user.email})

    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "email": user.email,
            "subscription_status": user.subscription_status,
            "email_verified": bool(user.email_verified),
        },
        "message": "Account created. Please verify your email address.",
    }


@app.post("/auth/login")
def login(
    request: Request,
    payload: LoginRequest = Body(...),
    db: Session = Depends(get_db),
):
    email = payload.email.lower().strip()

    check_auth_rate_limit(
        auth_rate_limit_key("login", request, email),
        max_attempts=10,
        window_seconds=15 * 60,
        detail="Too many login attempts. Please try again in a few minutes.",
    )

    user = get_user_by_email(db, email)
    if not user or not verify_password(payload.password, user.hashed_password):
        log_auth_event("login", outcome="rejected", request=request, email=email, detail="invalid_credentials")
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    if not user.email_verified:
        log_auth_event("login", outcome="rejected", request=request, email=email, user_id=user.id, detail="email_not_verified")
        raise HTTPException(
            status_code=403,
            detail="Please verify your email before signing in."
        )

    token = create_access_token({"sub": user.email})

    log_auth_event("login", outcome="success", request=request, email=user.email, user_id=user.id)

    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "email": user.email,
            "subscription_status": user.subscription_status,
            "email_verified": bool(user.email_verified),
        },
    }


@app.post("/auth/verify-email")
def verify_email(
    request: Request,
    data: dict = Body(...),
    db: Session = Depends(get_db),
):
    raw_token = (data.get("token") or "").strip()
    if not raw_token:
        raise HTTPException(status_code=400, detail="Verification token is required.")

    token_hash = hash_token(raw_token)

    user = db.query(User).filter(User.email_verification_token == token_hash).first()
    if not user:
        log_auth_event("verify_email", outcome="rejected", request=request, detail="invalid_token")
        raise HTTPException(status_code=400, detail="Invalid verification link.")

    if not user.email_verification_expires_at or user.email_verification_expires_at < datetime.utcnow():
        log_auth_event("verify_email", outcome="rejected", request=request, email=user.email, user_id=user.id, detail="expired_token")
        raise HTTPException(status_code=400, detail="Verification link has expired.")

    user.email_verified = True
    user.email_verification_token = None
    user.email_verification_expires_at = None
    db.add(user)
    db.commit()
    db.refresh(user)

    log_auth_event("verify_email", outcome="success", request=request, email=user.email, user_id=user.id)

    return {
        "status": "success",
        "message": "Email verified successfully.",
    }


@app.post("/auth/resend-verification")
def resend_verification_email(
    request: Request,
    data: dict = Body(...),
    db: Session = Depends(get_db),
):
    email = (data.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email is required.")

    check_auth_rate_limit(
        auth_rate_limit_key("resend_verification", request, email),
        max_attempts=5,
        window_seconds=60 * 60,
        detail="Too many verification email requests. Please try again later.",
    )

    user = get_user_by_email(db, email)
    if not user:
        log_auth_event("resend_verification", outcome="rejected", request=request, email=email, detail="user_not_found")
        return {
            "status": "success",
            "message": "If an account exists for that email, a verification email has been sent.",
        }

    if user.email_verified:
        log_auth_event("resend_verification", outcome="rejected", request=request, email=email, user_id=user.id, detail="already_verified")
        return {
            "status": "success",
            "message": "That email is already verified.",
        }

    raw_verification_token = generate_secure_token()
    user.email_verification_token = hash_token(raw_verification_token)
    user.email_verification_expires_at = datetime.utcnow() + timedelta(hours=24)
    db.add(user)
    db.commit()
    db.refresh(user)

    try:
        send_verification_email(user, raw_verification_token)
    except Exception:
        logger.exception("Failed resending verification email to user_id=%s", user.id)

    log_auth_event("resend_verification", outcome="success", request=request, email=user.email, user_id=user.id)

    return {
        "status": "success",
        "message": "If an account exists for that email, a verification email has been sent.",
    }


@app.post("/auth/forgot-password")
def forgot_password(
    request: Request,
    payload: ForgotPasswordRequest = Body(...),
    db: Session = Depends(get_db),
):
    email = payload.email.lower().strip()

    check_auth_rate_limit(
        auth_rate_limit_key("forgot_password", request, email),
        max_attempts=5,
        window_seconds=15 * 60,
        detail="Too many password reset requests. Please try again later.",
    )

    user = get_user_by_email(db, email)

    if user:
        raw_reset_token = generate_secure_token()
        user.password_reset_token = hash_token(raw_reset_token)
        user.password_reset_expires_at = datetime.utcnow() + timedelta(hours=1)
        db.add(user)
        db.commit()
        db.refresh(user)

        try:
            send_password_reset_email(user, raw_reset_token)
        except Exception:
            logger.exception("Failed sending password reset email to user_id=%s", user.id)

        log_auth_event("forgot_password", outcome="success", request=request, email=user.email, user_id=user.id)
    else:
        log_auth_event("forgot_password", outcome="rejected", request=request, email=email, detail="user_not_found")

    return {
        "status": "success",
        "message": "If an account exists for that email, a reset link has been sent.",
    }


@app.post("/auth/reset-password")
def reset_password(
    request: Request,
    payload: ResetPasswordRequest = Body(...),
    db: Session = Depends(get_db),
):
    raw_token = (payload.token or "").strip()
    password = payload.password

    check_auth_rate_limit(
        auth_rate_limit_key("reset_password", request),
        max_attempts=10,
        window_seconds=60 * 60,
        detail="Too many password reset attempts. Please try again later.",
    )

    if not raw_token:
        raise HTTPException(status_code=400, detail="Reset token is required.")

    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters long.")

    if not re.search(r"[A-Z]", password):
        raise HTTPException(status_code=400, detail="Password must include an uppercase letter.")

    if not re.search(r"[a-z]", password):
        raise HTTPException(status_code=400, detail="Password must include a lowercase letter.")

    if not re.search(r"\d", password):
        raise HTTPException(status_code=400, detail="Password must include a number.")

    token_hash = hash_token(raw_token)

    user = db.query(User).filter(User.password_reset_token == token_hash).first()
    if not user:
        log_auth_event("reset_password", outcome="rejected", request=request, detail="invalid_token")
        raise HTTPException(status_code=400, detail="Invalid reset link.")

    if not user.password_reset_expires_at or user.password_reset_expires_at < datetime.utcnow():
        log_auth_event("reset_password", outcome="rejected", request=request, email=user.email, user_id=user.id, detail="expired_token")
        raise HTTPException(status_code=400, detail="Reset link has expired.")

    user.hashed_password = hash_password(password)
    user.password_reset_token = None
    user.password_reset_expires_at = None
    db.add(user)
    db.commit()

    log_auth_event("reset_password", outcome="success", request=request, email=user.email, user_id=user.id)

    return {
        "status": "success",
        "message": "Password updated successfully.",
    }


@app.get("/auth/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return {
        "id": current_user.id,
        "email": current_user.email,
        "subscription_status": current_user.subscription_status or "trial",
        "is_paid": user_has_active_subscription(current_user),
        "email_verified": bool(current_user.email_verified),
        "has_stripe_customer": bool((current_user.stripe_customer_id or "").strip()),
    }

# =============================================================
# BILLING / STRIPE
# =============================================================

@app.post("/billing/create-checkout-session")
def create_checkout_session(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not STRIPE_SECRET_KEY or not STRIPE_PRICE_ID:
        raise HTTPException(
            status_code=500,
            detail="Stripe billing is not configured on the server.",
        )

    if user_has_active_subscription(current_user):
        raise HTTPException(
            status_code=400,
            detail="Your account is already on a paid plan.",
        )

    stripe_customer_id = (current_user.stripe_customer_id or "").strip()

    if stripe_customer_id:
        try:
            stripe.Customer.retrieve(stripe_customer_id)
        except Exception:
            logger.warning(
                "Stored Stripe customer id %s was invalid for user_id=%s. Creating a new customer.",
                stripe_customer_id,
                current_user.id,
            )
            stripe_customer_id = ""

    if not stripe_customer_id:
        customer = stripe.Customer.create(email=current_user.email)
        stripe_customer_id = customer["id"]
        current_user.stripe_customer_id = stripe_customer_id
        db.add(current_user)
        db.commit()
        db.refresh(current_user)

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            payment_method_types=["card"],
            customer=stripe_customer_id,
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            success_url=f"{FRONTEND_BASE_URL}{STRIPE_SUCCESS_PATH}?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{FRONTEND_BASE_URL}{STRIPE_CANCEL_PATH}",
        )
        return {"checkout_url": session.url}
    except Exception as exc:
        logger.exception("Failed to create Stripe checkout session for user_id=%s", current_user.id)
        raise HTTPException(status_code=500, detail=f"Failed to start checkout: {exc}")


@app.post("/billing/create-portal-session")
def create_portal_session(
    current_user: User = Depends(get_current_user),
):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=500,
            detail="Stripe billing is not configured on the server.",
        )

    stripe_customer_id = (current_user.stripe_customer_id or "").strip()

    if not stripe_customer_id:
        raise HTTPException(
            status_code=400,
            detail="Billing management is unavailable for this account because it is not linked to a Stripe subscription.",
        )

    try:
        stripe.Customer.retrieve(stripe_customer_id)
    except Exception:
        logger.warning(
            "Stripe billing portal blocked because stored customer id was invalid for user_id=%s",
            current_user.id,
        )
        raise HTTPException(
            status_code=400,
            detail="Billing management is unavailable for this account because its Stripe billing record could not be found.",
        )

    try:
        session = stripe.billing_portal.Session.create(
            customer=stripe_customer_id,
            return_url=f"{FRONTEND_BASE_URL}/dashboards",
        )
        return {"portal_url": session.url}
    except Exception as exc:
        logger.exception("Failed to create Stripe billing portal session for user_id=%s", current_user.id)
        raise HTTPException(status_code=500, detail=f"Failed to open billing portal: {exc}")


@app.get("/billing/checkout-status")
def get_checkout_status(
    session_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(
            status_code=500,
            detail="Stripe billing is not configured on the server.",
        )

    session_id = (session_id or "").strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="session_id is required.")

    try:
        checkout_session = stripe.checkout.Session.retrieve(session_id)
    except Exception as exc:
        logger.exception("Failed to retrieve Stripe checkout session %s", session_id)
        raise HTTPException(status_code=400, detail=f"Failed to verify checkout session: {exc}")

    session_customer_id = (checkout_session.get("customer") or "").strip()
    payment_status = (checkout_session.get("payment_status") or "").strip().lower()
    checkout_status = (checkout_session.get("status") or "").strip().lower()

    if not session_customer_id:
        raise HTTPException(status_code=400, detail="Checkout session did not include a customer.")

    stored_customer_id = (current_user.stripe_customer_id or "").strip()
    if stored_customer_id and stored_customer_id != session_customer_id:
        raise HTTPException(status_code=403, detail="This checkout session does not belong to the current user.")

    if not stored_customer_id:
        current_user.stripe_customer_id = session_customer_id

    subscription_status = "trial"
    try:
        subscriptions = stripe.Subscription.list(customer=session_customer_id, limit=1)
        subscription_items = subscriptions.get("data") or []
        if subscription_items:
            subscription_status = (subscription_items[0].get("status") or "trial").strip().lower()
    except Exception:
        logger.exception(
            "Failed to fetch Stripe subscription status for customer %s during checkout reconciliation.",
            session_customer_id,
        )

    if subscription_status in {"active", "trialing"}:
        current_user.subscription_status = subscription_status
    elif checkout_status == "complete" and payment_status in {"paid", "no_payment_required"}:
        current_user.subscription_status = "active"

    db.add(current_user)
    db.commit()
    db.refresh(current_user)

    return {
        "status": "success",
        "subscription_status": current_user.subscription_status or "trial",
        "is_paid": user_has_active_subscription(current_user),
        "stripe_customer_id": current_user.stripe_customer_id,
    }

@app.post("/billing/webhook")
async def stripe_webhook(
    request: Request,
    db: Session = Depends(get_db),
) -> Dict[str, bool]:
    """
    Handle Stripe webhooks in a safe, non-crashing way.

    - Verifies signature if STRIPE_WEBHOOK_SECRET is configured.
    - Logs unexpected / unsupported events but still returns 200.
    - Updates user.subscription_status for subscription-related events.
    """
    payload = await request.body()
    sig_header = request.headers.get("Stripe-Signature")

    try:
        if STRIPE_WEBHOOK_SECRET:
            try:
                event = stripe.Webhook.construct_event(
                    payload, sig_header, STRIPE_WEBHOOK_SECRET
                )
            except stripe.error.SignatureVerificationError:
                logger.warning("Stripe webhook signature verification failed.")
                raise HTTPException(status_code=400, detail="Invalid signature")
        else:
            # Fallback: no webhook secret configured, do a basic parse
            event = stripe.Event.construct_from(
                json.loads(payload.decode("utf-8")), stripe.api_key
            )
    except HTTPException:
        # re-raise explicit HTTPExceptions
        raise
    except Exception:
        logger.exception("Stripe webhook: invalid payload.")
        raise HTTPException(status_code=400, detail="Invalid payload")

    event_type = event.get("type")
    logger.info("Stripe webhook received event type=%s", event_type)

    try:
        if event_type == "checkout.session.completed":
            data_object = (event.get("data") or {}).get("object") or {}
            customer_id = data_object.get("customer")

            if not customer_id:
                logger.warning("Stripe webhook checkout.session.completed missing customer id.")
                return {"received": True}

            user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
            if not user:
                logger.warning(
                    "Stripe webhook checkout.session.completed: no user found for customer %s",
                    customer_id,
                )
                return {"received": True}

            user.subscription_status = "active"
            db.add(user)
            db.commit()
            logger.info(
                "Marked user %s subscription as 'active' from checkout.session.completed.",
                user.id,
            )

        elif event_type == "customer.subscription.updated":
            data_object = (event.get("data") or {}).get("object") or {}
            customer_id = data_object.get("customer")
            status_stripe = data_object.get("status")

            if not customer_id:
                logger.warning("Stripe webhook subscription.updated missing customer id.")
                return {"received": True}

            user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
            if not user:
                logger.warning(
                    "Stripe webhook subscription.updated: no user found for customer %s",
                    customer_id,
                )
                return {"received": True}

            if status_stripe == "active":
                user.subscription_status = "active"
            elif status_stripe in {
                "trialing",
                "past_due",
                "unpaid",
                "canceled",
                "incomplete",
                "incomplete_expired",
            }:
                user.subscription_status = status_stripe

            db.add(user)
            db.commit()
            logger.info(
                "Updated subscription_status for user %s to '%s' via webhook.",
                user.id,
                user.subscription_status,
            )

        elif event_type == "customer.subscription.deleted":
            data_object = (event.get("data") or {}).get("object") or {}
            customer_id = data_object.get("customer")

            if not customer_id:
                logger.warning("Stripe webhook subscription.deleted missing customer id.")
                return {"received": True}

            user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
            if not user:
                logger.warning(
                    "Stripe webhook subscription.deleted: no user found for customer %s",
                    customer_id,
                )
                return {"received": True}

            user.subscription_status = "canceled"
            db.add(user)
            db.commit()
            logger.info(
                "Marked user %s subscription as 'canceled' from webhook.",
                user.id,
            )

        else:
            logger.info("Stripe webhook: ignoring unsupported event type '%s'.", event_type)
    except Exception:
        # Never let webhook processing crash the app; log and acknowledge.
        logger.exception("Error handling Stripe webhook event type=%s", event_type)

    return {"received": True}

@app.post("/dashboard/preview")
async def dashboard_preview(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
):
    check_rate_limit(current_user.id)

    filename = validate_uploaded_filename(file.filename or "")

    if hasattr(file, "size") and file.size and file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large.")

    if not filename.endswith((".csv", ".xlsx")):
        return JSONResponse(
            status_code=400,
            content={"error": "Please upload a .csv or .xlsx file."},
        )

    content = await file.read()
    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(content))
        else:
            df = pd.read_excel(BytesIO(content))
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": f"Could not read file: {str(e)}"},
        )

    if df.empty:
        return {"error": "The file contains no data."}

    df_clean = sanitise_dataframe(df)

    preview_rows = (
        df_clean.head(5).replace({np.nan: None}).to_dict(orient="records")
    )

    detection = detect_columns_ml(df_clean)

    return {
        "file_name": file.filename,
        "num_rows": len(df_clean),
        "num_columns": len(df_clean.columns),
        "columns": list(df_clean.columns),
        "preview_rows": preview_rows,
        "detected_mapping": detection["results"],
        "overall_confidence": detection["overall_confidence"],
        "warnings": detection["warnings"],
    }


@app.post("/dashboard/generate")
async def dashboard_generate(
    file: UploadFile = File(...),
    date_col: Optional[str] = Form(None),
    amount_col: Optional[str] = Form(None),
    product_col: Optional[str] = Form(None),
    customer_col: Optional[str] = Form(None),
    extra_dimension_col: Optional[str] = Form(None),
    filter_start_date: Optional[str] = Form(None),
    filter_end_date: Optional[str] = Form(None),
    filter_product: Optional[str] = Form(None),
    filter_customer: Optional[str] = Form(None),
    currency_symbol: Optional[str] = Form("£"),
    time_grouping: Literal["day", "week", "month", "year"] = Form("month"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Generate a dashboard JSON from an uploaded CSV/XLSX file.

    Includes:
    - subscription + rate limit checks
    - file type / size validation
    - empty/invalid file handling
    - filter + time_grouping validation
    - safe auto-mapping with clear error messages on failure
    - basic logging of file processing and data quality
    """
    # ----- Auth / subscription -----
    if current_user.subscription_status not in ("trial", "active"):
        logger.warning(
            "User %s attempted dashboard generation without active subscription.",
            current_user.id,
        )
        raise HTTPException(
            status_code=402,
            detail="Subscription required to generate dashboards.",
        )

    # ----- Rate limiting -----
    check_rate_limit(current_user.id)

    # ----- Basic file validation -----
    if file is None:
        logger.warning("User %s submitted request without file.", current_user.id)
        raise HTTPException(status_code=400, detail="No file was uploaded.")

    filename = validate_uploaded_filename(file.filename or "")

    if hasattr(file, "size") and file.size and file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        logger.warning(
            "User %s uploaded file '%s' exceeding size limit.",
            current_user.id,
            filename,
        )
        raise HTTPException(status_code=400, detail="File too large.")

    if not filename.endswith((".csv", ".xlsx")):
        logger.warning(
            "User %s uploaded unsupported file type '%s'.",
            current_user.id,
            filename,
        )
        raise HTTPException(
            status_code=400,
            detail="Please upload a .csv or .xlsx file exported from your system.",
        )

    logger.info(
        "User %s started dashboard generation for file '%s'.",
        current_user.id,
        filename,
    )

    # ----- Validate & normalise time_grouping -----
    tg = (time_grouping or "month").lower()
    if tg not in ("day", "week", "month", "year"):
        logger.warning(
            "User %s provided invalid time_grouping '%s' for file '%s'.",
            current_user.id,
            time_grouping,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail="Invalid time grouping. Please choose day, week, month, or year.",
        )
    time_grouping = tg

    # ----- Validate date filters (if provided) -----
    def _validate_filter_date(label: str, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        v = value.strip()
        if not v:
            return None
        try:
            # Expect ISO format (YYYY-MM-DD). Frontend should send in this format.
            datetime.fromisoformat(v)
        except ValueError:
            logger.warning(
                "User %s provided invalid %s date filter '%s' for file '%s'.",
                current_user.id,
                label,
                value,
                file.filename,
            )
            raise HTTPException(
                status_code=400,
                detail=f"Invalid {label} date filter. Please use YYYY-MM-DD format.",
            )
        return v

    filter_start_date = _validate_filter_date("start", filter_start_date)
    filter_end_date = _validate_filter_date("end", filter_end_date)

    # ----- Read file content safely -----
    content = await file.read()
    if not content:
        logger.warning(
            "User %s uploaded an empty file '%s'.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail="Your file appears to be empty or invalid. Try re-exporting it from your accounting/POS system.",
        )

    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(content))
        else:
            df = pd.read_excel(BytesIO(content))
    except Exception:
        logger.exception(
            "User %s: failed to parse file '%s' as CSV/XLSX.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail="Your file appears to be empty or invalid. Try re-exporting it from your accounting/POS system.",
        )

    if df.empty:
        logger.warning(
            "User %s: parsed file '%s' but DataFrame is empty.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail="Your file appears to be empty or invalid. Try re-exporting it from your accounting/POS system.",
        )

    logger.info(
        "User %s: loaded file '%s' with %d raw rows.",
        current_user.id,
        file.filename,
        len(df),
    )

    # ----- Cleaning + auto-detection -----
    df_clean = sanitise_dataframe(df)

    if df_clean.empty:
        logger.warning(
            "User %s: all rows dropped as invalid during cleaning for file '%s'.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail=(
                "We couldn’t generate a report because there was no valid data after cleaning "
                "(all rows were invalid or filtered out)."
            ),
        )

    logger.info(
        "User %s: cleaned dataframe for '%s' has %d rows.",
        current_user.id,
        file.filename,
        len(df_clean),
    )

    try:
        detection = detect_columns_ml(df_clean)
    except Exception:
        logger.exception(
            "User %s: automatic column detection failed for file '%s'.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=500,
            detail=(
                "We could not automatically understand your columns. "
                "Please review your file and mapping, then try again."
            ),
        )

    # Sanity check for detection structure
    results = detection.get("results") or {}
    if not isinstance(results, dict) or not results:
        logger.error(
            "User %s: detection returned no usable results for file '%s'.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=500,
            detail=(
                "Automatic column detection returned no usable result. "
                "Please try again or contact support."
            ),
        )

    # If we still have no amount column (and the user did not override), fail early
    detected_amount = (results.get("amount") or {}).get("column")
    if not (amount_col or detected_amount):
        logger.warning(
            "User %s: no Amount/Revenue column detected or provided for file '%s'.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail=(
                "We couldn't find a clear Amount/Revenue column in your file. "
                "Please select the correct column manually and try again."
            ),
        )

    # If we still have no date column (and the user did not override), prompt manual choice
    detected_date = (results.get("date") or {}).get("column")
    if not (date_col or detected_date):
        logger.warning(
            "User %s: no date column detected or provided for file '%s'.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail=(
                "We couldn’t find a clear date column in your file. "
                "Please select it manually and try again."
            ),
        )

    # ----- Build mapping (user overrides take priority over detection) -----
    df_for_analysis = df_clean.copy()

    resolved_date_col = date_col or detected_date

    effective_time_grouping = (time_grouping or "month").strip().lower()
    chosen_date_parse_mode = "month_first"

    if resolved_date_col and resolved_date_col in df_for_analysis.columns:
        parsed_dates, chosen_date_parse_mode = parse_dates_three_tier(
            df_for_analysis[resolved_date_col]
        )
        parsed_dates = parsed_dates.dropna()

        if not parsed_dates.empty:
            effective_time_grouping = get_adaptive_time_grouping(
                parsed_dates.min(),
                parsed_dates.max(),
            )

    resolved_amount_col = amount_col or detected_amount
    resolved_currency_symbol = get_dashboard_currency_symbol(
        df_for_analysis,
        resolved_amount_col,
        currency_symbol,
    )

    mapping = {
        "date": resolved_date_col,
        "amount": resolved_amount_col,
        "product": product_col or (results.get("product") or {}).get("column"),
        "customer": customer_col or (results.get("customer") or {}).get("column"),
        "filter_start_date": filter_start_date or "",
        "filter_end_date": filter_end_date or "",
        "filter_product": filter_product.split("|") if filter_product else [],
        "filter_customer": filter_customer.split("|") if filter_customer else [],
        "currency_symbol": resolved_currency_symbol,
        "time_grouping": effective_time_grouping,
        "date_parse_mode": chosen_date_parse_mode,
        "detection_confidence": detection.get("overall_confidence", 0.0),
        "detection_warnings": detection.get("warnings", []),
    }

    resolved_date_col = mapping["date"]
    if resolved_date_col and resolved_date_col not in df_for_analysis.columns:
        logger.warning(
            "User %s: selected date column '%s' not found in columns for file '%s'.",
            current_user.id,
            resolved_date_col,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail=(
                f"Selected date column '{resolved_date_col}' was not found in the file. "
                f"Available columns are: {list(df_for_analysis.columns)}"
            ),
        )

    # ----- Run analytics engine -----
    logger.warning(
        "DASHBOARD-GENERATE | resolved_date_col=%s | date_parse_mode=%s | effective_time_grouping=%s",
        resolved_date_col,
        chosen_date_parse_mode,
        effective_time_grouping,
    )

    dashboard = analyze_sales(
        df_for_analysis,
        mapping,
        extra_dimension_col=extra_dimension_col,
        currency_symbol=resolved_currency_symbol,
        time_grouping=effective_time_grouping,
    )

    # Extract data quality info for logging
    quality = (dashboard.get("data_quality_report") or {}) if isinstance(dashboard, dict) else {}
    rows_total = quality.get("rows_total")
    rows_used = quality.get("rows_used_for_insights")
    rows_removed_invalid_date = quality.get("rows_removed_invalid_date")
    rows_removed_invalid_amount = quality.get("rows_removed_invalid_amount")

    # Persist dashboard in DB
    dash_record = Dashboard(
        user_id=current_user.id,
        file_name=file.filename,
        mapping_json=json.dumps(mapping),
        dashboard_json=json.dumps(dashboard),
    )
    db.add(dash_record)
    db.commit()
    db.refresh(dash_record)

    logger.info(
        "User %s: dashboard %s created for file '%s' | rows_total=%s rows_used_for_insights=%s "
        "rows_removed_invalid_date=%s rows_removed_invalid_amount=%s",
        current_user.id,
        dash_record.id,
        file.filename,
        rows_total,
        rows_used,
        rows_removed_invalid_date,
        rows_removed_invalid_amount,
    )

    return {
        "dashboard_id": dash_record.id,
        "file_name": file.filename,
        "mapping_used": mapping,
        "dashboard": dashboard,
    }

@app.get("/dashboard")
def list_dashboards(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    records = (
        db.query(Dashboard)
        .filter(Dashboard.user_id == current_user.id)
        .order_by(Dashboard.created_at.desc())
        .all()
    )
    return [
        {
            "id": d.id,
            "file_name": d.file_name,
            "created_at": d.created_at.isoformat(),
        }
        for d in records
    ]


@app.get("/dashboard/{dashboard_id}")
def get_dashboard(
    dashboard_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    d = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .first()
    )
    if not d:
        raise HTTPException(status_code=404, detail="Dashboard not found.")
    return {
        "id": d.id,
        "file_name": d.file_name,
        "created_at": d.created_at.isoformat(),
        "mapping": json.loads(d.mapping_json),
        "dashboard": json.loads(d.dashboard_json),
    }

# =============================================================
# PDF EXPORT (Multi-language, 2-page, sleek)
# =============================================================

# ---------------- PDF EXPORT (Styled, multi-language, card layout) ----------------

# Simple language packs for PDF labels
LANG_MAP = {
    "en": {
        "report_title": "Sales Insights Report",
        "details_title": "Sales Details",
        "file_label": "File",
        "date_range_label": "Data range",
        "generated_label": "Generated",
        "total_revenue": "Total Revenue",
        "avg_sale": "Average Sale",
        "num_sales": "Number of Sales",
        "period_total": "Period Total",
        "mean_value": "Mean Value",
        "transactions": "Transactions",
        "data_quality": "Data Quality",
        "rows_in_file": "Rows in file",
        "invalid_amount": "Invalid amount removed",
        "invalid_date": "Invalid date removed",
        "rows_used": "Rows used for insights",
        "revenue_over_time": "Revenue Over Time",
        "weekday_pattern": "Revenue by Day of Week",
        "top_products": "Top Products",
        "top_customers": "Top Customers",
        "dimension_breakdown": "By Dimension",
        "key_insights": "Key Insights",
        "product": "Product",
        "customer": "Customer",
        "revenue": "Revenue",
        "orders": "# Orders",
        "percent_of_total": "% of total",
        "dimension": "Label",
        "footer": "Generated by your analytics dashboard",
    },
    "es": {
        "report_title": "Informe de Ventas",
        "details_title": "Detalles de Ventas",
        "file_label": "Archivo",
        "date_range_label": "Rango de datos",
        "generated_label": "Generado",
        "total_revenue": "Ingresos Totales",
        "avg_sale": "Venta Promedio",
        "num_sales": "Número de Ventas",
        "period_total": "Total del período",
        "mean_value": "Valor medio",
        "transactions": "Transacciones",
        "data_quality": "Calidad de Datos",
        "rows_in_file": "Filas en el archivo",
        "invalid_amount": "Importes inválidos eliminados",
        "invalid_date": "Fechas inválidas eliminadas",
        "rows_used": "Filas usadas",
        "revenue_over_time": "Ingresos en el Tiempo",
        "weekday_pattern": "Ingresos por Día de la Semana",
        "top_products": "Productos Principales",
        "top_customers": "Clientes Principales",
        "dimension_breakdown": "Por Dimensión",
        "key_insights": "Conclusiones Clave",
        "product": "Producto",
        "customer": "Cliente",
        "revenue": "Ingresos",
        "orders": "# Pedidos",
        "percent_of_total": "% del total",
        "dimension": "Etiqueta",
        "footer": "Generado por su panel de análisis",
    },
    "fr": {
        "report_title": "Rapport d’Analyse des Ventes",
        "details_title": "Détails des Ventes",
        "file_label": "Fichier",
        "date_range_label": "Période de données",
        "generated_label": "Généré",
        "total_revenue": "Chiffre d’affaires",
        "avg_sale": "Vente moyenne",
        "num_sales": "Nombre de ventes",
        "period_total": "Total de la période",
        "mean_value": "Valeur moyenne",
        "transactions": "Transactions",
        "data_quality": "Qualité des Données",
        "rows_in_file": "Lignes dans le fichier",
        "invalid_amount": "Montants invalides supprimés",
        "invalid_date": "Dates invalides supprimées",
        "rows_used": "Lignes utilisées",
        "revenue_over_time": "Revenus dans le temps",
        "weekday_pattern": "Revenus par jour de semaine",
        "top_products": "Meilleurs Produits",
        "top_customers": "Meilleurs Clients",
        "dimension_breakdown": "Par Dimension",
        "key_insights": "Points Clés",
        "product": "Produit",
        "customer": "Client",
        "revenue": "Revenus",
        "orders": "# Commandes",
        "percent_of_total": "% du total",
        "dimension": "Libellé",
        "footer": "Généré par votre tableau de bord d’analyse",
    },
    "de": {
        "report_title": "Umsatzanalyse Bericht",
        "details_title": "Verkaufsdetails",
        "file_label": "Datei",
        "date_range_label": "Datenzeitraum",
        "generated_label": "Erstellt",
        "total_revenue": "Gesamtumsatz",
        "avg_sale": "Durchschnittlicher Verkauf",
        "num_sales": "Anzahl Verkäufe",
        "period_total": "Zeitraum gesamt",
        "mean_value": "Durchschnittswert",
        "transactions": "Transaktionen",
        "data_quality": "Datenqualität",
        "rows_in_file": "Zeilen in Datei",
        "invalid_amount": "Ungültige Beträge entfernt",
        "invalid_date": "Ungültige Daten entfernt",
        "rows_used": "Verwendete Zeilen",
        "revenue_over_time": "Umsatz im Zeitverlauf",
        "weekday_pattern": "Umsatz nach Wochentag",
        "top_products": "Top-Produkte",
        "top_customers": "Top-Kunden",
        "dimension_breakdown": "Nach Dimension",
        "key_insights": "Wichtige Erkenntnisse",
        "product": "Produkt",
        "customer": "Kunde",
        "revenue": "Umsatz",
        "orders": "# Bestellungen",
        "percent_of_total": "% vom Gesamt",
        "dimension": "Bezeichnung",
        "footer": "Erstellt von Ihrem Analytics-Dashboard",
    },
}


def t(lang: str, key: str) -> str:
    """Translate a label key into the chosen language, fallback to English."""
    lang = (lang or "en").lower()
    base = LANG_MAP.get("en", {})
    trans = LANG_MAP.get(lang, {})
    return trans.get(key, base.get(key, key))


# ---------- drawing helpers ----------

def draw_rounded_card(c, x, y, w, h, radius=16, shadow=True):
    """
    Base 'card' used everywhere: soft rounded corners + subtle drop shadow.
    Ink-friendly: shadow is very light, card fill is white.
    """
    if shadow:
        c.setFillColor(colors.HexColor("#E5E7EB"))
        c.roundRect(x + 2.5, y - 2.5, w, h, radius, fill=1, stroke=0)

    c.setFillColor(colors.white)
    c.roundRect(x, y, w, h, radius, fill=1, stroke=0)

    # light border for separation when printing
    c.setStrokeColor(colors.HexColor("#E5E7EB"))
    c.roundRect(x, y, w, h, radius, fill=0, stroke=1)


def draw_bar_chart(c, x, y, w, h, data, labels, color_hex, value_formatter):
    """Simple bar chart used for revenue over time and weekday patterns."""
    if not data:
        return

    max_val = max(data) or 1
    padding_x = 26
    padding_bottom = 28
    padding_top = 20

    inner_w = w - 2 * padding_x
    inner_h = h - padding_bottom - padding_top
    n = len(data)
    if n == 0:
        return

    bar_w = inner_w / n

    # Determine how dense x-axis labels and value labels should be to avoid crowding
    if n <= 12:
        label_step = 1
    elif n <= 24:
        label_step = 2
    else:
        label_step = max(1, math.ceil(n / 12))

    for i, v in enumerate(data):
        bar_x = x + padding_x + i * bar_w
        bar_height = (v / max_val) * inner_h
        bar_y = y + padding_bottom

        # bar
        c.setFillColor(colors.HexColor(color_hex))
        c.roundRect(bar_x, bar_y, bar_w * 0.55, max(bar_height, 2), 4, fill=1, stroke=0)

        # value label (thin these out a bit for very dense charts)
        show_value = n <= 16 or i % label_step == 0
        if show_value:
            c.setFillColor(colors.HexColor(TEXT_PRIMARY))
            c.setFont("Helvetica", 7)
            c.drawCentredString(
                bar_x + bar_w * 0.275,
                bar_y + bar_height + 9,
                value_formatter(v),
            )

        # x-axis label (only every label_step to prevent overlap)
        if i % label_step == 0 and i < len(labels):
            c.setFillColor(colors.HexColor(TEXT_MUTED))
            c.setFont("Helvetica", 7)
            c.drawCentredString(
                bar_x + bar_w * 0.275,
                y + 10,
                labels[i][:10],
            )


def draw_pie_chart(c, center_x, center_y, radius, items, colors_hex):
    """Very simple pie chart for top products/customers."""
    if not items:
        return
    total = sum(v for _, v in items) or 1
    start_angle = 90
    for idx, (label, value) in enumerate(items):
        angle = float(value) / total * 360.0
        c.setFillColor(colors.HexColor(colors_hex[idx % len(colors_hex)]))
        c.wedge(
            center_x - radius,
            center_y - radius,
            center_x + radius,
            center_y + radius,
            start_angle,
            start_angle - angle,
            stroke=0,
            fill=1,
        )
        start_angle -= angle


def build_safe_pdf_filename(file_name: Optional[str]) -> str:
    base_name = (file_name or "dashboard").strip()

    if not base_name:
        base_name = "dashboard"

    base_name = re.sub(r"[^A-Za-z0-9._-]+", "_", base_name)
    base_name = base_name.strip("._-") or "dashboard"

    if base_name.lower().endswith(".csv"):
        base_name = base_name[:-4] or "dashboard"

    if not base_name.lower().endswith(".pdf"):
        base_name = f"{base_name}.pdf"

    return base_name


def build_dashboard_pdf_fallback_bytes(
    file_name: str,
    dashboard_payload: Dict[str, Any],
) -> bytes:
    """
    Fallback PDF generator using ReportLab.
    This version is designed to feel cleaner, more structured,
    and much closer to a polished dashboard report.
    """
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    page_width, page_height = A4

    margin_x = 42
    top_margin = 46
    bottom_margin = 42
    content_width = page_width - (margin_x * 2)

    summary = dashboard_payload.get("summary", {}) or {}
    charts = dashboard_payload.get("charts", {}) or {}
    date_range = summary.get("date_range", {}) or {}

    currency_symbol = get_email_currency_symbol(dashboard_payload)

    total_revenue = format_email_money(summary.get("total_revenue"), currency_symbol)
    average_sale = format_email_money(summary.get("average_sale"), currency_symbol)

    num_sales_value = summary.get("num_sales")
    if isinstance(num_sales_value, int):
        orders_text = f"{num_sales_value:,}"
    elif num_sales_value is not None:
        orders_text = str(num_sales_value)
    else:
        orders_text = "—"

    top_products = (charts.get("top_products") or [])[:5]
    top_customers = (charts.get("top_customers") or [])[:5]
    insights = extract_email_insights(dashboard_payload)[:4]

    top_customer_name = "—"
    if top_customers:
        top_customer_name = str(top_customers[0].get("customer") or "—")

    y = page_height - top_margin

    def reset_page_state() -> None:
        nonlocal y
        y = page_height - top_margin

    def new_page() -> None:
        pdf.showPage()
        reset_page_state()

    def ensure_space(height_needed: float) -> None:
        nonlocal y
        if y - height_needed < bottom_margin:
            new_page()

    def draw_page_background() -> None:
        pdf.setFillColor(colors.white)
        pdf.rect(0, 0, page_width, page_height, fill=1, stroke=0)

    def draw_header() -> None:
        nonlocal y

        draw_page_background()

        pdf.setFillColor(colors.HexColor("#0F172A"))
        pdf.roundRect(margin_x, y - 52, content_width, 56, 14, fill=1, stroke=0)

        pdf.setFillColor(colors.HexColor("#A7F3D0"))
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(margin_x + 16, y - 18, "EASY-DASH")

        pdf.setFillColor(colors.white)
        pdf.setFont("Helvetica-Bold", 18)
        pdf.drawString(margin_x + 16, y - 37, "Dashboard report")

        y -= 72

        pdf.setFillColor(colors.HexColor(TEXT_PRIMARY))
        pdf.setFont("Helvetica-Bold", 15)
        pdf.drawString(margin_x, y, file_name[:80] or "Dashboard")
        y -= 16

        pdf.setFillColor(colors.HexColor(TEXT_SECONDARY))
        pdf.setFont("Helvetica", 10)
        pdf.drawString(
            margin_x,
            y,
            f"Date range: {date_range.get('start', '—')} to {date_range.get('end', '—')}",
        )
        y -= 22

    def draw_footer() -> None:
        pdf.setStrokeColor(colors.HexColor("#E5E7EB"))
        pdf.line(margin_x, 28, page_width - margin_x, 28)
        pdf.setFillColor(colors.HexColor(TEXT_MUTED))
        pdf.setFont("Helvetica", 8)
        pdf.drawString(margin_x, 16, "Generated by Easy-dash")

    def finish_page() -> None:
        draw_footer()
        pdf.showPage()
        reset_page_state()

    def section_title(title: str, subtitle: Optional[str] = None) -> None:
        nonlocal y
        ensure_space(36)

        pdf.setFillColor(colors.HexColor(TEXT_PRIMARY))
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(margin_x, y, title)
        y -= 16

        if subtitle:
            pdf.setFillColor(colors.HexColor(TEXT_SECONDARY))
            pdf.setFont("Helvetica", 9)
            pdf.drawString(margin_x, y, subtitle)
            y -= 14

        y -= 4

    def draw_stat_card_row(cards: List[Tuple[str, str]]) -> None:
        nonlocal y
        ensure_space(92)

        gap = 10
        card_width = (content_width - (gap * (len(cards) - 1))) / len(cards)
        card_height = 74
        card_y = y - card_height

        for index, (label, value) in enumerate(cards):
            x = margin_x + index * (card_width + gap)

            pdf.setFillColor(colors.HexColor("#F8FAFC"))
            pdf.roundRect(x, card_y, card_width, card_height, 12, fill=1, stroke=0)

            pdf.setStrokeColor(colors.HexColor("#E5E7EB"))
            pdf.roundRect(x, card_y, card_width, card_height, 12, fill=0, stroke=1)

            pdf.setFillColor(colors.HexColor(TEXT_MUTED))
            pdf.setFont("Helvetica-Bold", 8)
            pdf.drawString(x + 12, card_y + card_height - 18, label.upper())

            pdf.setFillColor(colors.HexColor(TEXT_PRIMARY))
            pdf.setFont("Helvetica-Bold", 15)
            pdf.drawString(x + 12, card_y + 24, value[:24])

        y = card_y - 18

    def draw_ranked_table(
        title: str,
        subtitle: str,
        rows: List[Tuple[str, str]],
    ) -> None:
        nonlocal y

        section_title(title, subtitle)

        if not rows:
            ensure_space(38)
            pdf.setFillColor(colors.HexColor("#F8FAFC"))
            pdf.roundRect(margin_x, y - 28, content_width, 30, 10, fill=1, stroke=0)
            pdf.setStrokeColor(colors.HexColor("#E5E7EB"))
            pdf.roundRect(margin_x, y - 28, content_width, 30, 10, fill=0, stroke=1)
            pdf.setFillColor(colors.HexColor(TEXT_SECONDARY))
            pdf.setFont("Helvetica", 10)
            pdf.drawString(margin_x + 12, y - 18, "No data available")
            y -= 42
            return

        row_height = 28

        for index, (left_text, right_text) in enumerate(rows):
            ensure_space(row_height + 8)

            row_y = y - row_height
            fill_color = "#FFFFFF" if index % 2 == 0 else "#F8FAFC"

            pdf.setFillColor(colors.HexColor(fill_color))
            pdf.roundRect(margin_x, row_y, content_width, row_height, 8, fill=1, stroke=0)

            pdf.setStrokeColor(colors.HexColor("#E5E7EB"))
            pdf.roundRect(margin_x, row_y, content_width, row_height, 8, fill=0, stroke=1)

            pdf.setFillColor(colors.HexColor(TEXT_PRIMARY))
            pdf.setFont("Helvetica", 10)
            pdf.drawString(margin_x + 12, row_y + 10, left_text[:60])

            pdf.setFont("Helvetica-Bold", 10)
            pdf.drawRightString(page_width - margin_x - 12, row_y + 10, right_text)

            y = row_y - 6

        y -= 8

    def draw_insights_block(items: List[Dict[str, str]]) -> None:
        nonlocal y

        section_title("Key insights", "Smart takeaways from your current dashboard view")

        if not items:
            ensure_space(40)
            pdf.setFillColor(colors.HexColor("#F8FAFC"))
            pdf.roundRect(margin_x, y - 30, content_width, 32, 10, fill=1, stroke=0)
            pdf.setStrokeColor(colors.HexColor("#E5E7EB"))
            pdf.roundRect(margin_x, y - 30, content_width, 32, 10, fill=0, stroke=1)
            pdf.setFillColor(colors.HexColor(TEXT_SECONDARY))
            pdf.setFont("Helvetica", 10)
            pdf.drawString(margin_x + 12, y - 19, "No insights available")
            y -= 46
            return

        for index, insight in enumerate(items):
            title = str(insight.get("title") or "Insight")
            message = str(insight.get("message") or "")
            wrapped_lines = textwrap.wrap(message, width=88) or [""]

            block_height = 24 + (len(wrapped_lines) * 13) + 16
            ensure_space(block_height + 8)

            block_y = y - block_height
            fill_color = "#FEF3C7" if index == 0 else "#F8FAFC"
            stroke_color = "#F59E0B" if index == 0 else "#E5E7EB"

            pdf.setFillColor(colors.HexColor(fill_color))
            pdf.roundRect(margin_x, block_y, content_width, block_height, 12, fill=1, stroke=0)

            pdf.setStrokeColor(colors.HexColor(stroke_color))
            pdf.roundRect(margin_x, block_y, content_width, block_height, 12, fill=0, stroke=1)

            pdf.setFillColor(colors.HexColor(TEXT_PRIMARY))
            pdf.setFont("Helvetica-Bold", 11)
            pdf.drawString(margin_x + 12, block_y + block_height - 18, title[:80])

            current_y = block_y + block_height - 34
            pdf.setFont("Helvetica", 10)
            for line in wrapped_lines:
                pdf.drawString(margin_x + 12, current_y, line)
                current_y -= 13

            y = block_y - 8

    draw_header()

    draw_stat_card_row([
        ("Total revenue", total_revenue),
        ("Orders", orders_text),
        ("Average order value", average_sale),
        ("Top customer", top_customer_name),
    ])

    draw_ranked_table(
        "Top products",
        "Highest-performing products by revenue",
        [
            (
                str(item.get("product") or "Unknown product"),
                format_email_money(item.get("revenue"), currency_symbol),
            )
            for item in top_products
        ],
    )

    draw_ranked_table(
        "Top customers",
        "Customer performance ranking",
        [
            (
                str(item.get("customer") or "Unknown customer"),
                format_email_money(item.get("revenue"), currency_symbol),
            )
            for item in top_customers
        ],
    )

    draw_insights_block(insights)

    draw_footer()
    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()




@app.get("/dashboard/{dashboard_id}/pdf")
def download_dashboard_pdf(
    dashboard_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not user_has_active_subscription(current_user):
        raise HTTPException(
            status_code=403,
            detail="PDF export is available on Pro only.",
        )

    check_pdf_rate_limit(current_user.id)

    dashboard = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .first()
    )

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found.")

    try:
        dashboard_payload = json.loads(dashboard.dashboard_json or "{}")
    except Exception:
        logger.exception(
            "Invalid saved dashboard JSON for dashboard_id=%s user_id=%s",
            dashboard_id,
            current_user.id,
        )
        raise HTTPException(status_code=500, detail="Saved dashboard data is invalid.")

    safe_filename = build_safe_pdf_filename(dashboard.file_name)

    try:
        try:
            saved_mapping = json.loads(dashboard.mapping_json or "{}")
        except Exception:
            saved_mapping = {}

        html_content = build_dashboard_report_email_html(
            file_name=dashboard.file_name,
            dashboard_payload=dashboard_payload,
            applied_filters=saved_mapping.get("filters", {}) or {},
        )

        pdf_bytes = HTML(string=html_content).write_pdf()

        if not pdf_bytes:
            raise ValueError("WeasyPrint returned empty PDF bytes")

        logger.info(
            "PDF generated with WeasyPrint for dashboard_id=%s user_id=%s filename=%s bytes=%s",
            dashboard_id,
            current_user.id,
            safe_filename,
            len(pdf_bytes),
        )

    except Exception:
        logger.exception(
            "WeasyPrint PDF generation failed for dashboard_id=%s user_id=%s. Falling back to ReportLab.",
            dashboard_id,
            current_user.id,
        )

        try:
            pdf_bytes = build_dashboard_pdf_fallback_bytes(
                file_name=dashboard.file_name,
                dashboard_payload=dashboard_payload,
            )
        except Exception:
            logger.exception(
                "ReportLab fallback PDF generation also failed for dashboard_id=%s user_id=%s",
                dashboard_id,
                current_user.id,
            )
            raise HTTPException(status_code=500, detail="Failed to generate PDF.")

        if not pdf_bytes:
            logger.error(
                "Fallback PDF generation returned empty bytes for dashboard_id=%s user_id=%s",
                dashboard_id,
                current_user.id,
            )
            raise HTTPException(status_code=500, detail="Failed to generate PDF.")

        logger.info(
            "PDF generated with ReportLab fallback for dashboard_id=%s user_id=%s filename=%s bytes=%s",
            dashboard_id,
            current_user.id,
            safe_filename,
            len(pdf_bytes),
        )

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_filename}"',
            "Content-Length": str(len(pdf_bytes)),
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "Access-Control-Expose-Headers": "Content-Disposition, Content-Length, Content-Type",
        },
    )