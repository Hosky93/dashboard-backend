# =============================================================
# backend.py  –  Small Business Insights SaaS
# =============================================================

from fastapi import (
    FastAPI, UploadFile, File, Depends, HTTPException, status, Form, Request, Header
)
from fastapi.responses import JSONResponse, FileResponse
from fastapi.security import OAuth2PasswordBearer
from fastapi.middleware.cors import CORSMiddleware
from uuid import uuid4

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
import smtplib


from email.message import EmailMessage
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

SECRET_KEY = os.getenv("SECRET_KEY", "").strip() or "dev_only_change_me"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

MAX_FILE_SIZE_MB = 10
MAX_DASHBOARDS_PER_HOUR = 20

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

EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "").strip()
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))
EMAIL_SMTP_USERNAME = os.getenv("EMAIL_SMTP_USERNAME", "").strip()
EMAIL_SMTP_PASSWORD = os.getenv("EMAIL_SMTP_PASSWORD", "").strip()
EMAIL_FROM_ADDRESS = os.getenv("EMAIL_FROM_ADDRESS", "").strip()
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "Small Business Insights")
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS", "true").strip().lower() in {"true", "1", "yes", "on"}
EMAIL_SEND_TIMEOUT_SECONDS = int(os.getenv("EMAIL_SEND_TIMEOUT_SECONDS", "20"))

stripe.api_key = STRIPE_SECRET_KEY or None

# =============================================================
# DATABASE INIT
# =============================================================

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
)
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

Base.metadata.create_all(bind=engine)

def run_sqlite_safe_migrations() -> None:
    """
    Tiny migration helper for local SQLite development.
    Adds missing SavedView scheduling columns if they do not exist yet.
    Also normalises old report_enabled values into 0/1 form for SQLite.
    """
    try:
        with engine.connect() as conn:
            result = conn.exec_driver_sql("PRAGMA table_info(saved_views)")
            columns = result.fetchall()
            existing_columns = {row[1] for row in columns}

            if "last_report_sent_at" not in existing_columns:
                conn.exec_driver_sql(
                    "ALTER TABLE saved_views ADD COLUMN last_report_sent_at DATETIME"
                )

            if "last_report_started_at" not in existing_columns:
                conn.exec_driver_sql(
                    "ALTER TABLE saved_views ADD COLUMN last_report_started_at DATETIME"
                )

            if "last_report_error" not in existing_columns:
                conn.exec_driver_sql(
                    "ALTER TABLE saved_views ADD COLUMN last_report_error TEXT"
                )

            # Normalize old string-like values into SQLite boolean style (1 / 0)
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
        logger.exception("Failed running SQLite safe migrations.")
        raise


run_sqlite_safe_migrations()

# =============================================================
# EMAIL
# =============================================================

def is_email_configured() -> bool:
    return all(
        [
            EMAIL_SMTP_HOST,
            EMAIL_SMTP_PORT,
            EMAIL_SMTP_USERNAME,
            EMAIL_SMTP_PASSWORD,
            EMAIL_FROM_ADDRESS,
        ]
    )


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
) -> None:
    """
    Send a plain text email using SMTP.

    Raises:
        HTTPException: for expected configuration / validation problems
        Exception: for unexpected SMTP failures
    """
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
        logger.error("Email sending attempted but SMTP environment variables are incomplete.")
        raise HTTPException(
            status_code=500,
            detail="Email is not configured on the server.",
        )

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = build_from_header()
    message["To"] = to_email
    message.set_content(body_text)

    if body_html:
        message.add_alternative(body_html, subtype="html")

    try:
        with smtplib.SMTP(
            EMAIL_SMTP_HOST,
            EMAIL_SMTP_PORT,
            timeout=EMAIL_SEND_TIMEOUT_SECONDS,
        ) as smtp:
            if EMAIL_USE_TLS:
                smtp.starttls()

            smtp.login(EMAIL_SMTP_USERNAME, EMAIL_SMTP_PASSWORD)
            smtp.send_message(message)

        logger.info(
            "Email sent successfully to=%s subject=%s",
            to_email,
            subject,
        )

    except HTTPException:
        raise
    except Exception:
        logger.exception(
            "Failed to send email to=%s subject=%s",
            to_email,
            subject,
        )
        raise HTTPException(
            status_code=500,
            detail="Failed to send email.",
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


def send_saved_view_report_email(
    *,
    saved_view: SavedView,
    dashboard: Dashboard,
) -> None:
    try:
        dashboard_payload = json.loads(dashboard.dashboard_json or "{}")
    except Exception:
        raise HTTPException(status_code=500, detail="Saved dashboard data is invalid.")

    email_body_text = build_dashboard_report_email_text(
        file_name=dashboard.file_name,
        dashboard_payload=dashboard_payload,
    )

    email_body_html = build_dashboard_report_email_html(
        file_name=dashboard.file_name,
        dashboard_payload=dashboard_payload,
    )

    send_email_message(
        to_email=(saved_view.report_recipient or "").strip(),
        subject=f"Your dashboard report: {dashboard.file_name}",
        body_text=email_body_text,
        body_html=email_body_html,
    )

def build_dashboard_report_email_text(
    file_name: str,
    dashboard_payload: Dict[str, Any],
) -> str:
    summary = dashboard_payload.get("summary", {}) or {}
    charts = dashboard_payload.get("charts", {}) or {}

    total_revenue = summary.get("total_revenue")
    num_sales = summary.get("num_sales")
    average_sale = summary.get("average_sale")
    date_range = summary.get("date_range", {}) or {}

    top_products = (charts.get("top_products") or [])[:5]
    top_customers = (charts.get("top_customers") or [])[:5]
    insights = (dashboard_payload.get("insights") or [])[:5]

    lines = [
        "Hello,",
        "",
        f"Here is your Small Business Insights report for: {file_name}",
        "",
        "SUMMARY",
        f"- Total revenue: £{total_revenue:,.2f}" if isinstance(total_revenue, (int, float)) else "- Total revenue: —",
        f"- Orders: {num_sales:,}" if isinstance(num_sales, int) else f"- Orders: {num_sales}" if num_sales is not None else "- Orders: —",
        f"- Average order value: £{average_sale:,.2f}" if isinstance(average_sale, (int, float)) else "- Average order value: —",
        f"- Date range: {date_range.get('start', '—')} to {date_range.get('end', '—')}",
        "",
        "TOP PRODUCTS",
    ]

    if top_products:
        for item in top_products:
            product_name = item.get("product") or "Unknown product"
            revenue = item.get("revenue")
            revenue_text = f"£{revenue:,.2f}" if isinstance(revenue, (int, float)) else "—"
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
            revenue = item.get("revenue")
            revenue_text = f"£{revenue:,.2f}" if isinstance(revenue, (int, float)) else "—"
            lines.append(f"- {customer_name}: {revenue_text}")
    else:
        lines.append("- No customer data available")

    lines.extend([
        "",
        "KEY INSIGHTS",
    ])

    if insights:
        for insight in insights:
            lines.append(f"- {insight}")
    else:
        lines.append("- No insights available")

    lines.extend([
        "",
        "Generated by Small Business Insights.",
    ])

    return "\n".join(lines)

def build_dashboard_report_email_html(
    file_name: str,
    dashboard_payload: Dict[str, Any],
) -> str:
    summary = dashboard_payload.get("summary", {}) or {}
    charts = dashboard_payload.get("charts", {}) or {}

    total_revenue = summary.get("total_revenue")
    num_sales = summary.get("num_sales")
    average_sale = summary.get("average_sale")
    date_range = summary.get("date_range", {}) or {}

    top_products = (charts.get("top_products") or [])[:5]
    top_customers = (charts.get("top_customers") or [])[:5]
    insights = (dashboard_payload.get("insights") or [])[:5]

    revenue_text = f"£{total_revenue:,.2f}" if isinstance(total_revenue, (int, float)) else "—"
    orders_text = f"{num_sales:,}" if isinstance(num_sales, int) else str(num_sales) if num_sales is not None else "—"
    aov_text = f"£{average_sale:,.2f}" if isinstance(average_sale, (int, float)) else "—"
    date_range_text = f"{date_range.get('start', '—')} to {date_range.get('end', '—')}"

    top_products_html = ""
    if top_products:
        for item in top_products:
            product_name = html_escape(item.get("product") or "Unknown product")
            revenue = item.get("revenue")
            revenue_value = f"£{revenue:,.2f}" if isinstance(revenue, (int, float)) else "—"
            top_products_html += f"""
                <tr>
                    <td style="padding:12px 14px;border-bottom:1px solid #1e293b;color:#e5e7eb;font-size:14px;">{product_name}</td>
                    <td style="padding:12px 14px;border-bottom:1px solid #1e293b;color:#ffffff;font-size:14px;font-weight:600;text-align:right;">{revenue_value}</td>
                </tr>
            """
    else:
        top_products_html = """
            <tr>
                <td colspan="2" style="padding:14px;color:#94a3b8;font-size:14px;">No product data available</td>
            </tr>
        """

    top_customers_html = ""
    if top_customers:
        for item in top_customers:
            customer_name = html_escape(item.get("customer") or "Unknown customer")
            revenue = item.get("revenue")
            revenue_value = f"£{revenue:,.2f}" if isinstance(revenue, (int, float)) else "—"
            top_customers_html += f"""
                <tr>
                    <td style="padding:12px 14px;border-bottom:1px solid #1e293b;color:#e5e7eb;font-size:14px;">{customer_name}</td>
                    <td style="padding:12px 14px;border-bottom:1px solid #1e293b;color:#ffffff;font-size:14px;font-weight:600;text-align:right;">{revenue_value}</td>
                </tr>
            """
    else:
        top_customers_html = """
            <tr>
                <td colspan="2" style="padding:14px;color:#94a3b8;font-size:14px;">No customer data available</td>
            </tr>
        """

    insights_html = ""
    if insights:
        for insight in insights:
            insights_html += f"""
                <div style="margin-bottom:12px;padding:14px 16px;border:1px solid #1f2937;border-radius:14px;background:#0f172a;color:#e5e7eb;font-size:14px;line-height:1.6;">
                    {html_escape(insight)}
                </div>
            """
    else:
        insights_html = """
            <div style="padding:14px 16px;border:1px solid #1f2937;border-radius:14px;background:#0f172a;color:#94a3b8;font-size:14px;">
                No insights available
            </div>
        """

    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Dashboard Report</title>
</head>
<body style="margin:0;padding:0;background:#020617;font-family:Arial,Helvetica,sans-serif;">
  <div style="margin:0;padding:32px 16px;background:#020617;">
    <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="max-width:760px;margin:0 auto;border-collapse:collapse;">
      <tr>
        <td style="padding:0;">
          <div style="border:1px solid #1e293b;border-radius:24px;overflow:hidden;background:linear-gradient(135deg,#0f172a 0%,#020617 100%);box-shadow:0 0 30px rgba(0,0,0,0.35);">
            
            <div style="padding:28px 28px 20px 28px;border-bottom:1px solid #1e293b;">
              <div style="display:inline-block;padding:6px 12px;border-radius:999px;background:#10b9811a;border:1px solid #10b98133;color:#a7f3d0;font-size:12px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;">
                Small Business Insights
              </div>
              <h1 style="margin:16px 0 8px 0;color:#ffffff;font-size:28px;line-height:1.2;font-weight:700;">
                Your dashboard report
              </h1>
              <p style="margin:0;color:#94a3b8;font-size:15px;line-height:1.6;">
                {html_escape(file_name)}
              </p>
            </div>

            <div style="padding:24px 28px;">
              <div style="margin-bottom:22px;padding:18px 20px;border:1px solid #1e293b;border-radius:18px;background:#081225;">
                <p style="margin:0 0 8px 0;color:#cbd5e1;font-size:14px;line-height:1.7;">
                  Here is your latest dashboard summary, styled for inbox reading.
                </p>
                <p style="margin:0;color:#94a3b8;font-size:13px;">
                  Date range: {html_escape(date_range_text)}
                </p>
              </div>

              <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="margin-bottom:22px;border-collapse:separate;border-spacing:12px 12px;">
                <tr>
                  <td width="33.33%" style="padding:18px;border:1px solid #1e293b;border-radius:18px;background:#0b1220;">
                    <div style="color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px;">Total revenue</div>
                    <div style="color:#ffffff;font-size:28px;font-weight:700;">{revenue_text}</div>
                  </td>
                  <td width="33.33%" style="padding:18px;border:1px solid #1e293b;border-radius:18px;background:#0b1220;">
                    <div style="color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px;">Orders</div>
                    <div style="color:#ffffff;font-size:28px;font-weight:700;">{orders_text}</div>
                  </td>
                  <td width="33.33%" style="padding:18px;border:1px solid #1e293b;border-radius:18px;background:#0b1220;">
                    <div style="color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px;">Average order value</div>
                    <div style="color:#ffffff;font-size:28px;font-weight:700;">{aov_text}</div>
                  </td>
                </tr>
              </table>

              <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="margin-bottom:22px;border-collapse:separate;border-spacing:0 0;">
                <tr>
                  <td valign="top" width="50%" style="padding-right:8px;">
                    <div style="border:1px solid #1e293b;border-radius:20px;background:#0b1220;overflow:hidden;">
                      <div style="padding:18px 18px 14px 18px;border-bottom:1px solid #1e293b;">
                        <h2 style="margin:0;color:#ffffff;font-size:20px;">Top products</h2>
                        <p style="margin:6px 0 0 0;color:#94a3b8;font-size:13px;">Highest-performing products by revenue</p>
                      </div>
                      <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="border-collapse:collapse;">
                        {top_products_html}
                      </table>
                    </div>
                  </td>
                  <td valign="top" width="50%" style="padding-left:8px;">
                    <div style="border:1px solid #1e293b;border-radius:20px;background:#0b1220;overflow:hidden;">
                      <div style="padding:18px 18px 14px 18px;border-bottom:1px solid #1e293b;">
                        <h2 style="margin:0;color:#ffffff;font-size:20px;">Top customers</h2>
                        <p style="margin:6px 0 0 0;color:#94a3b8;font-size:13px;">Customer performance ranking</p>
                      </div>
                      <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="border-collapse:collapse;">
                        {top_customers_html}
                      </table>
                    </div>
                  </td>
                </tr>
              </table>

              <div style="border:1px solid #1e293b;border-radius:20px;background:#0b1220;padding:20px;">
                <h2 style="margin:0 0 8px 0;color:#ffffff;font-size:20px;">Key insights</h2>
                <p style="margin:0 0 18px 0;color:#94a3b8;font-size:13px;">Smart takeaways from your latest dashboard data</p>
                {insights_html}
              </div>
            </div>

            <div style="padding:18px 28px;border-top:1px solid #1e293b;background:#07101f;">
              <p style="margin:0;color:#94a3b8;font-size:12px;line-height:1.6;">
                Generated by Small Business Insights.
              </p>
            </div>
          </div>
        </td>
      </tr>
    </table>
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

def user_has_active_subscription(user: User) -> bool:
    return (user.subscription_status or "").strip().lower() == "active"


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
PDF_RATE_LIMIT: Dict[int, List[datetime]] = {}


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

# =============================================================
# APP INIT
# =============================================================

app = FastAPI(title="Small Business Insights SaaS API - REANALYZE LIVE")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/test-email")
async def send_test_email(data: TestEmailRequest):
    recipient = (data.to_email or "").strip()

    if not recipient:
        raise HTTPException(status_code=400, detail="Recipient email is required.")

    send_email_message(
        to_email=recipient,
        subject="Small Business Insights - Test Email",
        body_text=(
            f"Hello,\n\n"
            f"This is a test email from your Small Business Insights SaaS.\n\n"
            f"Sent at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
            f"If you received this, your email sending is working."
        ),
    )

    return {
        "status": "success",
        "message": f"Test email sent successfully to {recipient}",
    }

@app.post("/send-dashboard-report")
async def send_dashboard_report(
    data: SendDashboardReportRequest,
    db: Session = Depends(get_db),
):
    recipient = (data.to_email or "").strip()

    if not recipient:
        raise HTTPException(status_code=400, detail="Recipient email is required.")

    dashboard = (
        db.query(Dashboard)
        .filter(
            Dashboard.id == data.dashboard_id,
        )
        .first()
    )

    if not dashboard:
        raise HTTPException(status_code=404, detail="Dashboard not found.")

    try:
        dashboard_payload = json.loads(dashboard.dashboard_json or "{}")
    except Exception:
        raise HTTPException(status_code=500, detail="Saved dashboard data is invalid.")

    email_body_text = build_dashboard_report_email_text(
        file_name=dashboard.file_name,
        dashboard_payload=dashboard_payload,
    )

    email_body_html = build_dashboard_report_email_html(
        file_name=dashboard.file_name,
        dashboard_payload=dashboard_payload,
    )

    send_email_message(
        to_email=recipient,
        subject=f"Your dashboard report: {dashboard.file_name}",
        body_text=email_body_text,
        body_html=email_body_html,
    )

    return {
        "status": "success",
        "message": f"Dashboard report sent successfully to {recipient}",
        "dashboard_id": dashboard.id,
        "file_name": dashboard.file_name,
    }

@app.post("/reports/run-scheduled")
async def run_scheduled_reports(
    request: Request,
    db: Session = Depends(get_db),
    x_cron_secret: str | None = Header(default=None, alias="X-Cron-Secret"),
):
    auth_header = request.headers.get("authorization", "").strip()
    is_user_request = auth_header.lower().startswith("bearer ")

    if is_user_request:
        await get_current_user(token=auth_header.split(" ", 1)[1], db=db)
    else:
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

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
):
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

    mapping = {
        "date": detected_date_col,
        "amount": (saved_mapping or {}).get("amount") or (detected.get("amount") or {}).get("column"),
        "product": (saved_mapping or {}).get("product") or (detected.get("product") or {}).get("column"),
        "customer": (saved_mapping or {}).get("customer") or (detected.get("customer") or {}).get("column"),
        "filter_start_date": "",
        "filter_end_date": "",
        "filter_product": "",
        "filter_customer": "",
        "currency_symbol": "£",
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
        currency_symbol="£",
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

    filter_options = {
        "products": [],
        "customers": [],
        "min_date": "",
        "max_date": "",
    }

    if product_col and product_col in df_clean.columns:
        filter_options["products"] = sorted(
            [
                str(v)
                for v in df_clean[product_col]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
                if str(v).strip()
            ]
        )

    if customer_col and customer_col in df_clean.columns:
        filter_options["customers"] = sorted(
            [
                str(v)
                for v in df_clean[customer_col]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
                if str(v).strip()
            ]
        )

    parsed_dates = pd.Series(dtype="datetime64[ns]")
    chosen_date_parse_mode = saved_date_parse_mode or "month_first"

    if date_col and date_col in df_clean.columns:
        parsed_dates, chosen_date_parse_mode = parse_dates_three_tier(
            df_clean[date_col],
            forced_mode=saved_date_parse_mode,
        )
        valid_dates = parsed_dates.dropna()
        if not valid_dates.empty:
            filter_options["min_date"] = valid_dates.min().date().isoformat()
            filter_options["max_date"] = valid_dates.max().date().isoformat()

    effective_time_grouping = time_grouping

    adaptive_start = filter_start_date or filter_options["min_date"]
    adaptive_end = filter_end_date or filter_options["max_date"]

    if adaptive_start and adaptive_end:
        effective_time_grouping = get_adaptive_time_grouping(adaptive_start, adaptive_end)

    mapping = {
        "date": date_col or None,
        "amount": amount_col or None,
        "product": product_col or None,
        "customer": customer_col or None,
        "filter_start_date": filter_start_date,
        "filter_end_date": filter_end_date,
        "filter_product": parsed_filter_product,
        "filter_customer": parsed_filter_customer,
        "currency_symbol": "£",
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
        currency_symbol="£",
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

    db.delete(dashboard)
    db.commit()

    return {"message": "Dashboard deleted successfully"}

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

    filter_options = {
        "products": [],
        "customers": [],
        "min_date": "",
        "max_date": "",
    }

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

    if product_col and product_col in df_clean.columns:
        filter_options["products"] = sorted(
            [
                str(v)
                for v in df_clean[product_col]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
                if str(v).strip()
            ]
        )

    if customer_col and customer_col in df_clean.columns:
        filter_options["customers"] = sorted(
            [
                str(v)
                for v in df_clean[customer_col]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
                if str(v).strip()
            ]
        )

    parsed_dates = pd.Series(dtype="datetime64[ns]")
    chosen_date_parse_mode = stored_date_parse_mode or "month_first"

    if date_col and date_col in df_clean.columns:
        parsed_dates, chosen_date_parse_mode = parse_dates_three_tier(
            df_clean[date_col],
            forced_mode=stored_date_parse_mode,
        )
        valid_dates = parsed_dates.dropna()
        if not valid_dates.empty:
            filter_options["min_date"] = valid_dates.min().date().isoformat()
            filter_options["max_date"] = valid_dates.max().date().isoformat()

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

    effective_filter_start_date = (filter_start_date or "").strip()
    effective_filter_end_date = (filter_end_date or "").strip()

    effective_time_grouping = time_grouping

    adaptive_start = effective_filter_start_date or filter_options["min_date"]
    adaptive_end = effective_filter_end_date or filter_options["max_date"]

    if adaptive_start and adaptive_end:
        effective_time_grouping = get_adaptive_time_grouping(adaptive_start, adaptive_end)

    mapping = {
        "date": date_col or None,
        "amount": amount_col or None,
        "product": product_col or None,
        "customer": customer_col or None,
        "filter_start_date": effective_filter_start_date,
        "filter_end_date": effective_filter_end_date,
        "filter_product": parsed_filter_product,
        "filter_customer": parsed_filter_customer,
        "currency_symbol": "£",
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
        currency_symbol="£",
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
        "amount": amount_col or None,
        "product": product_col or None,
        "customer": customer_col or None,
        "date_parse_mode": chosen_date_parse_mode,
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
    "filter_options": {
        "products": filter_options["products"],
        "customers": filter_options["customers"],
        "min_date": filter_options["min_date"],
        "max_date": filter_options["max_date"],
    },
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

    # --- 2. Product concentration / standout performance ---
    top_products = charts.get("top_products", []) or []
    if top_products:
        top_product = top_products[0]
        top_product_name = top_product.get("product", "Top product")
        top_product_share = float(top_product.get("share_of_total", 0) or 0)

        if top_product_share >= 40:
            add_smart_insight(
                title="Product concentration risk",
                message=f"{top_product_name} generates {top_product_share:.1f}% of revenue, indicating strong product concentration risk.",
                severity="warning",
                priority=90,
                category="concentration",
                why_it_matters="Heavy dependency on one product increases risk if demand changes or supply is disrupted.",
                suggested_action="Consider broadening revenue across more products or strengthening backup winners.",
            )
        elif top_product_share >= 25:
            add_smart_insight(
                title="Leading product",
                message=f"{top_product_name} is your strongest product, contributing {top_product_share:.1f}% of revenue.",
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
                    message=f"{top_product_name} is materially ahead of the next best product, making it your clearest revenue driver.",
                    severity="info",
                    priority=64,
                    category="highlight",
                    why_it_matters="A standout product often explains a large share of overall performance.",
                    suggested_action="Understand what makes it win and whether those patterns can be applied elsewhere.",
                )

    # --- 3. Customer concentration / dependency ---
    top_customers = charts.get("top_customers", []) or []
    if top_customers:
        top_customer = top_customers[0]
        top_customer_name = top_customer.get("customer", "Top customer")
        top_customer_share = float(top_customer.get("share_of_total", 0) or 0)

        if top_customer_share >= 50:
            add_smart_insight(
                title="Customer concentration risk",
                message=f"{top_customer_name} accounts for {top_customer_share:.1f}% of revenue, creating significant customer concentration risk.",
                severity="warning",
                priority=92,
                category="concentration",
                why_it_matters="Reliance on one customer can create revenue volatility if that account slows or churns.",
                suggested_action="Try to diversify revenue across more customers or strengthen retention planning for this account.",
            )
        elif top_customer_share >= 30:
            add_smart_insight(
                title="Largest customer",
                message=f"{top_customer_name} is your most important customer, contributing {top_customer_share:.1f}% of total revenue.",
                severity="info",
                priority=70,
                category="highlight",
                why_it_matters="This customer has an outsized influence on business performance.",
                suggested_action="Monitor this account closely and look for ways to reduce over-reliance over time.",
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
def signup(payload: SignupRequest = Body(...), db: Session = Depends(get_db)):
    existing_user = get_user_by_email(db, payload.email.lower().strip())
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered.")

    password = payload.password

    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters long.")

    if not re.search(r"[A-Z]", password):
        raise HTTPException(status_code=400, detail="Password must include an uppercase letter.")

    if not re.search(r"[a-z]", password):
        raise HTTPException(status_code=400, detail="Password must include a lowercase letter.")

    if not re.search(r"\d", password):
        raise HTTPException(status_code=400, detail="Password must include a number.")

    user = User(
        email=payload.email.lower().strip(),
        hashed_password=hash_password(payload.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token({"sub": user.email})

    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "email": user.email,
            "subscription_status": user.subscription_status,
        },
    }

@app.post("/auth/login")
def login(payload: LoginRequest = Body(...), db: Session = Depends(get_db)):
    user = get_user_by_email(db, payload.email.lower().strip())
    if not user or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    token = create_access_token({"sub": user.email})

    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "email": user.email,
            "subscription_status": user.subscription_status,
        },
    }

@app.get("/auth/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return {
        "id": current_user.id,
        "email": current_user.email,
        "subscription_status": current_user.subscription_status or "trial",
        "is_paid": user_has_active_subscription(current_user),
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

    if not current_user.stripe_customer_id:
        customer = stripe.Customer.create(email=current_user.email)
        current_user.stripe_customer_id = customer["id"]
        db.add(current_user)
        db.commit()

    session = stripe.checkout.Session.create(
        mode="subscription",
        payment_method_types=["card"],
        customer=current_user.stripe_customer_id,
        line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
        success_url=f"{FRONTEND_BASE_URL}{STRIPE_SUCCESS_PATH}",
        cancel_url=f"{FRONTEND_BASE_URL}{STRIPE_CANCEL_PATH}",
    )
    return {"checkout_url": session.url}


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
        if event_type == "customer.subscription.updated":
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

            # Map Stripe status into our subscription_status field
            if status_stripe == "active":
                user.subscription_status = "active"
            elif status_stripe in {
                "past_due",
                "unpaid",
                "canceled",
                "incomplete",
                "incomplete_expired",
                "trialing",
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
            if customer_id:
                user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
                if user:
                    user.subscription_status = "canceled"
                    db.add(user)
                    db.commit()
                    logger.info(
                        "Marked user %s subscription as 'canceled' from webhook.",
                        user.id,
                    )

        else:
            # Unsupported or unimportant event; just log and ack
            logger.info("Stripe webhook: ignoring unsupported event type '%s'.", event_type)
    except Exception:
        # Never let webhook processing crash the app; log and acknowledge.
        logger.exception("Error handling Stripe webhook event type=%s", event_type)

    return {"received": True}

@app.post("/dashboard/preview")
async def dashboard_preview(file: UploadFile = File(...)):
    if hasattr(file, "size") and file.size and file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large.")

    if not file.filename.endswith((".csv", ".xlsx")):
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

    if not file.filename:
        logger.warning("User %s uploaded a file with no name.", current_user.id)
        raise HTTPException(status_code=400, detail="Uploaded file has no name.")

    if hasattr(file, "size") and file.size and file.size > MAX_FILE_SIZE_MB * 1024 * 1024:
        logger.warning(
            "User %s uploaded file '%s' exceeding size limit.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(status_code=400, detail="File too large.")

    if not file.filename.endswith((".csv", ".xlsx")):
        logger.warning(
            "User %s uploaded unsupported file type '%s'.",
            current_user.id,
            file.filename,
        )
        raise HTTPException(
            status_code=400,
            detail="Please upload a .csv or .xlsx file exported from your system.",
        )

    logger.info(
        "User %s started dashboard generation for file '%s'.",
        current_user.id,
        file.filename,
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

    mapping = {
        "date": resolved_date_col,
        "amount": amount_col or detected_amount,
        "product": product_col or (results.get("product") or {}).get("column"),
        "customer": customer_col or (results.get("customer") or {}).get("column"),
        "filter_start_date": filter_start_date or "",
        "filter_end_date": filter_end_date or "",
        "filter_product": filter_product.split("|") if filter_product else [],
        "filter_customer": filter_customer.split("|") if filter_customer else [],
        "currency_symbol": currency_symbol,
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
        currency_symbol=currency_symbol or "£",
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




@app.get("/dashboard/{dashboard_id}/pdf")
def download_dashboard_pdf(
    dashboard_id: int,
    lang: str = "en",
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Generate a one-page PDF that visually matches the v15 marketing design."""
    # Ensure the dashboard exists and belongs to the current user
    d = (
        db.query(Dashboard)
        .filter(Dashboard.id == dashboard_id, Dashboard.user_id == current_user.id)
        .first()
    )
    if not d:
        logger.warning(
            "User %s requested PDF for missing dashboard %s.",
            current_user.id,
            dashboard_id,
        )
        raise HTTPException(status_code=404, detail="Dashboard not found.")
    # Basic per-user rate limiting for PDF downloads
    check_pdf_rate_limit(current_user.id)

    try:
        dashboard = json.loads(d.dashboard_json)
        mapping = json.loads(d.mapping_json)
    except Exception:
        logger.exception(
            "User %s: failed to parse stored JSON for dashboard %s.",
            current_user.id,
            dashboard_id,
        )
        raise HTTPException(
            status_code=500,
            detail=(
                "This saved dashboard appears to be corrupted or in an old format. "
                "Please regenerate the report from your data."
            ),
        )

    if not isinstance(dashboard, dict):
        logger.error(
            "User %s: dashboard %s JSON has unexpected structure.",
            current_user.id,
            dashboard_id,
        )
        raise HTTPException(
            status_code=500,
            detail=(
                "This saved dashboard is missing the expected structure. "
                "Please regenerate the report from your data."
            ),
        )

    # Use safe defaults for sections so we never crash PDF generation
    summary = dashboard.get("summary", {}) or {}
    charts = dashboard.get("charts", {}) or {}
    insights = dashboard.get("insights", []) or []
    data_quality = dashboard.get("data_quality_report", {}) or {}

    currency = (mapping.get("currency_symbol") or "£").strip() or "£"
    detection_warnings = mapping.get("detection_warnings") or []

    logger.info(
        "User %s: generating PDF for dashboard %s.",
        current_user.id,
        dashboard_id,
    )

    summary = dashboard.get("summary", {}) or {}
    charts = dashboard.get("charts", {}) or {}
    insights = dashboard.get("insights", []) or []
    data_quality = dashboard.get("data_quality_report", {}) or {}

    # --- currency + detection warnings ---
    currency = (mapping.get("currency_symbol") or "£").strip()
    detection_warnings = mapping.get("detection_warnings") or []

    total_revenue = float(summary.get("total_revenue", 0.0))
    num_sales = int(summary.get("num_sales", 0))
    avg_sale = float(summary.get("average_sale", 0.0))

    has_profit_flag = bool(summary.get("has_profit", False))
    total_profit = float(summary.get("total_profit", 0.0)) if has_profit_flag else 0.0
    margin_pct = float(summary.get("profit_margin_pct", 0.0)) if has_profit_flag else 0.0

    rows_total = int(data_quality.get("rows_total", 0))
    rows_invalid_amount = int(data_quality.get("rows_removed_invalid_amount", 0))
    rows_invalid_date = int(data_quality.get("rows_removed_invalid_date", 0))
    rows_used = int(data_quality.get("rows_used_for_insights", 0))

    # low-margin products count (for warning line)
    try:
        low_margin_count = int(summary.get("num_products_below_20_margin", 0))
    except (TypeError, ValueError):
        low_margin_count = 0

    # optional date range for header line
    date_range = summary.get("date_range") or {}
    date_start_raw = (
        date_range.get("start")
        or date_range.get("from")
        or date_range.get("start_date")
        or None
    )
    date_end_raw = (
        date_range.get("end")
        or date_range.get("to")
        or date_range.get("end_date")
        or None
    )

    from datetime import datetime

    def fmt_date_label(iso_str: str) -> str:
        if not iso_str:
            return ""
        try:
            d = datetime.fromisoformat(iso_str)
            return d.strftime("%d %b %Y")
        except Exception:
            return str(iso_str)

    def fmt_currency(v: float) -> str:
        """
        Format currency according to symbol:
        - £, $  -> 1,234 / 1,234.56
        - €     -> 1.234 / 1.234,56
        - other -> 1,234 / 1,234.56
        """
        if abs(v) >= 100:
            base = f"{int(round(v)):,.0f}"
        else:
            base = f"{v:,.2f}"

        if currency == "€":
            base = base.replace(",", "X").replace(".", ",").replace("X", ".")

        return f"{currency}{base}"

    fd, temp_path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)

    c = canvas.Canvas(temp_path, pagesize=A4)
    width, height = A4

    # Background + main rounded panel
    c.setFillColor(colors.HexColor(BACKGROUND_GREY))
    c.rect(0, 0, width, height, fill=1, stroke=0)

    panel_x, panel_y = 32, 36
    panel_w, panel_h = width - 64, height - 72
    draw_rounded_card(c, panel_x, panel_y, panel_w, panel_h, radius=26, shadow=True)

    # ===== HEADER (TOP SECTION) =====
    title_x = panel_x + 56
    title_y = height - 115

    # branding hook: optional logo in top-right of main panel
    logo_path = (mapping.get("logo_path") or mapping.get("company_logo_path")) or None
    if logo_path:
        try:
            if os.path.exists(logo_path):
                logo_w = 80
                logo_h = 28
                logo_x = panel_x + panel_w - 56 - logo_w
                logo_y = title_y - 4
                c.drawImage(
                    logo_path,
                    logo_x,
                    logo_y,
                    width=logo_w,
                    height=logo_h,
                    preserveAspectRatio=True,
                    mask="auto",
                )
        except Exception:
            logger.exception(
                "Failed to draw logo for dashboard %s using path %s",
                dashboard_id,
                logo_path,
            )

    c.setFillColor(colors.HexColor(TEXT_PRIMARY))
    c.setFont("Helvetica-Bold", 24)
    c.drawString(title_x, title_y, "Sales Insights Report")

    # Subtitle line now used for the period (date range) instead of static text
    subtitle_y = title_y - 18
    meta_text = ""
    if date_start_raw and date_end_raw:
        meta_text = (
            f"Period: {fmt_date_label(date_start_raw)} to {fmt_date_label(date_end_raw)}"
        )
    elif date_start_raw or date_end_raw:
        meta_text = f"Period: {fmt_date_label(date_start_raw or date_end_raw)}"

    if meta_text:
        c.setFont("Helvetica", 10)
        c.setFillColor(colors.HexColor(TEXT_MUTED))
        c.drawString(title_x, subtitle_y, meta_text)
    # if no meta_text, we simply leave this line blank

    # ===== KPI CARDS ROW — LOCKED IN =====
    kpi_h = 96
    gap_under_subtitle = 36  # keeps original vertical layout
    kpi_y = subtitle_y - gap_under_subtitle - kpi_h

    kpi_gap = 18
    num_kpis = 4
    kpi_w = (panel_w - 2 * 56 - (num_kpis - 1) * kpi_gap) / num_kpis
    kpi_x0 = panel_x + 56

    has_profit = has_profit_flag and not pd.isna(total_profit)
    total_profit_str = fmt_currency(total_profit) if has_profit else "—"
    margin_str = f"{margin_pct:.1f}%" if has_profit else "—"

    currency_icon = currency[0] if currency else "£"
    ICON_TEAL = "#0E9F9A"

    kpis = [
        {
            "label": "Total revenue",
            "value": fmt_currency(total_revenue) if total_revenue > 0 else "—",
            "kind": "money",
            "has_data": total_revenue > 0,
        },
        {
            "label": "Total profit",
            "value": total_profit_str,
            "kind": "money",
            "has_data": has_profit,
        },
        {
            "label": "Average sale",
            "value": fmt_currency(avg_sale) if num_sales > 0 else "—",
            "kind": "avg",
            "has_data": num_sales > 0,
        },
        {
            "label": "Profit margin",
            "value": margin_str,
            "kind": "margin",
            "has_data": has_profit,
        },
    ]

    for i, k in enumerate(kpis):
        x = kpi_x0 + i * (kpi_w + kpi_gap)
        draw_rounded_card(c, x, kpi_y, kpi_w, kpi_h, radius=18, shadow=True)

        circle_y = kpi_y + kpi_h - 30
        circle_x = x + kpi_w / 2
        c.setFillColor(colors.HexColor(ICON_TEAL))
        c.circle(circle_x, circle_y, 15, fill=1, stroke=0)

        if k["kind"] == "money":
            c.setFillColor(colors.white)
            c.setFont("Helvetica-Bold", 12)
            c.drawCentredString(circle_x, circle_y - 4, currency_icon)

        elif k["kind"] == "avg":
            doc_w, doc_h = 10, 12
            doc_x = circle_x - doc_w / 2
            doc_y = circle_y - doc_h / 2
            c.setFillColor(colors.white)
            c.roundRect(doc_x, doc_y, doc_w, doc_h, 2, fill=1, stroke=0)
            c.setStrokeColor(colors.HexColor(ICON_TEAL))
            c.setLineWidth(0.7)
            c.line(doc_x + 2, doc_y + doc_h - 4, doc_x + doc_w - 2, doc_y + doc_h - 4)
            c.line(doc_x + 2, doc_y + doc_h - 7, doc_x + doc_w - 2, doc_y + doc_h - 7)

        elif k["kind"] == "margin":
            c.setStrokeColor(colors.white)
            c.setLineWidth(1.4)
            p1_x, p1_y = circle_x - 7, circle_y - 3
            p2_x, p2_y = circle_x - 2, circle_y + 2
            p3_x, p3_y = circle_x + 2, circle_y - 1
            p4_x, p4_y = circle_x + 7, circle_y + 4
            c.line(p1_x, p1_y, p2_x, p2_y)
            c.line(p2_x, p2_y, p3_x, p3_y)
            c.line(p3_x, p3_y, p4_x, p4_y)
            c.line(p4_x, p4_y, p4_x - 3, p4_y)
            c.line(p4_x, p4_y, p4_x, p4_y - 3)

        label_y = kpi_y + kpi_h - 56
        c.setFont("Helvetica", 9)
        c.setFillColor(colors.HexColor(TEXT_MUTED))
        c.drawCentredString(circle_x, label_y, k["label"])

        value_y = kpi_y + 18
        c.setFont("Helvetica-Bold", 18)
        c.setFillColor(colors.HexColor(TEXT_PRIMARY))
        c.drawCentredString(circle_x, value_y, k["value"])

        if not k["has_data"]:
            c.setFont("Helvetica", 7)
            c.setFillColor(colors.HexColor(TEXT_MUTED))
            c.drawCentredString(circle_x, value_y - 14, "Data not available")

    # ===== REVENUE OVER TIME CARD (UNCHANGED, TEAL BARS) =====
    rot = charts.get("revenue_over_time")
    chart_h = 172
    chart_x = panel_x + 56
    chart_y = kpi_y - chart_h - 30
    chart_w = panel_w - 112

    if rot and rot.get("points"):
        draw_rounded_card(c, chart_x, chart_y, chart_w, chart_h, radius=20, shadow=True)

        c.setFont("Helvetica-Bold", 11)
        c.setFillColor(colors.HexColor(TEXT_PRIMARY))
        c.drawString(chart_x + 20, chart_y + chart_h - 30, "Revenue over time")

        grouping = (rot.get("grouping") or "month").lower()
        if grouping not in {"week", "month", "year"}:
            grouping = "month"

        grouping_phrase = {
            "week": "by week",
            "month": "by month",
            "year": "by year",
        }[grouping]

        c.setFont("Helvetica", 8)
        c.setFillColor(colors.HexColor(TEXT_MUTED))
        c.drawString(
            chart_x + 20,
            chart_y + chart_h - 42,
            f"{grouping_phrase} ({currency})",
        )

        points = rot["points"]
        bar_data = [float(p.get("revenue", 0.0)) for p in points]
        raw_labels = [str(p.get("period", "")) for p in points]

        month_map = {
            "01": "Jan",
            "02": "Feb",
            "03": "Mar",
            "04": "Apr",
            "05": "May",
            "06": "Jun",
            "07": "Jul",
            "08": "Aug",
            "09": "Sep",
            "10": "Oct",
            "11": "Nov",
            "12": "Dec",
        }

        bar_labels: List[str] = []
        for label in raw_labels:
            if grouping == "month":
                parts = label.split("-")
                if len(parts) >= 2:
                    mm = parts[-1].zfill(2)
                    bar_labels.append(month_map.get(mm, label))
                else:
                    bar_labels.append(label)
            elif grouping == "week":
                if "W" in label:
                    wk = label.split("W")[-1]
                    bar_labels.append(f"W{wk.zfill(2)}")
                else:
                    bar_labels.append(label)
            else:  # year
                bar_labels.append(label)

        draw_bar_chart(
            c,
            x=chart_x,
            y=chart_y + 10,
            w=chart_w,
            h=chart_h - 60,
            data=bar_data,
            labels=bar_labels,
            color_hex=ICON_TEAL,
            value_formatter=lambda v: fmt_currency(v),
        )

    # ===== BOTTOM ROW, HERO INSIGHT PILL, CTA, DATA QUALITY, FOOTER =====

    bottom_card_h = 188
    bottom_gap_x = 22
    bottom_y = panel_y + 112
    bottom_w = (panel_w - 112 - bottom_gap_x) / 2
    left_x = panel_x + 56
    right_x = left_x + bottom_w + bottom_gap_x

    top_products = charts.get("top_products") or []
    top_customers = charts.get("top_customers") or []

    repeat_rev = 0.0
    repeat_customer_count = 0
    for cust in top_customers:
        if int(cust.get("num_purchases", 1)) > 1:
            repeat_customer_count += 1
            repeat_rev += float(cust.get("revenue", 0.0))
    repeat_share = (repeat_rev / total_revenue * 100.0) if total_revenue > 0 else 0.0

    # small helper to wrap text (used in cards and hero pill)
    def draw_wrapped_text(
        canvas_obj,
        text: str,
        x: float,
        y: float,
        max_width: float,
        font_name: str = "Helvetica",
        font_size: int = 8,
        leading: float = 10.0,
        min_y: float | None = None,
    ) -> float:
        """Draw text wrapped so it does not leave the box horizontally."""
        if not text:
            return y
        canvas_obj.setFont(font_name, font_size)
        approx_char_width = font_size * 0.5  # rough but fine for our use
        max_chars = max(8, int(max_width / approx_char_width))
        lines = textwrap.wrap(str(text), width=max_chars)
        for line in lines:
            if min_y is not None and y < min_y:
                break
            canvas_obj.drawString(x, y, line)
            y -= leading
        return y

    # HERO INSIGHT PILL — between chart and bottom cards, wrapped, labelled "Hero Insight"
    if insights:
        hero_raw = str(insights[0]).strip()
        if hero_raw:
            max_len = 160  # allow a bit more text, we'll wrap anyway
            if len(hero_raw) > max_len:
                hero_raw = hero_raw[: max_len - 1] + "…"
            hero_text = f"Hero Insight: {hero_raw}"

            font_name = "Helvetica"
            font_size = 8
            leading = 11
            c.setFont(font_name, font_size)

            # compute available vertical band between chart bottom and top of bottom cards
            top_band_y = chart_y  # bottom of chart card
            bottom_band_y = bottom_y + bottom_card_h  # top of bottom cards

            # set horizontal constraints
            max_pill_width = panel_w - 120
            max_text_width = max_pill_width - 24  # padding inside pill

            # estimate chars per line and wrap
            approx_char_width = font_size * 0.5
            max_chars = max(10, int(max_text_width / approx_char_width))
            lines = textwrap.wrap(hero_text, width=max_chars)
            if lines:
                # pill height based on number of lines
                text_block_height = leading * len(lines)
                vertical_padding = 6
                hero_pill_h = text_block_height + vertical_padding * 2

                # choose pill width from max line width
                max_line_width = max(c.stringWidth(line, font_name, font_size) for line in lines)
                pill_w = min(max_pill_width, max_line_width + 24)
                pill_x = panel_x + (panel_w - pill_w) / 2

                # place pill centered in the band between chart and bottom cards
                band_mid = (top_band_y + bottom_band_y) / 2
                hero_pill_y = band_mid - hero_pill_h / 2

                # draw pill
                c.setFillColor(colors.HexColor("#EEF2FF"))
                c.roundRect(pill_x, hero_pill_y, pill_w, hero_pill_h, 11, fill=1, stroke=0)

                # draw wrapped text inside pill
                c.setFillColor(colors.HexColor(BRAND_BLUE_DARK))
                text_y = hero_pill_y + hero_pill_h - vertical_padding - font_size
                text_x = pill_x + 12
                min_text_y = hero_pill_y + vertical_padding

                for line in lines:
                    if text_y < min_text_y:
                        break
                    c.drawString(text_x, text_y, line)
                    text_y -= leading

    # ----- LEFT CARD: Top products & customers -----
    if top_products:
        draw_rounded_card(
            c, left_x, bottom_y, bottom_w, bottom_card_h, radius=18, shadow=True
        )

        c.setFont("Helvetica-Bold", 11)
        c.setFillColor(colors.HexColor(TEXT_PRIMARY))
        c.drawString(
            left_x + 20, bottom_y + bottom_card_h - 30, "Top products & customers"
        )

        max_rows = 4
        row_y = bottom_y + bottom_card_h - 54
        bar_max_w = bottom_w - 50
        c.setFont("Helvetica", 8)

        shown = 0
        last_bar_bottom_y = None
        for row in top_products:
            if shown >= max_rows:
                break
            name = str(row.get("product", ""))[:24]
            share = float(row.get("share_of_total", 0.0))
            revenue_val = float(row.get("revenue", 0.0))
            bar_w = bar_max_w * max(0.0, min(share / 100.0, 1.0))

            # product name
            c.setFillColor(colors.HexColor(TEXT_PRIMARY))
            c.drawString(left_x + 20, row_y, name)

            # value + percentage on the right
            c.setFillColor(colors.HexColor(TEXT_MUTED))
            revenue_str = fmt_currency(revenue_val)
            share_str = f"{share:.1f}".replace(".", ",")
            c.drawRightString(
                left_x + bottom_w - 14, row_y, f"{revenue_str} · {share_str}%"
            )

            # bar slightly further below the text to avoid overlap
            bar_y = row_y - 10
            c.setFillColor(colors.HexColor("#E5E7EB"))
            c.roundRect(left_x + 20, bar_y, bar_max_w, 6, 3, fill=1, stroke=0)
            c.setFillColor(colors.HexColor(ICON_TEAL))  # teal to match main chart
            c.roundRect(left_x + 20, bar_y, bar_w, 6, 3, fill=1, stroke=0)

            last_bar_bottom_y = bar_y
            row_y -= 24  # spacing between rows

            shown += 1

        # ---- summary text block under the bars, with wrapping ----
        max_text_width = bottom_w - 40
        min_text_y = bottom_y + 22  # safe margin above rounded bottom

        if last_bar_bottom_y is not None:
            text_y = last_bar_bottom_y - 12  # clear separation from last bar
        else:
            text_y = bottom_y + 50

        c.setFillColor(colors.HexColor(TEXT_SECONDARY))

        # Repeat customers line
        if repeat_share > 0:
            repeat_str = f"{repeat_share:.1f}".replace(".", ",")
            repeat_line = f"Repeat customers: {repeat_str}% of revenue"
        else:
            repeat_line = "Repeat customers: no repeat customers in this period"

        text_y = draw_wrapped_text(
            c,
            repeat_line,
            left_x + 20,
            text_y,
            max_text_width,
            font_size=8,
            leading=11,
            min_y=min_text_y,
        )
        text_y -= 2  # small spacer

        # Highest-margin product (currently using top product by revenue share)
        top_prod = top_products[0]
        top_prod_name = str(top_prod.get("product", ""))[:24]
        top_prod_share = float(top_prod.get("share_of_total", 0.0))
        share_str_hp = f"{top_prod_share:.1f}".replace(".", ",")

        hm_line = f"Highest-margin product: {top_prod_name} ({share_str_hp}%)"
        text_y = draw_wrapped_text(
            c,
            hm_line,
            left_x + 20,
            text_y,
            max_text_width,
            font_size=8,
            leading=11,
            min_y=min_text_y,
        )
        text_y -= 4

        # Low-margin warning, only if there are such products
        if low_margin_count > 0 and text_y >= min_text_y:
            if low_margin_count == 1:
                warning_text = "1 product below 20% margin"
            else:
                warning_text = f"{low_margin_count} products below 20% margin"

            approx_char_width = 8 * 0.5
            max_chars_warn = max(8, int((max_text_width - 20) / approx_char_width))
            warn_lines = textwrap.wrap(warning_text, width=max_chars_warn)

            if warn_lines:
                icon_x = left_x + 20
                icon_y = text_y

                c.setFillColor(colors.HexColor("#FDBA74"))
                c.circle(icon_x + 4, icon_y + 4, 4, fill=1, stroke=0)
                c.setFillColor(colors.white)
                c.setFont("Helvetica-Bold", 7)
                c.drawCentredString(icon_x + 4, icon_y + 2.5, "!")

                c.setFont("Helvetica", 8)
                c.setFillColor(colors.HexColor(TEXT_SECONDARY))
                # first line beside the icon
                c.drawString(icon_x + 14, icon_y, warn_lines[0])

                next_y = icon_y - 11
                # remaining lines (if any) aligned with the text, no icon
                for extra in warn_lines[1:]:
                    if next_y < min_text_y:
                        break
                    c.drawString(icon_x + 14, next_y, extra)
                    next_y -= 11

    # ----- RIGHT CARD: Key insights (wrapped, lighter styling) -----
    if insights:
        draw_rounded_card(
            c, right_x, bottom_y, bottom_w, bottom_card_h, radius=18, shadow=True
        )

        c.setFont("Helvetica-Bold", 11)
        c.setFillColor(colors.HexColor(TEXT_PRIMARY))
        c.drawString(right_x + 20, bottom_y + bottom_card_h - 30, "Key insights")

        # body text: slightly smaller font, softer grey
        font_size = 8
        leading = 12
        c.setFont("Helvetica", font_size)
        c.setFillColor(colors.HexColor(TEXT_SECONDARY))

        text_y = bottom_y + bottom_card_h - 54
        max_text_width = bottom_w - 40  # a bit of margin on the right
        min_text_y = bottom_y + 24

        approx_char_width = font_size * 0.5
        max_chars = max(8, int(max_text_width / approx_char_width))

        for raw_line in insights:
            if text_y < min_text_y:
                break

            bullet_text = f"• {raw_line}"
            lines = textwrap.wrap(str(bullet_text), width=max_chars)
            if not lines:
                continue

            # Ensure the whole bullet fits; if not, don't draw a partial bullet
            needed_height = leading * len(lines)
            if text_y - needed_height < min_text_y:
                break

            for line in lines:
                c.drawString(right_x + 28, text_y, line)
                text_y -= leading

            text_y -= 2  # small gap between bullets

    # Tip pill – copy depends on whether we have margin/profit data
    if has_profit:
        pill_text = (
            "Tip: Focus on improving low-margin products and nurturing repeat customers."
        )
    else:
        pill_text = (
            "Tip: Add cost prices to your data to identify low-margin products and "
            "understand profitability."
        )

    c.setFont("Helvetica", 8)
    pill_text_width = c.stringWidth(pill_text, "Helvetica", 8)
    pill_padding = 26
    pill_w = min(panel_w - 120, pill_text_width + pill_padding)
    pill_x = panel_x + (panel_w - pill_w) / 2
    pill_y = panel_y + 70
    pill_h = 22

    c.setFillColor(colors.HexColor("#E5F6FF"))
    c.roundRect(pill_x, pill_y, pill_w, pill_h, 11, fill=1, stroke=0)
    c.setFillColor(colors.HexColor(BRAND_BLUE_DARK))
    c.drawCentredString(pill_x + pill_w / 2, pill_y + 7, pill_text)

    dq_line = (
        f"{rows_total} rows in file · "
        f"{rows_invalid_amount} invalid amounts · "
        f"{rows_invalid_date} invalid dates · "
        f"{rows_used} used for insights"
    )

    lower_warnings = " ".join(w.lower() for w in detection_warnings)
    if "currency" in lower_warnings:
        if "multiple currency symbols" in lower_warnings or "mixed currency" in lower_warnings:
            dq_line += " · mixed currencies detected"
        else:
            dq_line += " · currency issues detected"

    c.setFont("Helvetica", 7)
    c.setFillColor(colors.HexColor(TEXT_MUTED))
    c.drawCentredString(panel_x + panel_w / 2, panel_y + 44, dq_line)

    footer_text = f"Dashboard generated from {d.file_name}"
    c.drawCentredString(panel_x + panel_w / 2, panel_y + 28, footer_text)

    c.showPage()
    c.save()

    return FileResponse(
        temp_path,
        media_type="application/pdf",
        filename=f"dashboard_{dashboard_id}.pdf",
    )
