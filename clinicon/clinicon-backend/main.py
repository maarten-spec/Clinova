import os
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

from text_parser import parse_command
from apply_actions import apply_action
from clinicon_ai import parse_command_with_ai

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="CliniCon Stellenplan-Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_conn():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL fehlt (siehe .env).")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        ensure_audit_table(conn)
        return conn
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Datenbankverbindung fehlgeschlagen: {exc}") from exc


class CommandRequest(BaseModel):
    command: str
    table: str  # z. B. "stellenplan_employees_gfodin"
    year: Optional[int] = None  # z. B. 2026
    site: Optional[str] = None  # Mandant / Standort


class AiCommandRequest(BaseModel):
    command: str


def ensure_audit_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS assistant_audit (
              id bigserial PRIMARY KEY,
              created_at timestamptz DEFAULT now(),
              site text NOT NULL,
              command text NOT NULL,
              action text,
              target_table text,
              plan_year int,
              status text DEFAULT 'ok',
              result jsonb
            );
            """
        )
    conn.commit()


@app.post("/api/command")
def api_command(req: CommandRequest, conn=Depends(get_conn)):
    parsed = parse_command(req.command)
    if not parsed:
        raise HTTPException(status_code=400, detail="Befehl konnte nicht erkannt werden.")

    try:
        result = apply_action(conn, req.table, parsed, year=req.year)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO assistant_audit(site, command, action, target_table, plan_year, status, result)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    req.site or "unknown",
                    req.command,
                    parsed.get("action"),
                    req.table,
                    req.year,
                    "ok",
                    psycopg2.extras.Json(result) if result else None,
                ),
            )
        conn.commit()
    except Exception as exc:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO assistant_audit(site, command, action, target_table, plan_year, status, result)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        req.site or "unknown",
                        req.command,
                        parsed.get("action"),
                        req.table,
                        req.year,
                        "error",
                        psycopg2.extras.Json({"error": str(exc)}),
                    ),
                )
            conn.commit()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"parsed": parsed, "applied": result}


@app.post("/api/ai-command")
def api_ai_command(req: AiCommandRequest):
    try:
        parsed = parse_command_with_ai(req.command)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"parsed": parsed}


@app.get("/api/audit")
def api_audit(site: str = "unknown", limit: int = 20, conn=Depends(get_conn)):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, created_at, site, command, action, target_table, plan_year, status, result
            FROM assistant_audit
            WHERE site = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (site, limit),
        )
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        data = [dict(zip(cols, r)) for r in rows]
    return {"audit": data}


@app.get("/health")
def health():
    return {"status": "ok"}


# Hinweis: Start im Terminal
# uvicorn main:app --reload
