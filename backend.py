import os
import json
from typing import Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Puma L1 AI Demo API")

# ----------------------
# DB Connection Helper
# ----------------------
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

# ======================================================
#                    REQUEST MODELS
# ======================================================

class EmailInboxIn(BaseModel):
    message_id: Optional[str] = None
    internet_message_id: Optional[str] = None

    from_name: Optional[str] = None
    from_email: EmailStr
    to_email: EmailStr

    subject: Optional[str] = None
    body_preview: Optional[str] = None
    body_html: Optional[str] = None

    received_at: Optional[str] = None  # ISO timestamp string

    channel: Optional[str] = "email"
    processing_status: Optional[str] = "new"

    linked_case_id: Optional[int] = None
    raw_payload: Optional[Dict[str, Any]] = None


class CaseIn(BaseModel):
    salesforce_case_id: Optional[str] = None
    channel: Optional[str] = None
    intent_type: Optional[str] = None
    confidence_score: Optional[float] = None
    risk_flag: Optional[bool] = False
    status: Optional[str] = None
    assigned_to: Optional[str] = None


class CaseOrderIn(BaseModel):
    case_id: int
    order_id: str
    is_valid: Optional[bool] = True


class OrderStatusSnapshotIn(BaseModel):
    order_id: str
    oms_status: Optional[str] = None
    courier_status: Optional[str] = None
    tracking_id: Optional[str] = None
    delivery_attempts: Optional[int] = 0
    last_movement_at: Optional[str] = None
    source_system: Optional[str] = None


class RefundCaseIn(BaseModel):
    case_id: int
    order_id: str
    refund_type: Optional[str] = None
    refund_status: Optional[str] = None
    arn_number: Optional[str] = None
    sla_start_date: Optional[str] = None


class RefundEventIn(BaseModel):
    refund_case_id: int
    event_type: Optional[str] = None
    event_source: Optional[str] = None
    event_payload: Optional[Dict[str, Any]] = None


class AIDecisionIn(BaseModel):
    case_id: int
    intent_detected: Optional[str] = None
    confidence_score: Optional[float] = None
    decision_type: Optional[str] = None
    reason_code: Optional[str] = None
    model_version: Optional[str] = None


class RiskEventIn(BaseModel):
    case_id: int
    keyword_detected: Optional[str] = None
    risk_level: Optional[str] = None
    action_taken: Optional[str] = None


class ChildCaseIn(BaseModel):
    parent_case_id: int
    type: Optional[str] = None
    assigned_team: Optional[str] = None
    status: Optional[str] = None
    closed_at: Optional[str] = None


class CommunicationIn(BaseModel):
    case_id: int
    channel: Optional[str] = None
    template_id: Optional[str] = None
    message_status: Optional[str] = None
    sent_at: Optional[str] = None


class EmailQueueIn(BaseModel):
    case_id: int
    to_address: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    status: Optional[str] = None
    retry_count: Optional[int] = 0
    sent_at: Optional[str] = None


class EmailTemplateIn(BaseModel):
    template_name: Optional[str] = None
    subject: Optional[str] = None
    body_html: Optional[str] = None
    body_text: Optional[str] = None


class AgentActionIn(BaseModel):
    case_id: int
    agent_id: Optional[str] = None
    action_type: Optional[str] = None
    notes: Optional[str] = None


class CaseReopenHistoryIn(BaseModel):
    case_id: int
    reopen_reason: Optional[str] = None
    reopened_at: Optional[str] = None


class ConfidenceThresholdIn(BaseModel):
    intent_type: Optional[str] = None
    min_confidence_for_fcr: Optional[float] = None


class SystemAuditLogIn(BaseModel):
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    action: Optional[str] = None
    performed_by: Optional[str] = None
    timestamp: Optional[str] = None


class PlatformMetricIn(BaseModel):
    fcr_rate: Optional[float] = None
    escalation_rate: Optional[float] = None
    sla_breach_rate: Optional[float] = None
    reopen_rate: Optional[float] = None
    calculated_at: Optional[str] = None


# ======================================================
#                    HEALTH CHECK
# ======================================================

@app.get("/health")
def health():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    EMAIL INBOX
# ======================================================

@app.post("/email-inbox")
def insert_email(email: EmailInboxIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            INSERT INTO "Puma_L1_AI".email_inbox (
                message_id,
                internet_message_id,
                from_name,
                from_email,
                to_email,
                subject,
                body_preview,
                body_html,
                received_at,
                channel,
                processing_status,
                linked_case_id,
                raw_payload
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING *;
        """

        cur.execute(
            query,
            (
                email.message_id,
                email.internet_message_id,
                email.from_name,
                email.from_email,
                email.to_email,
                email.subject,
                email.body_preview,
                email.body_html,
                email.received_at,
                email.channel,
                email.processing_status,
                email.linked_case_id,
                json.dumps(email.raw_payload) if email.raw_payload else None,
            ),
        )

        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        return {"status": "inserted", "data": row}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/email-inbox")
def get_emails(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".email_inbox
            ORDER BY received_at DESC NULLS LAST
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )

        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/email-inbox/{email_id}")
def get_email(email_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".email_inbox
            WHERE email_id = %s
            """,
            (email_id,),
        )

        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Email not found")
        return row

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    CASES
# ======================================================

@app.post("/cases")
def create_case(payload: CaseIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".cases
            (salesforce_case_id, channel, intent_type, confidence_score, risk_flag, status, assigned_to)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (
                payload.salesforce_case_id,
                payload.channel,
                payload.intent_type,
                payload.confidence_score,
                payload.risk_flag,
                payload.status,
                payload.assigned_to,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/cases")
def get_cases(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".cases
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders")
def get_orders(email: Optional[str] = None, limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        if email:
            cur.execute(
                """
                SELECT * FROM "Puma_L1_AI".orders 
                WHERE email = %s 
                ORDER BY created_at DESC
                """,
                (email,)
            )
        else:
            cur.execute(
                """
                SELECT * FROM "Puma_L1_AI".orders 
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset)
            )
            
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/seed-data")
def seed_data():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # 2. Dummy Data - Comprehensive Test Set
        
        # A. Happy Path Scenarios
        cur.execute("""
            INSERT INTO "Puma_L1_AI".orders (order_id, email, status, items, amount, created_at)
            VALUES 
            ('PUMA-1001', 'soorya@example.com', 'Shipped', 'Nitro Running Shoes', 12999.00, NOW() - INTERVAL '2 DAYS'),
            ('PUMA-1002', 'user_created@example.com', 'Created', 'Puma White Sneakers', 4500.00, NOW() - INTERVAL '4 HOURS'),
            ('PUMA-1003', 'user_packed@example.com', 'Packed', 'Gym Duffel Bag', 1599.00, NOW() - INTERVAL '1 DAY'),
            ('PUMA-1004', 'user_delivered@example.com', 'Delivered', 'Ferrari Race T-Shirt', 2999.00, NOW() - INTERVAL '5 DAYS'),
            ('PUMA-1005', 'user_returned@example.com', 'Returned', 'Yoga Mat', 1299.00, NOW() - INTERVAL '10 DAYS')
            ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status;
        """)

        # B. Edge Cases & Issues
        cur.execute("""
            INSERT INTO "Puma_L1_AI".orders (order_id, email, status, items, amount, created_at)
            VALUES 
            ('PUMA-9001', 'stuck@example.com', 'Shipped', 'Track Pants Black', 2999.00, NOW() - INTERVAL '12 DAYS'), -- Stuck/Delayed
            ('PUMA-9002', 'pradishshivani1729@gmail.com', 'Shipped', 'Puma Suede Classic', 6999.00, NOW() - INTERVAL '3 DAYS'), -- Your Test User
            ('PUMA-9003', 'failed_delivery@example.com', 'Delivery Failed', 'Hoodie Grey', 3499.00, NOW() - INTERVAL '1 DAY')
            ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status;
        """)

        # C. Multiple Orders Scenario (for table view testing)
        cur.execute("""
            INSERT INTO "Puma_L1_AI".orders (order_id, email, status, items, amount, created_at)
            VALUES 
            ('PUMA-2001', 'john.doe@example.com', 'Delivered', 'Puma T-Shirt Black', 1499.00, NOW() - INTERVAL '15 DAYS'),
            ('PUMA-2002', 'john.doe@example.com', 'Shipped', 'Sneakers White', 4999.00, NOW() - INTERVAL '2 DAYS'),
            ('PUMA-2003', 'john.doe@example.com', 'Created', 'Socks 3-Pack', 499.00, NOW() - INTERVAL '1 HOUR')
            ON CONFLICT (order_id) DO NOTHING;
        """)

        conn.commit()
        cur.close()
        conn.close()
        return {"status": "seeded", "message": "Dummy orders created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/{order_id}")
def get_order_by_id(order_id: str):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute(
            """
            SELECT * FROM "Puma_L1_AI".orders 
            WHERE order_id = %s
            """,
            (order_id,)
        )
            
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if not row:
            raise HTTPException(status_code=404, detail="Order not found")
            
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cases/{case_id}")
def get_case(case_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".cases WHERE case_id=%s""", (case_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Case not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    CASE ORDERS
# ======================================================

@app.post("/case-orders")
def create_case_order(payload: CaseOrderIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".case_orders
            (case_id, order_id, is_valid)
            VALUES (%s,%s,%s)
            RETURNING *;
            """,
            (payload.case_id, payload.order_id, payload.is_valid),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/case-orders")
def get_case_orders(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".case_orders
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/case-orders/{id}")
def get_case_order(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".case_orders WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Case order not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    ORDER STATUS SNAPSHOT
# ======================================================

@app.post("/order-status-snapshots")
def create_order_status_snapshot(payload: OrderStatusSnapshotIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".order_status_snapshot
            (order_id, oms_status, courier_status, tracking_id, delivery_attempts, last_movement_at, source_system)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (
                payload.order_id,
                payload.oms_status,
                payload.courier_status,
                payload.tracking_id,
                payload.delivery_attempts,
                payload.last_movement_at,
                payload.source_system,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/order-status-snapshots")
def get_order_status_snapshots(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".order_status_snapshot
            ORDER BY fetched_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/order-status-snapshots/{id}")
def get_order_status_snapshot(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".order_status_snapshot WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Order status snapshot not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    REFUND CASES
# ======================================================

@app.post("/refund-cases")
def create_refund_case(payload: RefundCaseIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".refund_cases
            (case_id, order_id, refund_type, refund_status, arn_number, sla_start_date)
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (
                payload.case_id,
                payload.order_id,
                payload.refund_type,
                payload.refund_status,
                payload.arn_number,
                payload.sla_start_date,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/refund-cases")
def get_refund_cases(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".refund_cases
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/refund-cases/{refund_case_id}")
def get_refund_case(refund_case_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".refund_cases WHERE refund_case_id=%s""", (refund_case_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Refund case not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    REFUND EVENTS
# ======================================================

@app.post("/refund-events")
def create_refund_event(payload: RefundEventIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".refund_events
            (refund_case_id, event_type, event_source, event_payload)
            VALUES (%s,%s,%s,%s)
            RETURNING *;
            """,
            (
                payload.refund_case_id,
                payload.event_type,
                payload.event_source,
                json.dumps(payload.event_payload) if payload.event_payload else None,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/refund-events")
def get_refund_events(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".refund_events
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/refund-events/{id}")
def get_refund_event(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".refund_events WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Refund event not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    AI DECISIONS
# ======================================================

@app.post("/ai-decisions")
def create_ai_decision(payload: AIDecisionIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".ai_decisions
            (case_id, intent_detected, confidence_score, decision_type, reason_code, model_version)
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (
                payload.case_id,
                payload.intent_detected,
                payload.confidence_score,
                payload.decision_type,
                payload.reason_code,
                payload.model_version,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/ai-decisions")
def get_ai_decisions(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".ai_decisions
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ai-decisions/{decision_id}")
def get_ai_decision(decision_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".ai_decisions WHERE decision_id=%s""", (decision_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="AI decision not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    RISK EVENTS
# ======================================================

@app.post("/risk-events")
def create_risk_event(payload: RiskEventIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".risk_events
            (case_id, keyword_detected, risk_level, action_taken)
            VALUES (%s,%s,%s,%s)
            RETURNING *;
            """,
            (payload.case_id, payload.keyword_detected, payload.risk_level, payload.action_taken),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/risk-events")
def get_risk_events(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".risk_events
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/risk-events/{id}")
def get_risk_event(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".risk_events WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Risk event not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    CHILD CASES
# ======================================================

@app.post("/child-cases")
def create_child_case(payload: ChildCaseIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".child_cases
            (parent_case_id, type, assigned_team, status, closed_at)
            VALUES (%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (payload.parent_case_id, payload.type, payload.assigned_team, payload.status, payload.closed_at),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/child-cases")
def get_child_cases(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".child_cases
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/child-cases/{child_case_id}")
def get_child_case(child_case_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".child_cases WHERE child_case_id=%s""", (child_case_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Child case not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    COMMUNICATIONS
# ======================================================

@app.post("/communications")
def create_communication(payload: CommunicationIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".communications
            (case_id, channel, template_id, message_status, sent_at)
            VALUES (%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (payload.case_id, payload.channel, payload.template_id, payload.message_status, payload.sent_at),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/communications")
def get_communications(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".communications
            ORDER BY sent_at DESC NULLS LAST
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/communications/{comm_id}")
def get_communication(comm_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".communications WHERE comm_id=%s""", (comm_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Communication not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    EMAIL QUEUE
# ======================================================

@app.post("/email-queue")
def create_email_queue(payload: EmailQueueIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".email_queue
            (case_id, to_address, payload, status, retry_count, sent_at)
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (
                payload.case_id,
                payload.to_address,
                json.dumps(payload.payload) if payload.payload else None,
                payload.status,
                payload.retry_count,
                payload.sent_at,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/email-queue")
def get_email_queue(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".email_queue
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/email-queue/{email_id}")
def get_email_queue_item(email_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".email_queue WHERE email_id=%s""", (email_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Email queue item not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    EMAIL TEMPLATES
# ======================================================

@app.post("/email-templates")
def create_email_template(payload: EmailTemplateIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".email_templates
            (template_name, subject, body_html, body_text)
            VALUES (%s,%s,%s,%s)
            RETURNING *;
            """,
            (payload.template_name, payload.subject, payload.body_html, payload.body_text),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/email-templates")
def get_email_templates(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".email_templates
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/email-templates/{template_id}")
def get_email_template(template_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".email_templates WHERE template_id=%s""", (template_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Email template not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    AGENT ACTIONS
# ======================================================

@app.post("/agent-actions")
def create_agent_action(payload: AgentActionIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".agent_actions
            (case_id, agent_id, action_type, notes)
            VALUES (%s,%s,%s,%s)
            RETURNING *;
            """,
            (payload.case_id, payload.agent_id, payload.action_type, payload.notes),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/agent-actions")
def get_agent_actions(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".agent_actions
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/agent-actions/{id}")
def get_agent_action(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".agent_actions WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Agent action not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    CASE REOPEN HISTORY
# ======================================================

@app.post("/case-reopen-history")
def create_case_reopen_history(payload: CaseReopenHistoryIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".case_reopen_history
            (case_id, reopen_reason, reopened_at)
            VALUES (%s,%s,%s)
            RETURNING *;
            """,
            (payload.case_id, payload.reopen_reason, payload.reopened_at),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/case-reopen-history")
def get_case_reopen_history(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".case_reopen_history
            ORDER BY reopened_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/case-reopen-history/{id}")
def get_case_reopen_item(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".case_reopen_history WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Reopen history not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    CONFIDENCE THRESHOLDS
# ======================================================

@app.post("/confidence-thresholds")
def create_confidence_threshold(payload: ConfidenceThresholdIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".confidence_thresholds
            (intent_type, min_confidence_for_fcr)
            VALUES (%s,%s)
            RETURNING *;
            """,
            (payload.intent_type, payload.min_confidence_for_fcr),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/confidence-thresholds")
def get_confidence_thresholds(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".confidence_thresholds
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/confidence-thresholds/{id}")
def get_confidence_threshold(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".confidence_thresholds WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Confidence threshold not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    SYSTEM AUDIT LOGS
# ======================================================

@app.post("/system-audit-logs")
def create_system_audit_log(payload: SystemAuditLogIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".system_audit_logs
            (entity_type, entity_id, action, performed_by, timestamp)
            VALUES (%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (payload.entity_type, payload.entity_id, payload.action, payload.performed_by, payload.timestamp),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/system-audit-logs")
def get_system_audit_logs(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".system_audit_logs
            ORDER BY timestamp DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/system-audit-logs/{id}")
def get_system_audit_log(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".system_audit_logs WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="System audit log not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================
#                    PLATFORM METRICS
# ======================================================

@app.post("/platform-metrics")
def create_platform_metric(payload: PlatformMetricIn):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            INSERT INTO "Puma_L1_AI".platform_metrics
            (fcr_rate, escalation_rate, sla_breach_rate, reopen_rate, calculated_at)
            VALUES (%s,%s,%s,%s,%s)
            RETURNING *;
            """,
            (
                payload.fcr_rate,
                payload.escalation_rate,
                payload.sla_breach_rate,
                payload.reopen_rate,
                payload.calculated_at,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "inserted", "data": row}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {e}")


@app.get("/platform-metrics")
def get_platform_metrics(limit: int = 20, offset: int = 0):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT *
            FROM "Puma_L1_AI".platform_metrics
            ORDER BY calculated_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/platform-metrics/{id}")
def get_platform_metric(id: int):
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""SELECT * FROM "Puma_L1_AI".platform_metrics WHERE id=%s""", (id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Platform metric not found")
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))