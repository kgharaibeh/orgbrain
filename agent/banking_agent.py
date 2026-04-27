"""
OrgBrain Banking Insights Agent — LangGraph
Multi-tool agent that queries the brain (graph + vector + timeseries + lake)
to answer banking intelligence questions.
"""

import json
import logging
import os
from typing import Annotated, TypedDict

import psycopg2
import requests
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langchain_ollama import ChatOllama
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from neo4j import GraphDatabase
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ─────────────────────────────────────────────────────────────────────────────
# Tool clients (initialized once at module load for reuse across requests)
# ─────────────────────────────────────────────────────────────────────────────

_neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "orgbrain_neo4j")),
)

_timescale_conn = psycopg2.connect(
    host=os.getenv("TIMESCALE_HOST", "timescale"),
    port=int(os.getenv("TIMESCALE_PORT", "5432")),
    dbname=os.getenv("TIMESCALE_DB", "orgbrain_metrics"),
    user=os.getenv("TIMESCALE_USER", "orgbrain"),
    password=os.getenv("TIMESCALE_PASSWORD", "orgbrain_secret"),
)

_qdrant_client = QdrantClient(
    host=os.getenv("QDRANT_HOST", "qdrant"),
    port=int(os.getenv("QDRANT_PORT", "6333")),
)

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")


def _ollama_embed(text: str) -> list[float]:
    resp = requests.post(
        f"{OLLAMA_HOST}/api/embeddings",
        json={"model": "nomic-embed-text", "prompt": text},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["embedding"]


# ─────────────────────────────────────────────────────────────────────────────
# Agent Tools
# ─────────────────────────────────────────────────────────────────────────────

@tool
def graph_query(cypher: str) -> str:
    """
    Execute a read-only Cypher query against the banking ontology graph (Neo4j).
    Use for entity relationship queries: customer holdings, product subscriptions,
    merchant relationships, segment memberships, loan linkages.

    Example:
      MATCH (c:Customer)-[:HOLDS]->(a:Account)
      WHERE a.account_type = 'CREDIT'
      RETURN c.id, a.balance LIMIT 10
    """
    try:
        with _neo4j_driver.session() as session:
            result = session.run(cypher)
            rows = [dict(record) for record in result]
            return json.dumps(rows[:100], default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def trend_query(sql: str) -> str:
    """
    Execute a read-only SQL query against TimescaleDB (banking trends and metrics).
    Available tables: tx_events, customer_spend_hourly, customer_spend_daily,
    customer_risk_signals, product_adoption, merchant_activity.
    Available views: dormant_customers_30d, churn_risk_customers.

    Example:
      SELECT customer_id, MAX(churn_score) as score
      FROM customer_risk_signals
      WHERE time > NOW() - INTERVAL '7 days'
      GROUP BY customer_id
      ORDER BY score DESC LIMIT 20
    """
    try:
        with _timescale_conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchmany(200)]
            return json.dumps(rows, default=str)
    except Exception as e:
        _timescale_conn.rollback()
        return json.dumps({"error": str(e)})


@tool
def semantic_search(query: str, collection: str = "banking_events", limit: int = 10) -> str:
    """
    Semantic similarity search against Qdrant vector store.
    Use for natural language questions about customer behavior, transaction patterns,
    product themes, or merchant intelligence.

    Collections available:
    - banking_events: individual transaction/event narratives
    - customer_profiles: summarized customer behavioral profiles
    - merchant_intel: merchant spending pattern summaries
    - product_insights: product usage and adoption narratives

    Example query: "customers reducing spending on luxury items"
    """
    try:
        vector = _ollama_embed(query)
        results = _qdrant_client.search(
            collection_name=collection,
            query_vector=vector,
            limit=limit,
            with_payload=True,
        )
        output = [
            {
                "score": r.score,
                "narration": r.payload.get("narration"),
                "customer_id": r.payload.get("customer_id"),
                "event_type": r.payload.get("event_type"),
                "amount": r.payload.get("amount"),
                "merchant_mcc": r.payload.get("merchant_mcc"),
            }
            for r in results
        ]
        return json.dumps(output, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_customer_360(customer_id: str) -> str:
    """
    Retrieve a full 360-degree view of a customer from all brain stores.
    Returns: graph relationships (products, loans, cards),
    recent transaction trends, and risk signals.
    Use this before making any recommendation about a specific customer.
    """
    result = {}

    # Graph: entity relationships
    with _neo4j_driver.session() as session:
        r = session.run("""
            MATCH (c:Customer {id: $cid})
            OPTIONAL MATCH (c)-[:HOLDS]->(a:Account)
            OPTIONAL MATCH (c)-[:HAS_LOAN]->(l:Loan)
            OPTIONAL MATCH (c)-[:USES]->(card:Card)
            OPTIONAL MATCH (c)-[:IN_SEGMENT]->(s:CustomerSegment)
            RETURN
              c,
              collect(DISTINCT {id: a.id, type: a.account_type, status: a.status, balance: a.balance}) AS accounts,
              collect(DISTINCT {id: l.id, type: l.loan_type, status: l.status, dpd: l.dpd}) AS loans,
              collect(DISTINCT {id: card.id, type: card.card_type}) AS cards,
              collect(DISTINCT s.name) AS segments
        """, cid=customer_id)
        for record in r:
            result["graph"] = {
                "customer": dict(record["c"]),
                "accounts": record["accounts"],
                "loans": record["loans"],
                "cards": record["cards"],
                "segments": record["segments"],
            }

    # TimescaleDB: risk signals
    with _timescale_conn.cursor() as cur:
        cur.execute("""
            SELECT churn_score, fraud_score, upsell_score, days_since_last_tx,
                   tx_count_30d, avg_monthly_spend, product_count, loan_dpd, time
            FROM customer_risk_signals
            WHERE customer_id = %s
            ORDER BY time DESC LIMIT 1
        """, (customer_id,))
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            result["risk_signals"] = dict(zip(cols, row))

    return json.dumps(result, default=str)


@tool
def list_churn_risk_customers(min_score: float = 0.6, limit: int = 20) -> str:
    """
    List customers with the highest churn risk scores from the last 48 hours.
    Returns anonymized customer tokens with their scores and inactivity signals.
    Use this to identify retention campaign targets.
    """
    try:
        with _timescale_conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id,
                       MAX(churn_score)         AS churn_score,
                       MIN(days_since_last_tx)  AS days_inactive,
                       MIN(product_count)       AS product_count,
                       MAX(time)                AS scored_at
                FROM customer_risk_signals
                WHERE time > NOW() - INTERVAL '48 hours'
                  AND churn_score >= %s
                GROUP BY customer_id
                ORDER BY churn_score DESC
                LIMIT %s
            """, (min_score, limit))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            return json.dumps(rows, default=str)
    except Exception as e:
        _timescale_conn.rollback()
        return json.dumps({"error": str(e)})


@tool
def product_performance(days_back: int = 30) -> str:
    """
    Retrieve product adoption trends over the specified period.
    Returns active count, new activations, closures, and avg balance per product.
    Use this to identify underperforming products or upsell opportunities.
    """
    try:
        with _timescale_conn.cursor() as cur:
            cur.execute("""
                SELECT product_code,
                       SUM(active_count)    AS total_active,
                       SUM(new_count)       AS total_new,
                       SUM(closed_count)    AS total_closed,
                       AVG(avg_balance)     AS avg_balance,
                       AVG(avg_utilization) AS avg_utilization
                FROM product_adoption
                WHERE time > NOW() - INTERVAL '%s days'
                GROUP BY product_code
                ORDER BY total_active DESC
            """, (days_back,))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            return json.dumps(rows, default=str)
    except Exception as e:
        _timescale_conn.rollback()
        return json.dumps({"error": str(e)})


TOOLS = [
    graph_query,
    trend_query,
    semantic_search,
    get_customer_360,
    list_churn_risk_customers,
    product_performance,
]

TOOL_MAP = {t.name: t for t in TOOLS}


# ─────────────────────────────────────────────────────────────────────────────
# LangGraph Agent
# ─────────────────────────────────────────────────────────────────────────────

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]


SYSTEM_PROMPT = """You are OrgBrain — the intelligence layer of a retail bank.
You have access to the bank's complete anonymized data via four tools:

1. graph_query    — Cypher queries on the banking ontology (customers, accounts, loans, cards, merchants)
2. trend_query    — SQL on TimescaleDB (transaction trends, risk scores, product metrics)
3. semantic_search — Semantic search on Qdrant (behavioral patterns in natural language)
4. get_customer_360 — Full view of a specific customer across all stores
5. list_churn_risk_customers — Pre-computed churn risk rankings
6. product_performance — Product adoption and utilization metrics

All customer identifiers are anonymized tokens — never attempt to deanonymize.
When generating campaign targets, output the anonymized token list; the CRM holds the real mapping.

Be concise, data-driven, and specific. Always cite which tool produced each finding.
Structure recommendations with: Finding → Evidence → Recommended Action."""


def build_agent():
    llm = ChatOllama(
        model=os.getenv("OLLAMA_MODEL", "llama3.1:8b"),
        base_url=OLLAMA_HOST,
        temperature=0.1,
    ).bind_tools(TOOLS)

    def call_model(state: AgentState) -> AgentState:
        messages = [SystemMessage(content=SYSTEM_PROMPT)] + state["messages"]
        response = llm.invoke(messages)
        return {"messages": [response]}

    def call_tools(state: AgentState) -> AgentState:
        last_message = state["messages"][-1]
        tool_messages = []
        for tool_call in last_message.tool_calls:
            tool_fn = TOOL_MAP.get(tool_call["name"])
            if tool_fn is None:
                result = json.dumps({"error": f"Unknown tool: {tool_call['name']}"})
            else:
                result = tool_fn.invoke(tool_call["args"])
            tool_messages.append(
                ToolMessage(content=str(result), tool_call_id=tool_call["id"])
            )
        return {"messages": tool_messages}

    def should_continue(state: AgentState) -> str:
        last = state["messages"][-1]
        if hasattr(last, "tool_calls") and last.tool_calls:
            return "tools"
        return END

    graph = StateGraph(AgentState)
    graph.add_node("agent", call_model)
    graph.add_node("tools", call_tools)
    graph.set_entry_point("agent")
    graph.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
    graph.add_edge("tools", "agent")

    return graph.compile()


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI wrapper for REST access
# ─────────────────────────────────────────────────────────────────────────────

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="OrgBrain Banking Agent", version="0.1.0")
_agent = None


def get_agent():
    global _agent
    if _agent is None:
        _agent = build_agent()
    return _agent


class QueryRequest(BaseModel):
    question: str
    session_id: str = "default"


class QueryResponse(BaseModel):
    answer: str
    tool_calls_made: list[str]


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    agent = get_agent()
    result = agent.invoke({"messages": [HumanMessage(content=req.question)]})
    messages = result["messages"]

    tool_calls_made = []
    final_answer = ""
    for msg in messages:
        if hasattr(msg, "tool_calls"):
            for tc in msg.tool_calls:
                tool_calls_made.append(tc["name"])
        if isinstance(msg, AIMessage) and msg.content:
            final_answer = msg.content

    return QueryResponse(answer=final_answer, tool_calls_made=tool_calls_made)


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
