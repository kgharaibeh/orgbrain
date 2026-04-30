"""
Agent Router — fully embedded LangGraph agent.
Queries Neo4j + TimescaleDB + Qdrant + Ollama directly from cp-backend.
No separate agent service required.
"""

import json
import logging
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, AsyncGenerator, TypedDict

import psycopg2
import psycopg2.extras
import requests
import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langchain_ollama import ChatOllama
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from neo4j import GraphDatabase
from pydantic import BaseModel
from qdrant_client import QdrantClient

log = logging.getLogger(__name__)
router = APIRouter()

# ── Config ─────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",           "bolt://neo4j:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",          "neo4j")
NEO4J_PASS     = os.getenv("NEO4J_PASSWORD",      "orgbrain_neo4j")
TIMESCALE_HOST = os.getenv("TIMESCALE_HOST",      "timescale")
TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT",  "5432"))
TIMESCALE_DB   = os.getenv("TIMESCALE_DB",        "orgbrain_metrics")
TIMESCALE_USER = os.getenv("TIMESCALE_USER",      "orgbrain")
TIMESCALE_PASS = os.getenv("TIMESCALE_PASSWORD",  "orgbrain_secret")
QDRANT_HOST    = os.getenv("QDRANT_HOST",         "qdrant")
QDRANT_PORT    = int(os.getenv("QDRANT_PORT",     "6333"))
OLLAMA_HOST    = os.getenv("OLLAMA_HOST",         "http://ollama:11434")
OLLAMA_MODEL   = os.getenv("OLLAMA_MODEL",        "llama3.1:8b")

_executor = ThreadPoolExecutor(max_workers=4)

# ── Lazy-initialised clients ───────────────────────────────────────────────────
_neo4j_driver = None
_qdrant_client = None


def _get_neo4j():
    global _neo4j_driver
    if _neo4j_driver is None:
        _neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    return _neo4j_driver


def _get_qdrant():
    global _qdrant_client
    if _qdrant_client is None:
        _qdrant_client = QdrantClient(
            host=QDRANT_HOST, port=QDRANT_PORT, check_compatibility=False
        )
    return _qdrant_client


def _ts_conn():
    return psycopg2.connect(
        host=TIMESCALE_HOST, port=TIMESCALE_PORT, dbname=TIMESCALE_DB,
        user=TIMESCALE_USER, password=TIMESCALE_PASS,
    )


def _ollama_embed(text: str) -> list:
    resp = requests.post(
        f"{OLLAMA_HOST}/api/embeddings",
        json={"model": "nomic-embed-text", "prompt": text},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["embedding"]


# ── Agent Tools ────────────────────────────────────────────────────────────────

@tool
def graph_query(cypher: str) -> str:
    """
    Execute a read-only Cypher query on the knowledge graph (Neo4j).

    Node labels   : Customer, Account, Transaction, Card, Loan, Merchant
    Relationships : (:Customer)-[:HAS_TRANSACTION]->(:Transaction)
                    (:Customer)-[:HAS_CARD]->(:Card)
                    (:Customer)-[:HAS_LOAN]->(:Loan)

    Key node properties (use these in WHERE/RETURN):
      Customer   : entity_id, customer_id, full_name, gender, nationality,
                   income_band, occupation, risk_rating, city, email, phone_number
      Account    : entity_id, account_id, customer_id, account_type, currency,
                   balance, available_balance, status, opened_date
      Transaction: entity_id, tx_id, customer_id, account_id, tx_type, amount,
                   currency, channel, merchant_name, merchant_mcc, status, value_date
      Card       : entity_id, card_id, customer_id, card_type, card_network, status
      Loan       : entity_id, loan_id, customer_id, loan_type, principal,
                   outstanding, interest_rate, status, dpd
      Merchant   : entity_id, merchant_id, merchant_name, city, mcc

    Examples:
      MATCH (c:Customer) WHERE c.risk_rating = 'HIGH' RETURN c.entity_id, c.income_band LIMIT 10
      MATCH (c:Customer)-[:HAS_LOAN]->(l:Loan) WHERE l.dpd > 0 RETURN c.entity_id, l.dpd ORDER BY l.dpd DESC LIMIT 10
      MATCH (c:Customer)-[:HAS_TRANSACTION]->(t:Transaction) WHERE t.amount > 5000 RETURN c.entity_id, count(t) AS big_txns ORDER BY big_txns DESC LIMIT 10
      MATCH (c:Customer) RETURN c.income_band, count(*) AS cnt ORDER BY cnt DESC
    """
    try:
        with _get_neo4j().session() as session:
            result = session.run(cypher)
            rows = [dict(record) for record in result]
            return json.dumps(rows[:100], default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def events_query(sql: str) -> str:
    """
    Execute a read-only SQL query on the brain event store (TimescaleDB).

    Table: brain_events
    Columns:
      time        (timestamptz) — when the event was recorded
      entity_type (text)        — 'Customer', 'Transaction', 'Account', 'Card', 'Loan', 'Merchant'
      entity_id   (text)        — e.g. 'CUS000001', 'TXN000000001', 'ACC00000001'
      event_type  (text)        — lowercase entity type slug
      payload     (jsonb)       — full entity data as JSON

    Useful payload fields by entity_type:
      Customer   : full_name, gender, income_band, risk_rating, city, nationality
      Transaction: tx_type, amount, currency, channel, merchant_name, status, value_date
      Account    : account_type, balance, available_balance, status, currency
      Loan       : loan_type, principal, outstanding, interest_rate, status, dpd
      Card       : card_type, card_network, status
      Merchant   : merchant_name, city, mcc

    Examples:
      SELECT payload->>'full_name', payload->>'income_band', payload->>'risk_rating'
      FROM brain_events WHERE entity_type = 'Customer' LIMIT 10

      SELECT payload->>'merchant_name' AS merchant,
             count(*) AS txn_count,
             round(sum((payload->>'amount')::numeric)) AS total_aed
      FROM brain_events WHERE entity_type = 'Transaction'
      GROUP BY 1 ORDER BY 2 DESC LIMIT 10

      SELECT payload->>'tx_type', count(*), avg((payload->>'amount')::numeric)
      FROM brain_events WHERE entity_type = 'Transaction'
      GROUP BY 1 ORDER BY 2 DESC
    """
    conn = None
    try:
        conn = _ts_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = [dict(r) for r in cur.fetchmany(200)]
            return json.dumps(rows, default=str)
    except Exception as e:
        if conn:
            conn.rollback()
        return json.dumps({"error": str(e)})
    finally:
        if conn:
            conn.close()


@tool
def semantic_search(query: str, entity_type: str = "any", limit: int = 10) -> str:
    """
    Semantic similarity search across the vector knowledge base (Qdrant).
    Use for natural language questions about behaviour, characteristics, or patterns.

    entity_type options:
      'Transaction'            — searches event_vectors (400 transaction records)
      'Customer','Account',
      'Loan','Card','Merchant' — searches entity_vectors (207 records)
      'any'                    — searches entity_vectors (default)

    Examples:
      query="customers with high disposable income", entity_type="Customer"
      query="large cash withdrawals at ATMs", entity_type="Transaction"
      query="luxury retail and hospitality merchants", entity_type="Merchant"
    """
    try:
        collection = "event_vectors" if entity_type == "Transaction" else "entity_vectors"
        vector = _ollama_embed(query)
        results = _get_qdrant().search(
            collection_name=collection,
            query_vector=vector,
            limit=limit * 2,   # fetch extra then filter
            with_payload=True,
        )
        output = []
        for r in results:
            payload = r.payload or {}
            if entity_type not in ("any", "Transaction") and payload.get("entity_type") != entity_type:
                continue
            row = {
                "score":       round(r.score, 3),
                "entity_id":   payload.get("entity_id"),
                "entity_type": payload.get("entity_type"),
            }
            # Include a few readable fields
            for field in ("full_name", "merchant_name", "tx_type", "amount",
                          "income_band", "risk_rating", "channel", "status"):
                if field in payload:
                    row[field] = payload[field]
            output.append(row)
            if len(output) >= limit:
                break
        return json.dumps(output, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_entity_details(entity_id: str) -> str:
    """
    Retrieve complete details for a specific entity from all brain stores.
    Works for any entity type — Customer, Account, Transaction, Card, Loan, Merchant.

    Returns:
      - All node properties from Neo4j
      - All outgoing relationships (type + target entity_id + target type)
      - The full payload from the most recent brain_events record

    Examples: entity_id="CUS000001"  entity_id="TXN000000001"  entity_id="ACC00000001"
    """
    result: dict = {}

    # Neo4j: properties + relationships
    try:
        with _get_neo4j().session() as session:
            rec = session.run(
                "MATCH (n {entity_id: $eid}) "
                "OPTIONAL MATCH (n)-[rel]->(nb) "
                "RETURN properties(n) AS props, "
                "       collect({rel: type(rel), target: nb.entity_id, "
                "                target_type: labels(nb)[0]}) AS rels",
                eid=entity_id,
            ).single()
            if rec:
                result["properties"]    = dict(rec["props"])
                result["relationships"] = [r for r in rec["rels"] if r.get("target")]
    except Exception as e:
        result["graph_error"] = str(e)

    # TimescaleDB: latest event payload
    conn = None
    try:
        conn = _ts_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT entity_type, event_type, time, payload "
                "FROM brain_events WHERE entity_id = %s ORDER BY time DESC LIMIT 1",
                (entity_id,),
            )
            row = cur.fetchone()
            if row:
                result["latest_event"] = dict(row)
    except Exception as e:
        result["timeseries_error"] = str(e)
    finally:
        if conn:
            conn.close()

    if not result.get("properties") and not result.get("latest_event"):
        return json.dumps({"error": f"Entity '{entity_id}' not found in brain stores"})
    return json.dumps(result, default=str)


TOOLS   = [graph_query, events_query, semantic_search, get_entity_details]
TOOL_MAP = {t.name: t for t in TOOLS}

# ── LangGraph Agent ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are OrgBrain — an intelligent knowledge assistant for an organizational data platform.
You have real-time access to a knowledge graph and event store populated with entity data.

Available node types: Customer, Account, Transaction, Card, Loan, Merchant
Key relationships:
  (Customer)-[:HAS_TRANSACTION]->(Transaction)
  (Customer)-[:HAS_CARD]->(Card)
  (Customer)-[:HAS_LOAN]->(Loan)

Your tools:
  graph_query       — Cypher queries on Neo4j (relationships, entity properties, counts)
  events_query      — SQL on TimescaleDB brain_events (transaction patterns, aggregations)
  semantic_search   — Vector similarity search on Qdrant (natural language entity discovery)
  get_entity_details — Full profile of a specific entity across all stores

Rules:
  - ALWAYS use tools to retrieve actual data — never invent numbers or entity IDs
  - Query the data first, then interpret the results in your response
  - For aggregations (counts, totals, averages), use events_query SQL
  - For relationship traversals, use graph_query Cypher
  - For "find entities like..." questions, use semantic_search
  - Cite which tool produced each finding
  - If a query returns an error, try a simpler query or explain the limitation
  - Be concise and data-driven. Structure answers as: Finding → Evidence → Insight"""


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]


_compiled_agent = None


def _get_agent():
    global _compiled_agent
    if _compiled_agent is None:
        llm = ChatOllama(
            model=OLLAMA_MODEL,
            base_url=OLLAMA_HOST,
            temperature=0.1,
        ).bind_tools(TOOLS)

        def call_model(state: AgentState) -> AgentState:
            messages = [SystemMessage(content=SYSTEM_PROMPT)] + state["messages"]
            return {"messages": [llm.invoke(messages)]}

        def call_tools(state: AgentState) -> AgentState:
            last = state["messages"][-1]
            tool_msgs = []
            for tc in last.tool_calls:
                fn = TOOL_MAP.get(tc["name"])
                res = fn.invoke(tc["args"]) if fn else json.dumps({"error": f"Unknown tool: {tc['name']}"})
                tool_msgs.append(ToolMessage(content=str(res), tool_call_id=tc["id"]))
            return {"messages": tool_msgs}

        def should_continue(state: AgentState) -> str:
            last = state["messages"][-1]
            return "tools" if getattr(last, "tool_calls", None) else END

        g = StateGraph(AgentState)
        g.add_node("agent", call_model)
        g.add_node("tools", call_tools)
        g.set_entry_point("agent")
        g.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
        g.add_edge("tools", "agent")
        _compiled_agent = g.compile()
        log.info("LangGraph agent compiled successfully")

    return _compiled_agent


def _run_agent(question: str) -> dict:
    """Run the agent synchronously and return {answer, tool_calls_made}."""
    agent = _get_agent()
    result = agent.invoke({"messages": [HumanMessage(content=question)]})
    messages = result["messages"]

    tool_calls_made = []
    final_answer = ""
    for msg in messages:
        if hasattr(msg, "tool_calls"):
            for tc in msg.tool_calls:
                tool_calls_made.append(tc["name"])
        if isinstance(msg, AIMessage) and msg.content:
            final_answer = msg.content

    return {"answer": final_answer, "tool_calls_made": tool_calls_made}


# ── Request / Response models ──────────────────────────────────────────────────

class ChatMessage(BaseModel):
    question: str
    session_id: str = "default"


EXAMPLE_QUESTIONS = [
    "How many customers are in the platform and what income bands do they fall into?",
    "Which customers have loans with overdue payments (dpd > 0)?",
    "What are the top merchants by transaction volume?",
    "Show me the distribution of transaction types and their average amounts",
    "Which customers have the highest account balances?",
    "Find customers with both a loan and a credit card",
    "What is the total transaction value in the dataset?",
    "Which merchants are associated with the most transactions?",
]


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/examples")
def example_questions():
    return {"examples": EXAMPLE_QUESTIONS}


@router.post("/chat")
async def chat(msg: ChatMessage):
    """
    Run the full LangGraph agent (graph_query + events_query + semantic_search + entity_details).
    Returns the final answer + list of tools used.
    """
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(_executor, _run_agent, msg.question)
        return result
    except Exception as e:
        log.error("Agent error: %s", e, exc_info=True)
        raise HTTPException(500, f"Agent error: {e}")


@router.post("/chat/stream")
async def chat_stream(msg: ChatMessage):
    """
    Stream the agent response as Server-Sent Events.
    Emits tool-call progress events, then streams the final answer token by token.
    """
    async def generate() -> AsyncGenerator[str, None]:
        # Step 1: run the agent (tool calls included) in a thread
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(_executor, _run_agent, msg.question)
        except Exception as e:
            yield f"data: {json.dumps({'token': f'Agent error: {e}', 'done': True})}\n\n"
            return

        tools_used = result.get("tool_calls_made", [])
        answer     = result.get("answer", "")

        # Emit a tool-summary event
        if tools_used:
            yield f"data: {json.dumps({'tools': tools_used, 'done': False})}\n\n"

        # Stream the answer token-by-token via Ollama generate
        # Build a prompt that asks Ollama to just reproduce / refine the answer
        stream_prompt = (
            f"The following answer was prepared using data from the knowledge graph. "
            f"Present it clearly to the user:\n\n{answer}"
        )
        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                f"{OLLAMA_HOST}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": stream_prompt, "stream": True},
                timeout=120,
            ) as response:
                async for line in response.aiter_lines():
                    if line:
                        try:
                            chunk = json.loads(line)
                            token = chunk.get("response", "")
                            done  = chunk.get("done", False)
                            if token:
                                yield f"data: {json.dumps({'token': token, 'done': False})}\n\n"
                            if done:
                                yield f"data: {json.dumps({'token': '', 'done': True, 'tool_calls': tools_used})}\n\n"
                                return
                        except json.JSONDecodeError:
                            pass

        yield f"data: {json.dumps({'token': '', 'done': True, 'tool_calls': tools_used})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/models")
def available_models():
    """List models available in Ollama."""
    try:
        resp = httpx.get(f"{OLLAMA_HOST}/api/tags", timeout=10)
        models = resp.json().get("models", [])
        return {"models": [{"name": m["name"], "size_gb": round(m.get("size", 0) / 1e9, 2)} for m in models]}
    except Exception as e:
        raise HTTPException(503, f"Ollama unavailable: {e}")


@router.post("/models/pull")
async def pull_model(model_name: str):
    """Pull a new model from Ollama registry (streams progress)."""
    async def stream_pull() -> AsyncGenerator[str, None]:
        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                f"{OLLAMA_HOST}/api/pull",
                json={"name": model_name, "stream": True},
                timeout=600,
            ) as response:
                async for line in response.aiter_lines():
                    if line:
                        yield f"data: {line}\n\n"

    return StreamingResponse(stream_pull(), media_type="text/event-stream")
