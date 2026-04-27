"""
Agent Router — banking insights agent chat interface.
Streams responses via Server-Sent Events for real-time UX.
"""

import json
import logging
import os
from typing import AsyncGenerator
import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

log = logging.getLogger(__name__)
router = APIRouter()

AGENT_URL   = os.getenv("AGENT_URL", "http://localhost:8000")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")

EXAMPLE_QUESTIONS = [
    "Which customers are at risk of churning this month?",
    "What products are underperforming and should be reviewed?",
    "Which customer segments should we target for a credit card upsell?",
    "Summarize the bank's transaction activity for the last 30 days",
    "Which merchants have the highest transaction volume?",
    "Show customers with loans that are past due",
    "What is the most popular channel for transactions?",
    "Which customers joined in the last 90 days and are highly engaged?",
]


class ChatMessage(BaseModel):
    question: str
    session_id: str = "default"


class DirectQuery(BaseModel):
    prompt: str
    model: str = "llama3.1:8b"
    stream: bool = True


@router.get("/examples")
def example_questions():
    """Return example banking questions to inspire users."""
    return {"examples": EXAMPLE_QUESTIONS}


@router.post("/chat")
def chat(msg: ChatMessage):
    """
    Forward a question to the OrgBrain banking agent and return the response.
    The agent has access to Neo4j, TimescaleDB, Qdrant, and Vault tools.
    """
    try:
        resp = httpx.post(
            f"{AGENT_URL}/query",
            json={"question": msg.question, "session_id": msg.session_id},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.RequestError as e:
        raise HTTPException(503, f"Agent service unavailable: {e}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, e.response.text)


@router.post("/chat/stream")
async def chat_stream(msg: ChatMessage):
    """
    Stream a response from Ollama directly for real-time typing effect.
    Used when the agent backend is not running (direct LLM mode).
    """
    async def generate() -> AsyncGenerator[str, None]:
        async with httpx.AsyncClient() as client:
            async with client.stream(
                "POST",
                f"{OLLAMA_HOST}/api/generate",
                json={
                    "model": "llama3.1:8b",
                    "prompt": f"""You are OrgBrain, an intelligent assistant for a retail bank.
Answer the following question concisely and with data-driven reasoning.
If you need specific data, explain what queries you would run.

Question: {msg.question}""",
                    "stream": True,
                },
                timeout=120,
            ) as response:
                async for line in response.aiter_lines():
                    if line:
                        try:
                            chunk = json.loads(line)
                            token = chunk.get("response", "")
                            if token:
                                yield f"data: {json.dumps({'token': token, 'done': chunk.get('done', False)})}\n\n"
                        except json.JSONDecodeError:
                            pass

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
