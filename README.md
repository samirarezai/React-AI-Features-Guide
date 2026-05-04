# React + AI: patterns, streaming, and production readiness

## About this repository

This project is a **documentation-first** reference for **React + AI** features: how to structure apps so secrets stay on the server, how to stream and cancel responses, and what to verify before production.

---

A practical guide for building **React** frontends that talk to **LLM providers** safely: streaming responses, cancellation, observability, and a production checklist.

---

## Table of contents

1. [Principles](#1-principles)
2. [Architecture: keys stay on the server](#2-architecture-keys-stay-on-the-server)
3. [Minimal server route (Node / Express-style)](#3-minimal-server-route-node--express-style)
4. [Streaming from the client (Fetch + `ReadableStream`)](#4-streaming-from-the-client-fetch--readablestream)
5. [Server-Sent Events (SSE) alternative](#5-server-sent-events-sse-alternative)
6. [Cancel streams: Stop button and navigation](#6-cancel-streams-stop-button-and-navigation)
7. [Embeddings and caching](#7-embeddings-and-caching)
8. [Model routing (cheap vs capable)](#8-model-routing-cheap-vs-capable)
9. [Cost control and observability](#9-cost-control-and-observability)
10. [Production checklist](#10-production-checklist)

---

## 1. Principles

| Do | Don’t |
|----|--------|
| Call providers from **backend** routes or server actions | Expose **API keys** in the browser bundle |
| Stream tokens to the UI for long answers | Buffer the full completion in memory on the client before showing anything (unless you have a good reason) |
| Use **`AbortController`** so users can stop generation | Leave long-running requests uncancelled |
| Log **metadata** (model, latency, rough tokens, cost), not raw prompts by default | Log full prompts/responses without a **redaction** policy |

---

## 2. Architecture: keys stay on the server

```text
[Browser: React]  --HTTPS-->  [Your API: Node/Bun/Edge]  --HTTPS-->  [OpenAI / Anthropic / ...]
        |                              |
   no API key                    API key + rate limits
```

The React app sends **user messages** (and optional session IDs) to **your** endpoint. Your server attaches the secret key, enforces auth/rate limits, and optionally logs observability fields.

---

## 3. Minimal server route (Node / Express-style)

Below is a **pattern** only: swap `fetch` to the provider’s SDK and the exact URL/body headers they require.

```js
// server/chat.js: example shape, not tied to a specific provider
import express from "express";

const app = express();
app.use(express.json({ limit: "256kb" }));

app.post("/api/chat", async (req, res) => {
  const started = Date.now();
  const { messages } = req.body;

  // TODO: auth, rate limit, validate `messages`

  res.setHeader("Content-Type", "text/plain; charset=utf-8");
  res.setHeader("Transfer-Encoding", "chunked");
  // If behind nginx/CDN: ensure buffering is off for this location (see checklist)

  const upstream = await fetch("https://api.provider.example/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.LLM_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "gpt-4.1-mini",
      messages,
      stream: true,
    }),
  });

  if (!upstream.ok || !upstream.body) {
    res.status(502).end("upstream_error");
    return;
  }

  const reader = upstream.body.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
  } finally {
    reader.releaseLock?.();
    res.end();
    const ms = Date.now() - started;
    // See §9 for structured logging (model, tokens, cost)
    console.log(JSON.stringify({ route: "/api/chat", latencyMs: ms }));
  }
});

app.listen(3001);
```

Your React app then `fetch`es `/api/chat` with `stream: true` on the response (see next section).

---

## 4. Streaming from the client (Fetch + `ReadableStream`)

Accumulate assistant text as chunks arrive; drive UI from React state.

```tsx
// hooks/useChatStream.ts
import { useCallback, useEffect, useRef, useState } from "react";

type Message = { role: "user" | "assistant"; content: string };

export function useChatStream(apiPath = "/api/chat") {
  const [messages, setMessages] = useState<Message[]>([]);
  const [assistant, setAssistant] = useState("");
  const [loading, setLoading] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const messagesRef = useRef<Message[]>([]);
  messagesRef.current = messages;

  const send = useCallback(
    async (userText: string) => {
      const userMsg: Message = { role: "user", content: userText };
      const history = [...messagesRef.current, userMsg];

      setMessages(history);
      setAssistant("");
      setLoading(true);

      abortRef.current?.abort();
      abortRef.current = new AbortController();
      const { signal } = abortRef.current;

      let fullAssistant = "";

      try {
        const res = await fetch(apiPath, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messages: history.map(({ role, content }) => ({ role, content })),
          }),
          signal,
        });

        if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);

        const reader = res.body.getReader();
        const dec = new TextDecoder();

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          const chunk = dec.decode(value, { stream: true });
          fullAssistant += chunk;
          // Raw text deltas from server; if you use SSE, parse lines here instead.
          setAssistant(fullAssistant);
        }

        if (fullAssistant.trim()) {
          setMessages((m) => [...m, { role: "assistant", content: fullAssistant }]);
        }
        setAssistant("");
      } catch (e: unknown) {
        if ((e as Error).name === "AbortError") return;
        console.error(e);
      } finally {
        setLoading(false);
        abortRef.current = null;
      }
    },
    [apiPath]
  );

  const stop = useCallback(() => abortRef.current?.abort(), []);

  useEffect(() => {
    return () => abortRef.current?.abort();
  }, []);

  return { messages, assistant, loading, send, stop };
}
```

---

## 5. Server-Sent Events (SSE) alternative

SSE is one frame per event and works well with **`EventSource`** for **server → client** one-way streams. For **POST** bodies (typical for chat), use **`fetch` + stream** (§4) or a small POST that returns an SSE stream ID.

Headers that often matter for SSE through proxies:

```http
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
```

Parse `data: ...` lines on the client, or use a library that normalizes provider-specific SSE.

---

## 6. Cancel streams: Stop button and navigation

### Stop button

Wire the `stop` function from `AbortController` to a **Stop** control.

```tsx
function Chat() {
  const { messages, assistant, loading, send, stop } = useChatStream();
  return (
    <>
      <button type="button" disabled={!loading} onClick={stop}>
        Stop
      </button>
      {/* render messages + streaming assistant */}
    </>
  );
}
```

### Navigate away

The `useChatStream` example above uses a **`useEffect` cleanup** that calls `abort()` on unmount so in-flight streams stop when the user navigates away.

If your provider supports **cancelling the upstream** generation (not only closing the HTTP response), call their cancel/disconnect API from the server when the client disconnects. That usually requires passing through **request IDs** from the provider’s streaming API.

---

## 7. Embeddings and caching

Embeddings are **deterministic** for the same input model: ideal for **caching**.

**Server-side cache keys** (example):

```ts
import crypto from "node:crypto";

function embeddingCacheKey(model: string, text: string) {
  return `emb:${model}:${crypto.createHash("sha256").update(text).digest("hex")}`;
}
```

Store results in Redis, your DB, or an LRU in memory (with a max size). Always **cap input length** and **normalize** whitespace to avoid cache fragmentation.

---

## 8. Model routing (cheap vs capable)

Use **small / cheap** models for:

- intent detection, classification, safety triage  
- extracting structured JSON with a tight schema  
- routing (“needs reasoning?” → escalate)

Use **larger** models for:

- multi-step reasoning, long context synthesis, fragile tool use

Pseudo-flow:

```text
user message → classifier (mini) → if hard: reasoning model; else: mini completes
```

Implement routing **on the server** so clients cannot override billing-sensitive choices without authorization.

---

## 9. Cost control and observability

### Minimal viable observability

Per request, log (structured JSON is ideal):

| Field | Why |
|-------|-----|
| **model** | Attribution and pricing lookup |
| **Rough token counts** | Input/output estimates (or provider usage fields when available) |
| **Latency** | SLAs, regressions, timeouts |
| **Estimated cost** | Rough daily totals; compare to budgets |

Example **server-side** log line shape:

```json
{
  "event": "llm_completion",
  "model": "gpt-4.1-mini",
  "inputTokensEst": 420,
  "outputTokensEst": 180,
  "latencyMs": 910,
  "costUsdEst": 0.0012,
  "userId": "anon_or_authed",
  "route": "/api/chat"
}
```

**Redaction:** by default log **hashes or lengths** of prompts, not raw text, unless you have a compliance-reviewed pipeline.

### Cache deterministic calls

Especially **embeddings** (§7) and idempotent **classification** calls with fixed temperature `0`.

### Route models

Small/cheap for classification; larger for hard reasoning (§8).

### Cancel streams

When users **navigate away** or hit **Stop**, abort the client fetch and propagate cancellation to the server where possible (§6).

---

## 10. Production checklist

- [ ] **API keys only on server** (env vars, secret manager; never `NEXT_PUBLIC_*` for provider keys)  
- [ ] **Rate limits** + basic abuse controls (per IP / per user / per org)  
- [ ] **Streaming** works through your **CDN / reverse proxy** (no surprise buffering), e.g. nginx `proxy_buffering off;` (and often `proxy_cache off;`, `gzip off;` for that `location`) on the route that streams chat; verify **chunked** end-to-end with a real client  
- [ ] **Client cancel** aborts upstream generation **where supported** (disconnect handlers, provider cancel APIs)  
- [ ] **Fallback UI** when provider is down (cached copy, graceful message, retry)  
- [ ] **Redacted logging** policy for prompts/responses  
- [ ] **Cost alerts** (daily spend thresholds, anomaly detection on token spikes)

**Example (nginx):** disable buffering for your streaming location so chunks reach the browser promptly.

```nginx
location /api/chat {
  proxy_pass http://backend;
  proxy_http_version 1.1;
  proxy_set_header Connection "";
  proxy_buffering off;
  proxy_cache off;
  gzip off;
}
```

Tune paths and upstream names for your stack; **Cloudflare** and other CDNs have their own streaming/buffering knobs, so test with a slow token stream.

---

Further reading

- [MDN: Server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)  
- [Fetch: consuming a streaming response](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch#processing_a_text_file_line_by_line)  
- [Vercel AI SDK](https://sdk.vercel.ai/docs) (if using Next.js / Vercel ecosystem)  
- Provider docs: **OpenAI** / **Anthropic** / **Google**; always verify current API surfaces and pricing  

