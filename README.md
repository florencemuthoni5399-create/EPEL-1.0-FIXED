# SynthEPEL-R75 v2.1 — Render WebSocket Diagnostics

This release is a research-only R_75 tick bot. It does **not** place contracts.

## What changed from v2.0
- Uses Deriv's documented secure WebSocket endpoint with `app_id`.
- Adds connection/open timeouts.
- Reports WebSocket HTTP upgrade failures including status code, headers and response body when available.
- Logs socket error, close code/reason, endpoint and reconnect attempt.
- Uses exponential reconnect backoff (3s → 60s max).
- Sends periodic Deriv `ping` messages to keep the connection alive.
- After connection, sends `time`, `active_symbols` and a public `ticks` subscription so logs distinguish transport, API and symbol problems.
- Confirms receipt of R_75 ticks before declaring the data stream healthy.
- Avoids overlapping reconnect attempts.

## Important
A Deriv token is **not required** for public tick streaming. Do not put your Deriv password in Render. If you later add account-scoped trading, use a properly scoped token/OTP flow separately.

Deriv's current documentation describes `wss://ws.derivws.com/websockets/v3?app_id={app_id}` as the WebSocket endpoint and its tick stream as a no-auth market-data subscription. See the official Deriv API documentation.

## Render
Create a Web Service, set the root directory to `synthtrade-server` if required by your deployment layout, and use:

```text
npm install
npm start
```

Set the variables from `.env.example`. Do not paste secrets into source code or commit them.

## What a healthy log looks like

```text
[WS] connecting ...
[WS] open
[API] pong received
[API] time OK
[API] active_symbols OK
[API] ticks subscription accepted
[STREAM] first R_75 tick received ...
[HEALTH] R_75 tick stream healthy
```

If Render still reports `Unexpected server response: 520`, v2.1 will show the HTTP status/headers/body during the WebSocket upgrade. That tells us whether the failure is occurring at Render's network/proxy layer or at Deriv's endpoint, instead of guessing.
