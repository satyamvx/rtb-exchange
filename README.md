# Mini Real-Time Bidding Ad Exchange

Second-price auction in <100 ms with budget caps, frequency caps, click dedup, an event pipeline and a live dashboard. Zero dependencies (Node 18+).

## Run
    node server.js          # http://localhost:3000 (dashboard)
    node loadtest.js 3000 100

## API
- `POST /bid-request {"user":"u1","category":"sports"}` -> winner, price, latency
- `POST /click {"impressionId":"..."}` -> duplicate clicks rejected
- `GET /stats` -> per-advertiser wins, spend, CTR, p50/p99 latency

## Flow
request -> filter (budget + frequency cap) -> parallel bids (60 ms timeout) -> sort -> second price -> atomic budget reserve -> event published -> aggregated + logged to `events.ndjson`

## Upgrade path (production stack)
| Here | Replace with |
|---|---|
| in-memory `store` | Redis (`INCRBYFLOAT`, `INCR`+`EXPIRE`, Lua script for reserve) |
| EventEmitter topic + NDJSON | Kafka topics (`impressions`, `clicks`) |
| in-memory `analytics` | ClickHouse table fed by a Kafka consumer |
| in-process mock bidders | separate HTTP services |

