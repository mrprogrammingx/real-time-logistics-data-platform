# Experiment: Kafka consumer-group rebalance

**Goal:** see how `flowfleet.driver.locations` (6 partitions) is shared across a consumer
group as members come and go, and confirm the `CooperativeStickyAssignor` only moves what
it must.

**Setup:** `make up` (generator producing ~200 events/s), then `make rebalance-demo`
(or the steps below).

```bash
docker compose up -d --scale location-consumer=3 --no-recreate location-consumer
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --describe --group flowfleet.location-consumer --members --verbose
```

## What to expect

| Consumers | Partitions each | Notes |
|-----------|-----------------|-------|
| 1 | 6 | one member owns everything |
| 2 | 3 + 3 | |
| 3 | 2 + 2 + 2 | |
| kill 1 of 3 | 3 + 3 | the dead member's 2 partitions split across survivors; survivors keep their existing partitions (sticky) |
| 4 (> partitions) | 2 + 2 + 1 + 1, one idle-ish | you cannot have more *active* consumers than partitions |

Evidence in the consumer logs:

```bash
docker compose logs location-consumer | grep REBALANCE
```

```
REBALANCE + assigned flowfleet.driver.locations-0,flowfleet.driver.locations-1 (now own [0,1])
REBALANCE - revoked  flowfleet.driver.locations-1                 # cooperative: only the moved one
REBALANCE + assigned flowfleet.driver.locations-4                 # picked up a partition from the dead member
```

## Results (fill in from your run)

```
date:
kafka image: confluentinc/cp-kafka:7.8.0
generator: 200 drivers, 1s tick

1 consumer   -> partitions: [...]
3 consumers  -> c1:[...] c2:[...] c3:[...]
kill c2      -> rebalance took ~___ s ; c1:[...] c3:[...]
max lag during rebalance: ___
```

## Talking points

* **Cooperative vs eager:** eager (`RangeAssignor`/`RoundRobin`) revokes *all* partitions
  from *all* members on every rebalance → a global processing pause. Cooperative revokes
  only partitions that change owner, in a second rebalance round → survivors never stop.
* **`max.poll.interval.ms`:** if processing a batch takes longer than this, the broker
  assumes the consumer is dead and rebalances it out — a classic "why does my group keep
  rebalancing" bug. Fix: smaller `max.poll.records` or faster processing, not a bigger
  timeout.
* **Static membership (`group.instance.id`):** lets a consumer restart within
  `session.timeout.ms` *without* triggering a rebalance — useful for rolling deploys.
* **Why Flink bypasses all of this:** it assigns partitions itself and checkpoints offsets,
  so a TaskManager restart is exact and doesn't depend on group-coordinator timing.
