#!/usr/bin/env python3
"""Hedger: keeps the desk's combined position (taker + quoter + hedger) low.

Rather than parsing the once-a-second `strat.<sender>.status` text (which
would add up to 1s of latency exactly when reacting promptly matters most),
this listens directly to the taker's and quoter's own-fill feeds
(`ex.md.<FEED>.<SENDER>`) to track their positions fill-by-fill in real time,
the same public execution stream anyone can subscribe to.

Whenever the desk's net exposure (taker + quoter + hedger positions) moves
outside +/- HEDGE_THRESHOLD, the hedger sends an aggressive fill-and-kill
order sized to bring it back toward zero, capped per action at
HEDGE_CLIP_MAX so one large breach doesn't turn into one enormous order.

Config (env):
  NATS_URL          default nats://127.0.0.1:4222
  FEED              contract being hedged            (default BTH6)
  TAKER_SENDER      the taker's sender tag            (default PYTKR001)
  QUOTER_SENDER     the quoter's sender tag           (default CPPQTR01)
  HEDGER_SENDER     this process's own sender tag     (default HEDGER01)
  HEDGE_THRESHOLD   max |desk net position| before hedging kicks in (default 5)
  HEDGE_CLIP_MAX    max size of a single hedge order  (default 20)
  HEDGER_MAX_POS    max absolute position for the hedger itself (default 200)
"""
import asyncio
import os
import random

import nats
from nats.js.api import ConsumerConfig, DeliverPolicy

NATS_URL = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
# Prefer an explicit FEED if set; otherwise fall back to TAKER_FEED, since
# that's what's actually provided in some compose setups (the desk trades
# one contract, but only the taker's env var name is guaranteed present).
FEED = os.environ.get("FEED") or os.environ.get("TAKER_FEED", "BTH6")
TAKER_SENDER = os.environ.get("TAKER_SENDER", "PYTKR001")
# Same idea: the quoter's task-spec env var is $SENDER (not $QUOTER_SENDER).
# Prefer an explicit QUOTER_SENDER if one's provided, else fall back to
# SENDER, else a default for standalone/local runs.
QUOTER_SENDER = (os.environ.get("QUOTER_SENDER")
                 or os.environ.get("SENDER", "CPPQTR01"))
HEDGER_SENDER = os.environ.get("HEDGER_SENDER", "HEDGER01")
HEDGE_THRESHOLD = int(os.environ.get("HEDGE_THRESHOLD", "5"))
HEDGE_CLIP_MAX = int(os.environ.get("HEDGE_CLIP_MAX", "20"))
HEDGER_MAX_POS = int(os.environ.get("HEDGER_MAX_POS", "200"))


def parse_execution(fields):
    """fields is the whitespace-split market-data message, index 0 = ts.
    Returns (incoming_id, resting_id, volume, price, agg_side) or None.

    Both E (execution) and T (trade) share the same field layout per
    PROTOCOL.md. Empirically, the resting side of a match sees it as E,
    while the aggressor (e.g. a fill-and-kill order) sees the same match
    as T on its own channel -- so both need to be handled here, or
    aggressor-only senders (like the taker) never get tracked."""
    if len(fields) < 8 or fields[1] not in ("E", "T"):
        return None
    incoming_id, resting_id = fields[2], fields[3]
    volume = int(fields[4])
    price = int(fields[5])
    agg_side = fields[7]
    return incoming_id, resting_id, volume, price, agg_side


class TrackedPosition:
    """Tracks one sender's position purely from watching the public
    execution stream -- used for the taker and quoter, whose orders this
    process does not itself send."""
    def __init__(self, sender):
        self.sender = sender
        self.position = 0

    def on_execution(self, incoming_id, resting_id, volume, agg_side):
        tag = f"{self.sender}:"
        if incoming_id.startswith(tag):
            side = agg_side
        elif resting_id.startswith(tag):
            side = "S" if agg_side == "B" else "B"
        else:
            return
        self.position += volume if side == "B" else -volume


class Hedger:
    def __init__(self, nc):
        self.nc = nc
        self.oid = random.randint(0, 80_000_000)
        self.best_bid = None
        self.best_ask = None

        self.taker = TrackedPosition(TAKER_SENDER)
        self.quoter = TrackedPosition(QUOTER_SENDER)
        self.position = 0     # hedger's own position
        self.cash = 0.0
        self.fills = 0

        self.hedge_lock = asyncio.Lock()

    def next_oid(self):
        self.oid += 1
        return f"{self.oid:08d}"

    def mid(self):
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    def desk_net(self):
        return self.taker.position + self.quoter.position + self.position

    # -------------------------------------------------------------
    # Market data
    # -------------------------------------------------------------

    async def on_bbo(self, msg):
        f = msg.data.decode().split()
        if len(f) < 6:
            return
        self.best_bid = None if f[2] == "-" else int(f[2])
        self.best_ask = None if f[4] == "-" else int(f[4])
        await self.maybe_hedge()

    async def on_taker_md(self, msg):
        parsed = parse_execution(msg.data.decode().split())
        if parsed is not None:
            incoming_id, resting_id, volume, _price, agg_side = parsed
            self.taker.on_execution(incoming_id, resting_id, volume, agg_side)
            await self.maybe_hedge()
        await msg.ack()

    async def on_quoter_md(self, msg):
        parsed = parse_execution(msg.data.decode().split())
        if parsed is not None:
            incoming_id, resting_id, volume, _price, agg_side = parsed
            self.quoter.on_execution(incoming_id, resting_id, volume, agg_side)
            await self.maybe_hedge()
        await msg.ack()

    # -------------------------------------------------------------
    # Hedging
    # -------------------------------------------------------------

    async def maybe_hedge(self):
        net = self.desk_net()
        if abs(net) <= HEDGE_THRESHOLD:
            return
        if self.best_bid is None or self.best_ask is None:
            return  # no price to hedge at yet

        async with self.hedge_lock:
            # Re-check net inside the lock: another call may have already
            # hedged this down while we were waiting for the lock.
            net = self.desk_net()
            if abs(net) <= HEDGE_THRESHOLD:
                return

            excess = abs(net) - HEDGE_THRESHOLD
            size = min(excess, HEDGE_CLIP_MAX)

            if net > 0:
                # Desk is net long -> sell to flatten.
                if self.position - size < -HEDGER_MAX_POS:
                    size = max(0, self.position - (-HEDGER_MAX_POS))
                if size <= 0:
                    print("[hedger] want to sell but at HEDGER_MAX_POS, "
                          "can't reduce further", flush=True)
                    return
                await self.send_hedge("S", size, self.best_bid)
            else:
                # Desk is net short -> buy to flatten.
                if self.position + size > HEDGER_MAX_POS:
                    size = max(0, HEDGER_MAX_POS - self.position)
                if size <= 0:
                    print("[hedger] want to buy but at HEDGER_MAX_POS, "
                          "can't reduce further", flush=True)
                    return
                await self.send_hedge("B", size, self.best_ask)

    async def send_hedge(self, side, size, px):
        oid = self.next_oid()
        order = f"{HEDGER_SENDER} A {FEED} {oid} {side} {size} {px} F"
        try:
            reply = await self.nc.request("ex.req." + HEDGER_SENDER,
                                           order.encode(), timeout=1.0)
        except Exception as e:
            print(f"[hedger] request failed: {e}", flush=True)
            return
        parts = reply.data.decode().split()
        if len(parts) >= 2 and parts[1] == "Y":
            traded = int(parts[2])
            if traded > 0:
                self.apply_fill(side, traded, px)
                self.fills += 1
                print(f"[hedger] hedged {side} {traded}@{px} "
                      f"(desk_net now {self.desk_net()})", flush=True)
        else:
            code = parts[2] if len(parts) > 2 else "?"
            print(f"[hedger] hedge order rejected: {code}", flush=True)

    def apply_fill(self, side, qty, px):
        signed = qty if side == "B" else -qty
        self.position += signed
        self.cash -= signed * px

    def pnl(self):
        m = self.mid()
        return self.cash + (self.position * m if m is not None else 0)

    # -------------------------------------------------------------
    # Reporting
    # -------------------------------------------------------------

    async def publish_status(self):
        s = (f"pos={self.position} cash={self.cash:.0f} pnl={self.pnl():.0f} "
             f"fills={self.fills} taker_pos={self.taker.position} "
             f"quoter_pos={self.quoter.position} desk_net={self.desk_net()}")
        print(f"[hedger] {s}", flush=True)
        await self.nc.publish(f"strat.{HEDGER_SENDER}.status", s.encode())

    async def reporter(self):
        while True:
            await asyncio.sleep(1.0)
            await self.publish_status()


async def main():
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    h = Hedger(nc)

    # Plain core subscription: we only ever care about the *current*
    # best bid/offer, so no replay is needed here.
    await nc.subscribe(f"ex.bbo.{FEED}", cb=h.on_bbo)

    # JetStream, replayed from the start of the stream every time this
    # process (re)connects. Without this, a hedger restart would silently
    # lose track of any taker/quoter fills that happened before it came
    # back up, and its tracked position would be permanently offset from
    # the truth from that point on -- exactly the kind of bug that looks
    # like a small, plausible-but-wrong number rather than an obvious zero.
    all_history = ConsumerConfig(deliver_policy=DeliverPolicy.ALL)
    await js.subscribe(f"ex.md.{FEED}.{TAKER_SENDER}", cb=h.on_taker_md,
                        config=all_history)
    await js.subscribe(f"ex.md.{FEED}.{QUOTER_SENDER}", cb=h.on_quoter_md,
                        config=all_history)

    print(f"[hedger] {HEDGER_SENDER} hedging {FEED} "
          f"(taker={TAKER_SENDER}, quoter={QUOTER_SENDER}) "
          f"threshold={HEDGE_THRESHOLD} clip_max={HEDGE_CLIP_MAX}", flush=True)

    rep = asyncio.create_task(h.reporter())
    try:
        await asyncio.Event().wait()  # run until killed (SIGTERM/SIGINT)
    except asyncio.CancelledError:
        pass
    finally:
        rep.cancel()
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())