# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Per-request phase timeline from Maru timing logs.

Reads an engine stderr log produced with ``maru_log_timing: true`` and draws
one horizontal bar per request (x = time since the round started), split into
the phases of a deferred KV load. Optionally joins client-side records to
extend each bar through prefill and decode.

Phases reconstructed from the log alone:

    queue     deferred load start  ->  retrieve start
    retrieve  CXL metadata RPC + mmap read   ("deferred retrieve batch")
    gpu copy  CXL -> GPU on the load stream  ("deferred gpu-load", CUDA-event
              measured; placed right after retrieve, where the job submits it)
    notify    GPU copy done -> reaped by get_finished_loading

Layerwise-overlap runs (maru_enable_layerwise_overlap) emit no gpu-load
line: the loader reports right after the retrieve and the copies run inside
the resumed forward. There "sched wait" spans retrieve -> reschedule and
"prefill" contains the overlapped per-layer copies.

With ``--client results.jsonl`` (one JSON object per line, in submission
order: {"name": ..., "start": epoch_s, "first_token": epoch_s, "end":
epoch_s}), two more phases appear: prefill (notify -> first token) and decode
(first token -> end). The join is by order of appearance on each side, which
matches submission order; treat it as approximate under high concurrency.

Usage:
    python tools/plot_request_timeline.py engine.stderr.log \
        [--client results.jsonl] [--out timeline.png] [--title "..."]
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from dataclasses import dataclass, field

# Validated categorical palette (dataviz reference, light mode, fixed slot
# order — do not re-order: the order is the CVD-safety mechanism).
PHASE_ORDER = [
    "wait",  # client sent -> loader picked it up (HTTP+tokenize+queue+lookup)
    "queue",
    "retrieve",
    "gpu copy",
    "notify",
    "sched wait",  # load reaped -> vLLM rescheduled the request (engine-exact)
    "prefill",
    "decode",
]
PHASE_COLORS = {
    "wait": "#2a78d6",
    "queue": "#eb6834",
    "retrieve": "#1baf7a",
    "gpu copy": "#eda100",
    "notify": "#e87ba4",
    "sched wait": "#008300",
    "prefill": "#e34948",
    "decode": "#4a3aa7",
}
SURFACE, INK, INK_2 = "#fcfcfb", "#1a1a19", "#5f5e56"

_LINE = re.compile(r"Maru timing: t=(?P<t>\d+\.\d+) (?P<msg>.*)")
_REQ = re.compile(r"\(req (?P<req>[^)]+)\)")
_MS = re.compile(r"(?P<ms>\d+(?:\.\d+)?) ms")


@dataclass
class Request:
    req_id: str
    submit: float | None = None  # deferred load start
    retrieve_end: float | None = None
    retrieve_ms: float | None = None
    gpu_ms: float | None = None
    reap: float | None = None  # emitted at get_finished_loading
    resumed: float | None = None  # vLLM rescheduled the request
    resumed_ntok: int | None = None  # tokens it was given to recompute
    client: dict = field(default_factory=dict)
    # Sub-phase durations, reported in the stats table only (they overlap or
    # nest inside the bar phases, so drawing them would double-count).
    extras: dict[str, float] = field(default_factory=dict)

    def phases(self) -> list[tuple[str, float, float]]:
        """Return (phase, start_epoch, end_epoch) tuples, best effort."""
        out = []
        arrive = self.client.get("arrive_epoch")
        if arrive and self.submit and self.submit > arrive:
            out.append(("wait", arrive, self.submit))
        r_end = self.retrieve_end
        r_start = r_end - self.retrieve_ms / 1e3 if r_end and self.retrieve_ms else None
        if self.submit and r_start:
            out.append(("queue", self.submit, r_start))
        if r_start and r_end:
            out.append(("retrieve", r_start, r_end))
        gpu_end = None
        if r_end and self.gpu_ms is not None:
            gpu_end = r_end + self.gpu_ms / 1e3
            out.append(("gpu copy", r_end, gpu_end))
        if gpu_end and self.reap and self.reap > gpu_end:
            out.append(("notify", gpu_end, self.reap))
        if self.reap and self.resumed and self.resumed > self.reap:
            out.append(("sched wait", self.reap, self.resumed))
        elif self.reap is None and r_end and self.resumed and self.resumed > r_end:
            # Layerwise overlap: the loader reports right after the retrieve
            # (no gpu-load line), so the whole retrieve -> reschedule gap is
            # unpark latency. The per-layer copies then run inside "prefill".
            out.append(("sched wait", r_end, self.resumed))
        ft, end = self.client.get("first_token"), self.client.get("end")
        anchor = self.resumed or self.reap or gpu_end
        if anchor and ft and ft > anchor:
            out.append(("prefill", anchor, ft))
        if ft and end and end > ft:
            out.append(("decode", ft, end))
        return out

    @property
    def label(self) -> str:
        return self.client.get("name") or self.req_id


def parse_log(path: str) -> dict[str, Request]:
    reqs: dict[str, Request] = {}

    def req_for(msg: str) -> Request | None:
        m = _REQ.search(msg)
        if not m:
            return None
        return reqs.setdefault(m.group("req"), Request(m.group("req")))

    for line in open(path, errors="replace"):
        m = _LINE.search(line)
        if not m:
            continue
        t, msg = float(m.group("t")), m.group("msg")
        r = req_for(msg)
        if r is None:
            continue
        ms = _MS.search(msg)
        if msg.startswith("deferred load start"):
            r.submit = t
        elif "retrieve batch" in msg and ms:
            r.retrieve_end, r.retrieve_ms = t, float(ms.group("ms"))
        elif msg.startswith("deferred gpu-load") and ms:
            r.gpu_ms, r.reap = float(ms.group("ms")), t
        elif msg.startswith("resumed sched"):
            r.resumed = t
            mt = re.search(r"resumed sched (\d+) tok", msg)
            r.resumed_ntok = int(mt.group(1)) if mt else None
        elif ms:
            # Sub-phases nested inside the bar phases: table-only.
            for tag in (
                "deferred slots",
                "deferred pin",
                "deferred submit",
                "admission wait",
            ):
                if msg.startswith(tag):
                    r.extras[tag] = float(ms.group("ms"))
    return reqs


def join_client(reqs: dict[str, Request], path: str) -> None:
    """Join client records to requests by order of appearance (approximate)."""
    records = [json.loads(ln) for ln in open(path) if ln.strip()]
    ordered = sorted(
        (r for r in reqs.values() if r.submit), key=lambda r: r.submit or 0
    )
    # strict=False: a partial client file still annotates what it covers.
    for r, rec in zip(ordered, records, strict=False):
        r.client = rec


def join_naru_csv(reqs: dict[str, Request], path: str) -> None:
    """Join a naru per-request CSV exactly, and drop unjoined requests.

    The CSV's ``response_id`` is a prefix of the engine request id, so the
    join is exact, not order-based. Its times are relative to the round
    start; the round's epoch t0 is recovered from two hard constraints that
    hold because client and engine share one clock: the first token cannot
    precede the load's completion (reap), and the load cannot start before
    the client sent the request. Requests absent from the CSV (e.g. the
    warmup round) are removed, so the chart shows exactly the CSV's round.
    """
    import csv as _csv

    rows = list(_csv.DictReader(open(path)))
    by_prefix = {row["response_id"]: row for row in rows}

    joined: list[tuple[Request, dict]] = []
    for r in list(reqs.values()):
        rec = next((v for k, v in by_prefix.items() if r.req_id.startswith(k)), None)
        if rec is None:
            del reqs[r.req_id]
            continue
        joined.append((r, rec))

    # t0 feasibility band: reap_i <= t0 + ft_rel_i  and  t0 + start_rel_i <=
    # submit_i for every joined request. Midpoint balances the two errors.
    lo, hi = [], []
    for r, rec in joined:
        ft_rel, start_rel = float(rec["ttft_time"]), float(rec["request_start"])
        if r.reap:
            lo.append(r.reap - ft_rel)
        if r.submit:
            hi.append(r.submit - start_rel)
    if not lo or not hi:
        print("naru-csv: not enough engine anchors to place client times")
        return
    t0 = (max(lo) + min(hi)) / 2
    slack = (min(hi) - max(lo)) * 1e3
    print(f"naru-csv: joined {len(joined)} requests, anchor slack {slack:.0f} ms")
    for r, rec in joined:
        r.client = {
            "name": f"r{rec['prompt_id']} ({rec['response_id']})",
            "arrive_epoch": t0 + float(rec["request_start"]),
            "first_token": t0 + float(rec["ttft_time"]),
            "end": t0 + float(rec["request_end"]),
        }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("log", help="engine stderr log with 'Maru timing' lines")
    ap.add_argument("--client", help="client JSONL: name/start/first_token/end")
    ap.add_argument(
        "--naru-csv",
        help="naru per-request CSV (e.g. inst2_query_round.csv): exact join "
        "by response_id, chart restricted to that round",
    )
    ap.add_argument("--out", default="timeline.png")
    ap.add_argument("--title", default="Per-request KV load timeline")
    args = ap.parse_args()

    reqs = parse_log(args.log)
    if args.naru_csv:
        join_naru_csv(reqs, args.naru_csv)
    elif args.client:
        join_client(reqs, args.client)
    rows = [r for r in reqs.values() if r.phases()]
    rows.sort(key=lambda r: r.submit or r.retrieve_end or 0)
    if not rows:
        print("no request timelines found; was maru_log_timing enabled?")
        return 1

    t0 = min(p[1] for r in rows for p in r.phases())
    durations: dict[str, list[float]] = {p: [] for p in PHASE_ORDER}
    for r in rows:
        for name, s, e in r.phases():
            durations[name].append((e - s) * 1e3)

    # Table view (also the relief for below-3:1 slots in the legend).
    extras: dict[str, list[float]] = {}
    for r in rows:
        for tag, v in r.extras.items():
            extras.setdefault(tag, []).append(v)
    print(f"{'phase':<16} {'n':>4} {'median ms':>10} {'p90 ms':>10} {'max ms':>10}")
    for name in PHASE_ORDER:
        d = durations[name]
        if not d:
            continue
        d.sort()
        print(
            f"{name:<16} {len(d):>4} {statistics.median(d):>10.1f} "
            f"{d[int(0.9 * (len(d) - 1))]:>10.1f} {d[-1]:>10.1f}"
        )
    for name in sorted(extras):
        d = sorted(extras[name])
        print(
            f"  {name:<14} {len(d):>4} {statistics.median(d):>10.1f} "
            f"{d[int(0.9 * (len(d) - 1))]:>10.1f} {d[-1]:>10.1f}  (nested)"
        )

    ntoks = sorted(r.resumed_ntok for r in rows if r.resumed_ntok is not None)
    if ntoks:
        print(
            f"recomputed tokens per resumed request: median "
            f"{ntoks[len(ntoks) // 2]}, max {ntoks[-1]}"
        )

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    fig, ax = plt.subplots(
        figsize=(12, max(3.0, 0.32 * len(rows) + 1.6)), facecolor=SURFACE
    )
    ax.set_facecolor(SURFACE)
    for i, r in enumerate(rows):
        for name, s, e in r.phases():
            ax.barh(
                i,
                (e - s) * 1e3,
                left=(s - t0) * 1e3,
                height=0.62,
                color=PHASE_COLORS[name],
                edgecolor=SURFACE,  # 2px surface gap between segments
                linewidth=0.8,
            )
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([r.label for r in rows], fontsize=7, color=INK_2)
    ax.invert_yaxis()
    ax.set_xlabel("time since round start (ms)", color=INK_2)
    ax.set_title(args.title, color=INK, loc="left", fontsize=12)
    ax.tick_params(colors=INK_2)
    for spine in ("top", "right", "left"):
        ax.spines[spine].set_visible(False)
    ax.spines["bottom"].set_color(INK_2)
    ax.grid(axis="x", color=INK_2, alpha=0.15, linewidth=0.6)
    ax.set_axisbelow(True)

    # Legend with median durations: direct labels are the relief for the
    # palette slots that sit below 3:1 contrast on the light surface.
    handles = [
        Patch(
            facecolor=PHASE_COLORS[p],
            label=f"{p} · median {statistics.median(durations[p]):.0f} ms",
        )
        for p in PHASE_ORDER
        if durations[p]
    ]
    # Upper right is empty by construction: requests stack down-and-right.
    ax.legend(
        handles=handles,
        loc="upper right",
        frameon=False,
        fontsize=8,
        labelcolor=INK,
    )
    fig.tight_layout()
    fig.savefig(args.out, dpi=140)
    print(f"wrote {args.out} ({len(rows)} requests)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
