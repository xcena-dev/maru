"""Redraw Figure 3 from the historical totals reported in lmcache.md.

Run from any directory with matplotlib installed:
    python docs/source/performance/plot_retrieval_summary.py

The original annotated power trace is preserved in
image/128k_power_comparison.png. These are reported phase totals, not new
measurements or an isolated NIC-transfer benchmark.
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

matplotlib.rcParams["svg.fonttype"] = "none"
matplotlib.rcParams["svg.hashsalt"] = "maru-retrieval-summary"

LABELS = ["LMCache P2P", "Maru (cxl)"]
DURATION_S = [103, 53]
ENERGY_WH = [36.1, 20.1]


def main() -> None:
    """Write a compact summary without changing historical data or assets."""
    fig, axes = plt.subplots(1, 2, figsize=(8.4, 2.4), layout="constrained")
    for ax, values, title, unit in zip(
        axes,
        [DURATION_S, ENERGY_WH],
        ["Retrieval duration", "Retrieval energy"],
        ["s", "Wh"],
        strict=True,
    ):
        bars = ax.barh(LABELS, values, color=["#df5b4d", "#397aa5"], height=0.5)
        ax.invert_yaxis()
        ax.set_title(title, fontsize=11)
        ax.set_xlabel(unit)
        ax.set_xlim(0, max(values) * 1.25)
        ax.bar_label(bars, labels=[f"{v:g} {unit}" for v in values], padding=5)
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_axisbelow(True)
        ax.grid(axis="x", alpha=0.2)
    target = Path(__file__).parent / "image" / "retrieval_phase_summary.svg"
    fig.savefig(target, metadata={"Date": None})
    target.write_text(
        "\n".join(line.rstrip() for line in target.read_text().splitlines()) + "\n"
    )
    plt.close(fig)


if __name__ == "__main__":
    main()
