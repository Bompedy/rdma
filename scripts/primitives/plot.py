#!/usr/bin/env python3
"""Plot primitive benchmark latency curves from a sweep.csv.

Produces three separate PNG plots (p50, p90, p99), each with one curve per
primitive (Synra-TAS, Synra-FAA, Mu-FAA). x-axis is total clients, y-axis is
latency in microseconds.

Usage:
    ./scripts/primitives/plot.py [results/sweep_XXX/sweep.csv]

If no argument is given, the most recent results/sweep_*/sweep.csv is used.

Requires: pip3 install matplotlib pandas
"""

import glob
import os
import sys

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PRIM_NAMES = {
    "synra_tas": "Synra-TAS",
    "synra_faa": "Synra-FAA",
    "mu_faa": "Mu-FAA",
}
PRIM_ORDER = ["synra_tas", "synra_faa", "mu_faa"]
PRIM_STYLE = {
    "synra_tas": ("o-", "tab:blue"),
    "synra_faa": ("s-", "tab:orange"),
    "mu_faa": ("^-", "tab:green"),
}
PLOTS = [("p50", "p50.png", "P50 latency under contention"),
         ("p90", "p90.png", "P90 latency under contention"),
         ("p99", "p99.png", "P99 latency under contention")]


def main():
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        sweeps = sorted(glob.glob("results/sweep_*/sweep.csv"))
        if not sweeps:
            sys.exit("ERROR: no sweep.csv found. Pass a path or run sweep.sh first.")
        csv_path = sweeps[-1]

    df = pd.read_csv(csv_path)
    outdir = os.path.join(os.path.dirname(csv_path), "plots")
    os.makedirs(outdir, exist_ok=True)

    for col, fname, title in PLOTS:
        fig, ax = plt.subplots(figsize=(6, 4))
        for prim in PRIM_ORDER:
            sub = df[df.primitive == prim].sort_values("total_clients")
            if sub.empty:
                continue
            marker, color = PRIM_STYLE[prim]
            ax.plot(sub.total_clients, sub[col], marker, color=color,
                    label=PRIM_NAMES[prim])
        ax.set_xlabel("# Clients")
        ax.set_ylabel(f"{col} latency (\u00b5s)")
        ax.set_title(title)
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(outdir, fname), dpi=150)
        plt.close(fig)

    print(f"Plots written to {outdir}/")


if __name__ == "__main__":
    main()
