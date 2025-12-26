#!/usr/bin/env python3
"""
datagen.py (backwards-compatible)

Usage:
    python datagen.py ROWS COLS SPARSITY OUT_FILE

Outputs lines: "row col value"
Does NOT use f-strings so it's compatible with Python 3.4/3.5/3.6+.
"""
from __future__ import print_function
import sys
import random
import numpy as np
import os

def gen_matrix(rows, cols, sparsity, outfile, seed=42):
    random.seed(seed)
    np.random.seed(seed)

    rows = int(rows)
    cols = int(cols)
    sparsity = float(sparsity)
    total = rows * cols
    nnz = int(total * sparsity)

    print("[datagen] rows={0}, cols={1}, sparsity={2}, approx nnz={3}".format(rows, cols, sparsity, nnz))
    print("[datagen] output -> {0}".format(outfile))

    outdir = os.path.dirname(outfile)
    if outdir:
        try:
            os.makedirs(outdir)
        except OSError:
            # directory may already exist
            pass

    with open(outfile, "w") as f:
        if sparsity >= 0.3:
            # dense-ish: iterate rows, sample per-row
            for i in range(rows):
                if sparsity >= 0.999999:
                    vals = np.random.randn(cols)
                    # write entire row
                    for j in range(cols):
                        f.write("%d,%d,%s\n" % (i, j, repr(float(vals[j]))))
                else:
                    mask = np.random.rand(cols) <= sparsity
                    if mask.any():
                        vals = np.random.randn(cols)
                        for j in range(cols):
                            if mask[j]:
                                f.write("%d,%d,%s\n" % (i, j, repr(float(vals[j]))))
        else:
            # sparse: sample unique coordinates
            coords = set()
            # sample until we have nnz unique positions
            while len(coords) < nnz:
                i = random.randint(0, rows - 1)
                j = random.randint(0, cols - 1)
                coords.add((i, j))
            for (i, j) in coords:
                val = float(np.random.randn())
                f.write("%d,%d,%s\n" % (i, j, repr(val)))

def gen_broadcast_pair(name, A_rows, A_cols, B_rows, B_cols,
                       A_sparsity, B_sparsity, outdir="data"):
    if A_cols != B_rows:
        raise ValueError("A_cols must equal B_rows for multiplication")
    if not os.path.exists(outdir):
        try:
            os.makedirs(outdir)
        except OSError:
            pass
    A_file = os.path.join(outdir, "A_{0}.txt".format(name))
    B_file = os.path.join(outdir, "B_{0}.txt".format(name))
    if not os.path.exists(A_file):
        gen_matrix(A_rows, A_cols, A_sparsity, A_file)
    else:
        print("[datagen] skip existing {0}".format(A_file))
    if not os.path.exists(B_file):
        gen_matrix(B_rows, B_cols, B_sparsity, B_file)
    else:
        print("[datagen] skip existing {0}".format(B_file))
    return A_file, B_file

if __name__ == "__main__":
    if len(sys.argv) == 5:
        rows = int(sys.argv[1])
        cols = int(sys.argv[2])
        sparsity = float(sys.argv[3])
        outfile = sys.argv[4]
        gen_matrix(rows, cols, sparsity, outfile)
    else:
        print("Usage: python datagen.py ROWS COLS SPARSITY OUT_FILE")
        sys.exit(1)
