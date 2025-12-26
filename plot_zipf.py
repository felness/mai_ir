import csv
import math
import numpy as np
import matplotlib.pyplot as plt

def read_zipf_csv(path="zipf.csv", max_points=200000):
    ranks = []
    freqs = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for i, row in enumerate(r):
            if i >= max_points:
                break
            ranks.append(int(row["rank"]))
            freqs.append(int(row["freq"]))
    return np.array(ranks), np.array(freqs)

def fit_zipf(ranks, freqs):
    x = np.log(ranks)
    y = np.log(freqs)
    a, b = np.polyfit(x, y, 1)   
    s = -a
    C = math.exp(b)
    return C, s

def plot_zipf(path="zipf.csv"):
    ranks, freqs = read_zipf_csv(path)

    C, s = fit_zipf(ranks, freqs)
    zipf = C / (ranks ** s)

    plt.figure()
    plt.xscale("log")
    plt.yscale("log")
    plt.plot(ranks, freqs, marker=".", linestyle="none", label="Corpus")
    plt.plot(ranks, zipf, label=f"Zipf fit: f=C/r^s, s={s:.3f}")
    plt.xlabel("Rank (log)")
    plt.ylabel("Frequency (log)")
    plt.title("Term frequency distribution with Zipf law")
    plt.legend()
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    plot_zipf("zipf.csv")
