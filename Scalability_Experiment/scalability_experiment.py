#!/usr/bin/env python3

import subprocess
import json
import csv
import matplotlib.pyplot as plt
import re

GENERATOR_SCRIPT = "kg_generator2.py"
RESULTS_FILE = "scalability_results_integer.csv"

# Test values of MSC length and #of sites per process, resources per processConfiguration, suppliers per resource
LENGTH_VALUES = [3, 4, 6, 8, 10, 12]
CARDINALITY_VALUES = [6, 10, 14, 18, 22, 26]
# LENGTH_VALUES = [3, 4, 6, 8]
# CARDINALITY_VALUES = [6, 10]

def parse_timings(stdout):
    """
    Extract timing numbers from kg_generator2 output.
    """
    pattern = r"AVG (\w+)_time\s*:\s*([0-9.]+)"
    matches = re.findall(pattern, stdout)

    result = {
        "query": 0,
        "csv": 0,
        "parsing": 0,
        "model": 0,
        "total": 0
    }

    for key, value in matches:
        if key in result:
            result[key] = float(value)

    return result

def run_experiment(length, cardinality):
    """
    Runs the graph generator with given parameters and extracts average query and optimization model times.
    """
    cmd = [
        "python3",
        GENERATOR_SCRIPT,
        "--length", str(length),
        "--sites-per-process", str(cardinality),
        "--resources-per-pc", str(cardinality),
        "--suppliers-per-resource", str(cardinality),
        "--measure"
    ]

    print(f"\nRunning experiment: length={length}, cardinality={cardinality}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )

    stdout, stderr = process.communicate()

    if process.returncode < 0 or process.returncode > 255:
        print(stderr)
        raise RuntimeError("Generator script failed")
    
    times = parse_timings(stdout)
    return times


# def export_results(filename, xs, ys, zs):
#     with open(filename, "w", newline="") as f:
#         writer = csv.writer(f)
#         writer.writerow(["length", "cardinality", "avg_time"])
#         for i in range(len(xs)):
#             writer.writerow([xs[i], ys[i], zs[i]])

#     print(f"\nResults exported to {filename}")

def main():

    rows = []

    for length in LENGTH_VALUES:
        for cardinality in CARDINALITY_VALUES:
            # print(f"\nRunning experiment length={length}, cardinality={cardinality}")

            times = run_experiment(length, cardinality)

            rows.append([
                length, cardinality,
                times["query"],
                times["csv"],
                times["parsing"],
                times["model"],
                times["total"]
            ])

    
    # Save results CSV
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "length", "cardinality",
            "query_time", "csv_time",
            "parsing_time", "model_time", "total_time"
        ])
        writer.writerows(rows)

    print(f"\nResults saved to {RESULTS_FILE}")


if __name__ == "__main__":
    main()
