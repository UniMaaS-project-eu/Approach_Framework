import csv
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401
import numpy as np
from scipy.interpolate import griddata
from matplotlib import cm

INPUT_CSV1 = "scalability_results_integer.csv"

# Storage
lengths1 = []
cardinalities1 = []

query_times1   = []
csv_times1     = []
parse_times1   = []
model_times1   = []
total_times1   = []

# Load the CSV
with open(INPUT_CSV1, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        lengths1.append(int(row["length"]))
        cardinalities1.append(int(row["cardinality"]))

        query_times1.append(float(row["query_time"]))
        csv_times1.append(float(row["csv_time"]))
        parse_times1.append(float(row["parsing_time"]))
        model_times1.append(float(row["model_time"]))
        total_times1.append(float(row["total_time"]))

def group_by_length(xs, ys, zs):
    grouped = {}
    for x, y, z in zip(xs, ys, zs):
        grouped.setdefault(x, []).append((y, z))
    return grouped

def plot_3d(xs, ys, zs, zlabel, filename, color):
    fig = plt.figure(figsize=(10, 7))
    ax = fig.add_subplot(111, projection="3d")

    # Create mesh grid for surface
    x_unique = np.unique(xs)
    y_unique = np.unique(ys)
    X, Y = np.meshgrid(x_unique, y_unique)
    
    # Interpolate Z values on the grid
    Z = griddata((xs, ys), zs, (X, Y), method='cubic')
    
    # Plot surface with gradient coloring based on Z values
    surf = ax.plot_surface(X, Y, Z, cmap='viridis', alpha=0.7, 
                          edgecolor='none', shade=True)
    
    # Add colorbar
    fig.colorbar(surf, ax=ax, shrink=0.5, aspect=5, label=zlabel)
    
    # Scatter points
    ax.scatter(xs, ys, zs, color='red', s=60, edgecolors='black', 
               linewidths=1.5, label="Data Points", zorder=5)

    ax.set_xlabel("Length", fontsize=11, fontweight='bold')
    ax.set_ylabel("Cardinality", fontsize=11, fontweight='bold')
    ax.set_zlabel(zlabel, fontsize=11, fontweight='bold')
    plt.title(f"{zlabel} vs Graph Size", fontsize=14, fontweight='bold')

    plt.tight_layout()
    plt.savefig(filename, dpi=200, bbox_inches='tight')
    plt.close()

# ============================================================================
# GENERATE 3D SURFACE PLOTS
# ============================================================================

plot_3d(lengths1, cardinalities1, query_times1,
        "Avg Query Time (s)", "query_time_3d.png", "red")

plot_3d(lengths1, cardinalities1, csv_times1,
        "Avg CSV Write Time (s)", "csv_time_3d.png", "green")

plot_3d(lengths1, cardinalities1, parse_times1,
        "Avg Parsing Time (s)", "parse_time_3d.png", "purple")

plot_3d(lengths1, cardinalities1, model_times1,
        "Avg Model Time (s)", "model_time_3d.png", "orange")

plot_3d(lengths1, cardinalities1, total_times1,
        "Avg Total Time (s)", "total_time_3d.png", "black")

# ============================================================================
# GENERATE STACKED BAR PLOT - IMPROVED VERSION WITH ERROR HANDLING
# ============================================================================

# Create combined data structure and sort by total time
data_points = list(zip(lengths1, cardinalities1, query_times1, csv_times1, 
                       parse_times1, model_times1, total_times1))

# ✅ FILTER OUT INVALID DATA POINTS (zero or near-zero total times)
valid_data_points = [d for d in data_points if d[6] > 0.001]

if len(valid_data_points) == 0:
    print("ERROR: No valid data points found (all total times are zero)")
    exit(1)

# Sort by total time (increasing complexity)
valid_data_points.sort(key=lambda x: x[6])

# ✅ MANUALLY SELECT SPECIFIC CONFIGURATIONS
desired_configs = [
    # (3, 6),   # L3×C6
    # (3, 10),  # L3×C18
    (4, 6),   # L4×C6
    (4, 10),
    (6, 10),   # L8×C6
    (3, 18),  # L3×C18
    (6, 18),  # L6×C18
    (6, 22),  # L6×C22
    (12, 26)  # L12×C26
]

# Filter to get only these specific configurations
selected_points = []
for length, card in desired_configs:
    # Find matching data point
    for data_point in valid_data_points:
        if data_point[0] == length and data_point[1] == card:
            selected_points.append(data_point)
            break

print(f"\nSelected {len(selected_points)} specific configurations from {len(valid_data_points)} valid data points")

# Print which configs were found
for point in selected_points:
    print(f"  ✓ Found: L{point[0]}×C{point[1]}")


# print(f"\nSelected {len(selected_points)} configurations from {num_points} valid data points")

# Extract selected data
sorted_lengths = [d[0] for d in selected_points]
sorted_cards = [d[1] for d in selected_points]
sorted_query = [d[2] for d in selected_points]
sorted_csv = [d[3] for d in selected_points]
sorted_parse = [d[4] for d in selected_points]
sorted_model = [d[5] for d in selected_points]
sorted_total = [d[6] for d in selected_points]

# Create x-axis labels (Length × Cardinality)
x_labels = [f"L{l}×C{c}" for l, c in zip(sorted_lengths, sorted_cards)]
x_positions = np.arange(len(x_labels))

# Define colors for each time component
colors = {
    'query': '#e74c3c',      # red
    'csv': '#2ecc71',        # green
    'parsing': '#9b59b6',    # purple
    'model': '#f39c12'       # orange
}

# ============================================================================
# PLOT 1: STACKED BAR WITH LOG SCALE (ALL COMPONENTS VISIBLE)
# ============================================================================

fig, ax = plt.subplots(figsize=(14, 9))

bar_width = 0.7
bars1 = ax.bar(x_positions, sorted_query, bar_width, label='Query Time', 
               color=colors['query'], alpha=0.85)
bars2 = ax.bar(x_positions, sorted_csv, bar_width, bottom=sorted_query, 
               label='CSV Write Time', color=colors['csv'], alpha=0.85)
bars3 = ax.bar(x_positions, sorted_parse, bar_width, 
               bottom=np.array(sorted_query) + np.array(sorted_csv), 
               label='Parsing Time', color=colors['parsing'], alpha=0.85)
bars4 = ax.bar(x_positions, sorted_model, bar_width, 
               bottom=np.array(sorted_query) + np.array(sorted_csv) + np.array(sorted_parse), 
               label='Model Time', color=colors['model'], alpha=0.85)

# ✅ USE LOG SCALE to make all components visible
ax.set_yscale('log')

# Add total time as text on top of each bar
for i, total in enumerate(sorted_total):
    ax.text(i, total * 1.15, f'{total:.1f}s', 
            ha='center', va='bottom', fontsize=10, fontweight='bold')

# Formatting
ax.set_xlabel('Graph Configuration (Length × Cardinality)', fontsize=14, fontweight='bold')
ax.set_ylabel('Time (seconds) - Log Scale', fontsize=14, fontweight='bold')
ax.set_title('Time Component Breakdown by Graph Size (Log Scale)', 
             fontsize=16, fontweight='bold', pad=20)
ax.set_xticks(x_positions)
ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=11)
ax.legend(fontsize=12, loc='upper left', framealpha=0.9)
ax.grid(True, alpha=0.3, axis='y', which='both', linestyle=':')

plt.subplots_adjust(bottom=0.15)  # ✅ Fix tight_layout warning
plt.savefig("time_breakdown_logscale.png", dpi=200, bbox_inches='tight')
plt.close()

# ============================================================================
# PLOT 2: NON-QUERY COMPONENTS ONLY (LINEAR SCALE)
# ============================================================================

fig, ax = plt.subplots(figsize=(14, 9))

bar_width = 0.7
bars2 = ax.bar(x_positions, sorted_csv, bar_width, 
               label='CSV Write Time', color=colors['csv'], alpha=0.85)
bars3 = ax.bar(x_positions, sorted_parse, bar_width, 
               bottom=np.array(sorted_csv), 
               label='Parsing Time', color=colors['parsing'], alpha=0.85)
bars4 = ax.bar(x_positions, sorted_model, bar_width, 
               bottom=np.array(sorted_csv) + np.array(sorted_parse), 
               label='Model Time', color=colors['model'], alpha=0.85)

# Calculate non-query total
non_query_total = np.array(sorted_csv) + np.array(sorted_parse) + np.array(sorted_model)

# Add total time as text on top of each bar
for i, total in enumerate(non_query_total):
    ax.text(i, total * 1.05, f'{total:.2f}s', 
            ha='center', va='bottom', fontsize=10, fontweight='bold')

# Formatting
ax.set_xlabel('Graph Configuration (Length × Cardinality)', fontsize=14, fontweight='bold')
ax.set_ylabel('Time (seconds)', fontsize=14, fontweight='bold')
ax.set_title('Time Component Breakdown (Excluding Query Time)', 
             fontsize=16, fontweight='bold', pad=20)
ax.set_xticks(x_positions)
ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=11)
ax.legend(fontsize=12, loc='upper left', framealpha=0.9)
ax.grid(True, alpha=0.3, axis='y')

plt.subplots_adjust(bottom=0.15)  # ✅ Fix tight_layout warning
plt.savefig("time_breakdown_no_query.png", dpi=200, bbox_inches='tight')
plt.close()

# ============================================================================
# PLOT 3: PERCENTAGE BREAKDOWN (RELATIVE CONTRIBUTION)
# ============================================================================

fig, ax = plt.subplots(figsize=(14, 9))

# ✅ SAFE PERCENTAGE CALCULATION with zero check
query_pct = [(q/t*100 if t > 0 else 0) for q, t in zip(sorted_query, sorted_total)]
csv_pct = [(c/t*100 if t > 0 else 0) for c, t in zip(sorted_csv, sorted_total)]
parse_pct = [(p/t*100 if t > 0 else 0) for p, t in zip(sorted_parse, sorted_total)]
model_pct = [(m/t*100 if t > 0 else 0) for m, t in zip(sorted_model, sorted_total)]

bar_width = 0.7
bars1 = ax.bar(x_positions, query_pct, bar_width, label='Query Time', 
               color=colors['query'], alpha=0.85)
bars2 = ax.bar(x_positions, csv_pct, bar_width, bottom=query_pct, 
               label='CSV Write Time', color=colors['csv'], alpha=0.85)
bars3 = ax.bar(x_positions, parse_pct, bar_width, 
               bottom=np.array(query_pct) + np.array(csv_pct), 
               label='Parsing Time', color=colors['parsing'], alpha=0.85)
bars4 = ax.bar(x_positions, model_pct, bar_width, 
               bottom=np.array(query_pct) + np.array(csv_pct) + np.array(parse_pct), 
               label='Model Time', color=colors['model'], alpha=0.85)

# Add percentage labels for significant components
for i in range(len(x_labels)):
    # Calculate cumulative positions for centering text
    q_mid = query_pct[i] / 2
    c_mid = query_pct[i] + csv_pct[i] / 2
    p_mid = query_pct[i] + csv_pct[i] + parse_pct[i] / 2
    m_mid = query_pct[i] + csv_pct[i] + parse_pct[i] + model_pct[i] / 2
    
    components = [
        ('Q', query_pct[i], q_mid),
        ('C', csv_pct[i], c_mid),
        ('P', parse_pct[i], p_mid),
        ('M', model_pct[i], m_mid)
    ]
    
    for label, pct, y_pos in components:
        if pct > 5:  # Only show if >5%
            ax.text(i, y_pos, f'{pct:.1f}%', 
                   ha='center', va='center', fontsize=8, 
                   color='white', fontweight='bold')

# Formatting
ax.set_xlabel('Graph Configuration (Length × Cardinality)', fontsize=14, fontweight='bold')
ax.set_ylabel('Percentage (%)', fontsize=14, fontweight='bold')
ax.set_title('Relative Time Component Distribution', 
             fontsize=16, fontweight='bold', pad=20)
ax.set_xticks(x_positions)
ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=11)
ax.set_ylim(0, 100)
ax.legend(fontsize=12, loc='upper left', framealpha=0.9)
ax.grid(True, alpha=0.3, axis='y')

plt.subplots_adjust(bottom=0.15)  # ✅ Fix tight_layout warning
plt.savefig("time_breakdown_percentage.png", dpi=200, bbox_inches='tight')
plt.close()

# ============================================================================
# PLOT 4: GROUPED BAR PLOT (SIDE-BY-SIDE) - 10 CONFIGS
# ============================================================================

fig, ax = plt.subplots(figsize=(14, 9))

bar_width = 0.2
x_pos = np.arange(len(x_labels))

bars1 = ax.bar(x_pos - 1.5*bar_width, sorted_query, bar_width, 
               label='Query Time', color=colors['query'], alpha=0.85)
bars2 = ax.bar(x_pos - 0.5*bar_width, sorted_csv, bar_width, 
               label='CSV Write Time', color=colors['csv'], alpha=0.85)
bars3 = ax.bar(x_pos + 0.5*bar_width, sorted_parse, bar_width, 
               label='Parsing Time', color=colors['parsing'], alpha=0.85)
bars4 = ax.bar(x_pos + 1.5*bar_width, sorted_model, bar_width, 
               label='Model Time', color=colors['model'], alpha=0.85)

# ✅ USE LOG SCALE
ax.set_yscale('log')

ax.set_xlabel('Graph Configuration (Length × Cardinality)', fontsize=14, fontweight='bold')
ax.set_ylabel('Time (seconds) - Log Scale', fontsize=14, fontweight='bold')
ax.set_title('Time Component Comparison by Graph Size (Grouped, Log Scale)', 
             fontsize=16, fontweight='bold', pad=20)
ax.set_xticks(x_pos)
ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=11)
ax.legend(fontsize=12, loc='upper left', framealpha=0.9)
ax.grid(True, alpha=0.3, axis='y', which='both', linestyle=':')

plt.subplots_adjust(bottom=0.15)  # ✅ Fix tight_layout warning
plt.savefig("time_breakdown_grouped_logscale.png", dpi=200, bbox_inches='tight')
plt.close()

# Create figure with custom width ratios (3D plot is much wider)
from mpl_toolkits.mplot3d import Axes3D
from scipy.interpolate import griddata
import matplotlib.gridspec as gridspec

# Create figure
fig = plt.figure(figsize=(24, 8))

# ========== LEFT SUBPLOT: Bar Chart ==========
ax1 = fig.add_axes([0.05, 0.12, 0.35, 0.75])

# ✅ Use the multiplication sign × (or normalize to handle both)
labels_to_show = ['L4×C6', 'L4×C10', 'L6×C10', 'L3×C18', 'L6×C18', 'L6×C22', 'L12×C26']
# Note: L5×C6 is NOT in your data, so I removed it

# Alternative: Normalize both x and × to match
def normalize_label(label):
    """Normalize both 'x' and '×' for matching"""
    return label.replace('×', 'x').replace('X', 'x').lower()

labels_to_show_norm = [normalize_label(label) for label in labels_to_show]

keep_indices = []
for i, label in enumerate(x_labels):
    if normalize_label(label) in labels_to_show_norm:
        keep_indices.append(i)
        print(f"✓ Matched: {label}")

print(f"\nFound {len(keep_indices)} matches: {keep_indices}")

# Filter data
filtered_query = [sorted_query[i] for i in keep_indices]
filtered_csv = [sorted_csv[i] for i in keep_indices]
filtered_parse = [sorted_parse[i] for i in keep_indices]
filtered_model = [sorted_model[i] for i in keep_indices]
filtered_x_labels = [x_labels[i] for i in keep_indices]

# Create positions and plot
bar_width = 0.2
x_pos = np.arange(len(filtered_x_labels))

bars1 = ax1.bar(x_pos - 1.5*bar_width, filtered_query, bar_width, 
                label='Query Time', color=colors['query'], alpha=0.85)
bars2 = ax1.bar(x_pos - 0.5*bar_width, filtered_csv, bar_width, 
                label='CSV Write Time', color=colors['csv'], alpha=0.85)
bars3 = ax1.bar(x_pos + 0.5*bar_width, filtered_parse, bar_width, 
                label='Parsing Time', color=colors['parsing'], alpha=0.85)
bars4 = ax1.bar(x_pos + 1.5*bar_width, filtered_model, bar_width, 
                label='Model Time', color=colors['model'], alpha=0.85)

ax1.set_yscale('log')
ax1.set_xlabel('Graph Configuration (Length × Cardinality)', fontsize=22, fontweight='bold')
ax1.set_ylabel('Time (seconds) - Log Scale', fontsize=23, fontweight='bold')
ax1.set_xticks(x_pos)
ax1.set_xticklabels(filtered_x_labels, rotation=45, ha='right', fontsize=15)
ax1.legend(fontsize=22, loc='upper left', framealpha=0.9)
ax1.grid(True, alpha=0.3, axis='y', which='both', linestyle=':')

# plt.show()



print("x_labels type:", type(x_labels))
print("x_labels length:", len(x_labels))
print("First 5 labels:", x_labels[:5])
print("Labels containing 'L3':", [label for label in x_labels if 'L3' in label or 'l3' in label.lower()])


# ========== RIGHT SUBPLOT: 3D Surface Plot ==========
# Manually position the 3D plot right next to the bar chart
ax2 = fig.add_axes([0.4, 0.05, 0.3, 0.9], projection='3d')  # Start at 0.48 (very close to 0.47 end of left plot)

# Create mesh grid for surface
x_unique = np.unique(lengths1)
y_unique = np.unique(cardinalities1)
X, Y = np.meshgrid(x_unique, y_unique)

# Interpolate Z values on the grid
Z = griddata((lengths1, cardinalities1), total_times1, (X, Y), method='cubic')
Z_scaled = Z / 1000
# Plot surface with gradient coloring based on Z values
surf = ax2.plot_surface(X, Y, Z_scaled, cmap='viridis', alpha=0.7, 
                        edgecolor='none', shade=True)

# Add colorbar
# fig.colorbar(surf, ax=ax2, shrink=0.5, aspect=5, label='Avg Total Time (×10³ s)')
fig.colorbar(surf, ax=ax2, shrink=0.2, aspect=5)

# Scatter points
# ax2.scatter(lengths1, cardinalities1, total_times1, color='red', s=60, 
#             edgecolors='black', linewidths=1.5, label="Data Points", zorder=5)

ax2.set_xlabel("Length", fontsize=17, fontweight='bold')
ax2.set_ylabel("Cardinality", fontsize=17, fontweight='bold')
ax2.set_zlabel('Avg Total Time (×10³ s)', fontsize=17, fontweight='bold')

# Make gridlines BARELY visible
ax2.grid(False)
ax2.xaxis._axinfo['grid'].update(color='gray', linestyle=':', linewidth=0.3, alpha=0.05)
ax2.yaxis._axinfo['grid'].update(color='gray', linestyle=':', linewidth=0.3, alpha=0.05)
ax2.zaxis._axinfo['grid'].update(color='gray', linestyle=':', linewidth=0.3, alpha=0.05)

# Make panes transparent and remove edges
ax2.xaxis.pane.fill = False
ax2.yaxis.pane.fill = False
ax2.zaxis.pane.fill = False
ax2.xaxis.pane.set_edgecolor('none')
ax2.yaxis.pane.set_edgecolor('none')
ax2.zaxis.pane.set_edgecolor('none')

plt.savefig("combined_time_analysis.png", dpi=200, bbox_inches='tight')
plt.close()



# ============================================================================
# PRINT SUMMARY
# ============================================================================

print("\n" + "="*70)
print("PLOTS GENERATED (10 SELECTED CONFIGURATIONS):")
print("="*70)
print("3D Surface Plots:")
print("  ✓ query_time_3d.png")
print("  ✓ csv_time_3d.png")
print("  ✓ parse_time_3d.png")
print("  ✓ model_time_3d.png")
print("  ✓ total_time_3d.png")
print("\nBar Plots (10 configurations):")
print("  ✓ time_breakdown_logscale.png (Stacked, log scale - all visible)")
print("  ✓ time_breakdown_no_query.png (Excluding query time)")
print("  ✓ time_breakdown_percentage.png (Relative percentages)")
print("  ✓ time_breakdown_grouped_logscale.png (Side-by-side, log scale)")
print("="*70)

# Print summary statistics for selected configurations
print("\nSELECTED CONFIGURATIONS TIME BREAKDOWN:")
print("="*70)
print(f"{'Config':<12} {'Query':<10} {'CSV':<10} {'Parse':<10} {'Model':<10} {'Total':<10}")
print("-"*70)
for i in range(len(x_labels)):
    print(f"{x_labels[i]:<12} {sorted_query[i]:<10.3f} {sorted_csv[i]:<10.3f} "
          f"{sorted_parse[i]:<10.3f} {sorted_model[i]:<10.3f} {sorted_total[i]:<10.3f}")
print("="*70)

# Calculate statistics (with safety check)
print("\nAVERAGE TIME DISTRIBUTION (SELECTED CONFIGS):")
valid_totals = [(q, c, p, m, t) for q, c, p, m, t in 
                zip(sorted_query, sorted_csv, sorted_parse, sorted_model, sorted_total) if t > 0]

if valid_totals:
    avg_query_pct = np.mean([q/t*100 for q, c, p, m, t in valid_totals])
    avg_csv_pct = np.mean([c/t*100 for q, c, p, m, t in valid_totals])
    avg_parse_pct = np.mean([p/t*100 for q, c, p, m, t in valid_totals])
    avg_model_pct = np.mean([m/t*100 for q, c, p, m, t in valid_totals])

    print(f"  Query Time:   {avg_query_pct:.1f}%")
    print(f"  CSV Time:     {avg_csv_pct:.1f}%")
    print(f"  Parsing Time: {avg_parse_pct:.1f}%")
    print(f"  Model Time:   {avg_model_pct:.1f}%")
else:
    print("  No valid data to calculate percentages")
    
print("="*70)
