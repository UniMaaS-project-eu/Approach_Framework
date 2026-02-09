import random
import json

random.seed(42)

# -----------------------------
# PARAMETERS
# -----------------------------
NUM_LAYERS = 3
OPTIONS = 20

FILES = {
    "full": "infrastructure_full.json",
    "intent1": "infrastructure_intent1.json",
    "intent2": "infrastructure_intent2.json",
    "intent3": "infrastructure_intent3.json",
}

# -----------------------------
# NODE GENERATION
# -----------------------------
suppliers = {}
sites = {}

for i in range(1, NUM_LAYERS + 1):
    # Ensure at least one Torino in each layer
    torino_assigned_supplier = False
    torino_assigned_site = False

    for j in range(1, OPTIONS + 1):
        loc_s = "Torino" if not torino_assigned_supplier or random.random() < 0.35 else "Other"
        if loc_s == "Torino":
            torino_assigned_supplier = True

        recyclable = "yes" if random.random() < 0.2 else "no"  # Increased probability for testing

        suppliers[f"S{i}{j}"] = {
            "type": "supplier",
            "layer": i,
            "location": loc_s,
            "recyclable": recyclable
        }

        loc_r = "Torino" if not torino_assigned_site or random.random() < 0.35 else "Other"
        if loc_r == "Torino":
            torino_assigned_site = True

        sites[f"R{i}{j}"] = {
            "type": "site",
            "layer": i,
            "location": loc_r
        }

# -----------------------------
# ARC GENERATION
# -----------------------------
arcs = []

def add_arc(u, v, base_cost=1, mode=None):
    arcs.append({
        "from": u,
        "to": v,
        "base_cost": base_cost,
        "mode": mode
    })

for i in range(1, NUM_LAYERS + 1):
    supplier_nodes = [s for s in suppliers if suppliers[s]["layer"] == i]
    site_nodes = [r for r in sites if sites[r]["layer"] == i]
    
    all_pairs = [(s, r) for s in supplier_nodes for r in site_nodes]

    # Guarantee full connectivity within layers
    for s, r in all_pairs:
        mode = "fast" if random.random() < 0.2 else "slow"
        add_arc(s, r, base_cost=1, mode=mode)

    # Connect layer i sites to layer i+1 suppliers
    if i < NUM_LAYERS:
        next_suppliers = [s for s in suppliers if suppliers[s]["layer"] == i + 1]
        
        for r in site_nodes:
            for s2 in next_suppliers:
                add_arc(r, s2, base_cost=1, mode="fast")  # Ensure fast arcs between layers

# -----------------------------
# BASE INFRASTRUCTURE
# -----------------------------
full_data = {
    "num_layers": NUM_LAYERS,
    "options": OPTIONS,
    "suppliers": suppliers,
    "sites": sites,
    "arcs": arcs
}

with open(FILES["full"], "w") as f:
    json.dump(full_data, f, indent=2)

# -----------------------------
# INTENT 1: Recyclable suppliers only
# -----------------------------
suppliers_i1 = {k: v for k, v in suppliers.items() if v["recyclable"] == "yes"}
sites_i1 = sites.copy()
arcs_i1 = [a for a in arcs if (a["from"] in suppliers_i1 or a["from"] in sites_i1)
                        and (a["to"] in suppliers_i1 or a["to"] in sites_i1)]

with open(FILES["intent1"], "w") as f:
    json.dump({
        "num_layers": NUM_LAYERS,
        "options": OPTIONS,
        "suppliers": suppliers_i1,
        "sites": sites_i1,
        "arcs": arcs_i1
    }, f, indent=2)

# -----------------------------
# INTENT 2: Torino only
# -----------------------------
suppliers_i2 = {k: v for k, v in suppliers.items() if v["location"] == "Torino"}
sites_i2 = {k: v for k, v in sites.items() if v["location"] == "Torino"}
arcs_i2 = [a for a in arcs if (a["from"] in suppliers_i2 or a["from"] in sites_i2)
                        and (a["to"] in suppliers_i2 or a["to"] in sites_i2)]

with open(FILES["intent2"], "w") as f:
    json.dump({
        "num_layers": NUM_LAYERS,
        "options": OPTIONS,
        "suppliers": suppliers_i2,
        "sites": sites_i2,
        "arcs": arcs_i2
    }, f, indent=2)

# -----------------------------
# INTENT 3: Fast transportation only
# -----------------------------
arcs_i3 = [a for a in arcs if a["mode"] == "fast"]
nodes_i3 = set()
for a in arcs_i3:
    nodes_i3.add(a["from"])
    nodes_i3.add(a["to"])

suppliers_i3 = {k: v for k, v in suppliers.items() if k in nodes_i3}
sites_i3 = {k: v for k, v in sites.items() if k in nodes_i3}

with open(FILES["intent3"], "w") as f:
    json.dump({
        "num_layers": NUM_LAYERS,
        "options": OPTIONS,
        "suppliers": suppliers_i3,
        "sites": sites_i3,
        "arcs": arcs_i3
    }, f, indent=2)

# Ensure that the full infrastructure is feasible while also stressing it for intent violations
# You can adjust the parameters in the main experimental loop

print("All infrastructures generated:")
for k, v in FILES.items():
    print(f"  {k}: {v}")


# -----------------------------
# DIAGNOSTICS
# -----------------------------
print("\n" + "="*60)
print("INFRASTRUCTURE GENERATION SUMMARY")
print("="*60)

print(f"\n📊 NODE STATISTICS:")
torino_suppliers = sum(1 for v in suppliers.values() if v["location"] == "Torino")
recyclable_suppliers = sum(1 for v in suppliers.values() if v["recyclable"] == "yes")
torino_sites = sum(1 for v in sites.values() if v["location"] == "Torino")

print(f"  Total suppliers: {len(suppliers)}")
print(f"  Torino suppliers: {torino_suppliers} ({100*torino_suppliers/len(suppliers):.1f}%)")
print(f"  Recyclable suppliers: {recyclable_suppliers} ({100*recyclable_suppliers/len(suppliers):.1f}%)")
print(f"  Total sites: {len(sites)}")
print(f"  Torino sites: {torino_sites} ({100*torino_sites/len(sites):.1f}%)")

print(f"\n📈 ARC STATISTICS:")
fast_arcs = sum(1 for a in arcs if a["mode"] == "fast")
print(f"  Total arcs: {len(arcs)}")
print(f"  Fast mode arcs: {fast_arcs} ({100*fast_arcs/len(arcs):.1f}%)")

print(f"\n📁 INTENT-SPECIFIC INFRASTRUCTURES:")
print(f"  Intent 1: {len(suppliers_i1)} suppliers, {len(sites_i1)} sites, {len(arcs_i1)} arcs")
print(f"  Intent 2: {len(suppliers_i2)} suppliers, {len(sites_i2)} sites, {len(arcs_i2)} arcs")
print(f"  Intent 3: {len(suppliers_i3)} suppliers, {len(sites_i3)} sites, {len(arcs_i3)} arcs")

print("\n✅ All infrastructures generated:")
for k, v in FILES.items():
    print(f"  {k}: {v}")
