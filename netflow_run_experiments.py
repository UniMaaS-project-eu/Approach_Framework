#!/usr/bin/env python3
"""
Experiment Runner: Multi-Commodity Flow with Complete Metrics Export
Exports ALL data needed for visualization scripts.
"""

import numpy as np
import gurobipy as gp
from gurobipy import GRB
import json
import time
import csv
from collections import defaultdict

# ============================================================================
# CONFIGURATION
# ============================================================================

# ============================================================================
# CONFIGURATION
# ============================================================================

VIOLATION_COST = 2
CAPACITY = 40
DEMAND_RANGE = range(10, 1670, 50)

INTENT_BASE_COSTS = {
    1: 4,
    2: 3,
    3: 2
}


# ============================================================================
# RETAILER SETUP
# ============================================================================

retailers = []
k = 0
for product in [1, 2, 3]:
    for intent in [1, 2, 3]:
        k += 1
        retailers.append({
            "id": k,
            "product": product,
            "intent": intent
        })

intent1_retailers = [r for r in retailers if r["intent"] == 1]
intent2_retailers = [r for r in retailers if r["intent"] == 2]
intent3_retailers = [r for r in retailers if r["intent"] == 3]

# Load full infrastructure
with open("infrastructure_full.json", "r") as f:
    infrafull = json.load(f)
arc_capacity_full = {(a["from"], a["to"]): CAPACITY for a in infrafull["arcs"]}

print(f"{len(retailers)} retailers configured")
print(f"{len(arc_capacity_full)} arcs in full infrastructure\n")

# ============================================================================
# GLOBAL TRACKING: Arc Usage Across All Demands
# ============================================================================

arc_usage_history = {
    "intent1": defaultdict(list),
    "intent2": defaultdict(list),
    "intent3": defaultdict(list),
    "full": defaultdict(list)
}

demand_history = []

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def analyze_graph_size(infra_file):
    """Analyze infrastructure graph complexity."""
    with open(infra_file, "r") as f:
        infra = json.load(f)
    
    num_suppliers = len(infra["suppliers"])
    num_sites = len(infra["sites"])
    num_nodes = num_suppliers + num_sites
    num_arcs = len(infra["arcs"])
    
    if "full" in infra_file.lower():
        num_commodities = 9
    else:
        num_commodities = 3
    
    num_variables = num_commodities * num_arcs
    num_constraints = num_commodities * num_nodes + num_arcs
    
    return {
        "nodes": num_nodes,
        "suppliers": num_suppliers,
        "sites": num_sites,
        "arcs": num_arcs,
        "commodities": num_commodities,  
        "variables": num_variables,
        "constraints": num_constraints,
        "state_space": num_variables * num_constraints 
    }


def calculate_enhanced_bottleneck_metrics(arc_usage, arc_capacities):
    
    active_utilizations = []
    all_utilizations = []
    residuals = []
    num_unused = 0
    
    for arc_key, capacity in arc_capacities.items():
        if capacity == float('inf'):
            continue
            
        used = arc_usage.get(arc_key, 0)
        residual = capacity - used
        utilization = (used / capacity * 100) if capacity > 0 else 0
        
        all_utilizations.append(utilization)
        residuals.append(residual)
        
        # Count unused arcs
        if used < 1e-3:  # Arc is NOT being used
            num_unused += 1
        else:
            active_utilizations.append(utilization)
    
    if not all_utilizations:
        return {
            "max_utilization": 0,
            "p95_utilization": 0,
            "p90_utilization": 0,
            "p75_utilization": 0,
            "active_arc_max_util": 0,
            "active_arc_avg_util": 0,
            "num_active_arcs": 0,
            "num_saturated_arcs": 0,
            "num_near_saturated_arcs": 0,
            "num_unused_arcs": 0, 
            "avg_utilization_pct": 0,
            "avg_residual_pct": 100.0,
            "total_residual_capacity": 0,
            "avg_residual_capacity": 0,
            "min_residual_capacity": 0, 
        }
    
    p95 = np.percentile(all_utilizations, 95) if len(all_utilizations) > 0 else 0
    p90 = np.percentile(all_utilizations, 90) if len(all_utilizations) > 0 else 0
    p75 = np.percentile(all_utilizations, 75) if len(all_utilizations) > 0 else 0
    
    active_max = max(active_utilizations) if active_utilizations else 0
    active_avg = np.mean(active_utilizations) if active_utilizations else 0
    
    num_saturated = sum(1 for u in all_utilizations if u > 99.9)
    num_near_saturated = sum(1 for u in all_utilizations if 90 <= u <= 99.9)
    
    avg_residual_pct = 100 - np.mean(all_utilizations)
    
    # calculate minimum residual capacity (tightest bottleneck)
    min_residual = min(residuals) if residuals else 0
    
    return {
        "max_utilization": max(all_utilizations),
        "p95_utilization": p95,
        "p90_utilization": p90,
        "p75_utilization": p75,
        "active_arc_max_util": active_max,
        "active_arc_avg_util": active_avg,
        "num_active_arcs": len(active_utilizations),
        "num_saturated_arcs": num_saturated,
        "num_near_saturated_arcs": num_near_saturated,
        "num_unused_arcs": num_unused,
        "avg_utilization_pct": np.mean(all_utilizations),
        "avg_residual_pct": avg_residual_pct,
        "total_residual_capacity": sum(residuals),
        "avg_residual_capacity": np.mean(residuals),
        "min_residual_capacity": min_residual, 
    }


def solve_instance_gurobi_style(infra_file, retailers_subset, demand_value, 
                                arc_capacities, apply_intent_penalty=True, 
                                infra_key=None):
    """Solve multi-commodity flow with complete metrics tracking."""
    start_time = time.perf_counter()

    with open(infra_file, "r") as f:
        infra = json.load(f)
    
    suppliers = infra["suppliers"]
    sites = infra["sites"]
    infra_arcs = infra["arcs"]
    
    commodities = [r["id"] for r in retailers_subset]
    
    nodes = set()
    for arc in infra_arcs:
        nodes.add(arc["from"])
        nodes.add(arc["to"])
    
    for comm in commodities:
        nodes.add(f"src_{comm}")
        nodes.add(f"sink_{comm}")
    nodes = list(nodes)

    arcs = []
    capacity = {}
    arc_data = {}
    
    for arc in infra_arcs:
        arc_key = (arc["from"], arc["to"])
        arcs.append(arc_key)
        capacity[arc_key] = arc_capacities.get(arc_key, 0)
        arc_data[arc_key] = {
            "base_cost": arc["base_cost"],
            "mode": arc.get("mode", None),
            "is_virtual": False
        }
    
    for comm in commodities:
        for supplier_id in suppliers:
            if suppliers[supplier_id]["layer"] == 1:
                arc_key = (f"src_{comm}", supplier_id)
                arcs.append(arc_key)
                capacity[arc_key] = float('inf')
                arc_data[arc_key] = {
                    "base_cost": 0,
                    "mode": None,
                    "is_virtual": True
                }

    for comm in commodities:
        retailer = next(r for r in retailers_subset if r["id"] == comm)
        for site_id in sites:
            if sites[site_id]["layer"] == retailer["product"]:
                arc_key = (site_id, f"sink_{comm}")
                arcs.append(arc_key)
                capacity[arc_key] = float('inf')
                arc_data[arc_key] = {
                    "base_cost": 0,
                    "mode": None,
                    "is_virtual": True
                }

    physical_nodes = len([n for n in nodes if not (n.startswith("src_") or n.startswith("sink_"))])
    physical_arcs = len([a for a in arcs if not arc_data[a]["is_virtual"]])
    
    graph_metrics = {
        "physical_nodes": physical_nodes,
        "physical_arcs": physical_arcs,
        "total_nodes": len(nodes),
        "total_arcs": len(arcs),
        "num_commodities": len(commodities),
        "num_variables": len(commodities) * len(arcs),
        "num_constraints": len(commodities) * len(nodes) + physical_arcs
    }
    
    cost = {}

    for comm in commodities:
        retailer = next(r for r in retailers_subset if r["id"] == comm)
        
        # intent-specific base cost
        intent_base_cost = INTENT_BASE_COSTS[retailer["intent"]]
        
        for arc_key in arcs:
            if arc_data[arc_key]["is_virtual"]:
                cost[comm, arc_key[0], arc_key[1]] = 0
                continue
            
            # Use intent-specific base cost instead of arc's base_cost
            total_cost = intent_base_cost
            
            if apply_intent_penalty:
                from_node, to_node = arc_key
                mode = arc_data[arc_key]["mode"]
                
                if retailer["intent"] == 1:
                    if from_node in suppliers and suppliers[from_node].get("recyclable") != "yes":
                        total_cost += VIOLATION_COST
                
                if retailer["intent"] == 2:
                    if from_node in suppliers and suppliers[from_node].get("location") != "Torino":
                        total_cost += VIOLATION_COST
                    if to_node in sites and sites[to_node].get("location") != "Torino":
                        total_cost += VIOLATION_COST
                
                if retailer["intent"] == 3 and mode is not None and mode != "fast":
                    total_cost += VIOLATION_COST
            
            cost[comm, arc_key[0], arc_key[1]] = total_cost

    
    inflow = {}
    for comm in commodities:
        for node in nodes:
            if node == f"src_{comm}":
                inflow[comm, node] = demand_value
            elif node == f"sink_{comm}":
                inflow[comm, node] = -demand_value
            else:
                inflow[comm, node] = 0
    
    m = gp.Model('netflow_supply_chain')
    m.Params.OutputFlag = 0
    
    flow = m.addVars(commodities, arcs, obj=cost, vtype=GRB.INTEGER, name="flow")
    
    for arc_key in arcs:
        if not arc_data[arc_key]["is_virtual"]:
            m.addConstr(
                flow.sum("*", arc_key[0], arc_key[1]) <= capacity[arc_key],
                name=f"capacity_{arc_key[0]}_{arc_key[1]}"
            )
    
    m.addConstrs(
        (flow.sum(h, "*", j) + inflow[h, j] == flow.sum(h, j, "*")
         for h in commodities for j in nodes),
        "flow_conservation"
    )
    
    m.optimize()
    solve_time = time.perf_counter() - start_time
    
    if m.status == GRB.OPTIMAL:
        solution = m.getAttr("X", flow)
        
        arc_usage = {}
        for arc_key in arcs:
            if not arc_data[arc_key]["is_virtual"]:
                total_flow = sum(solution.get((h, arc_key[0], arc_key[1]), 0) for h in commodities)
                arc_usage[arc_key] = total_flow
        
        if infra_key:
            for arc_key in arc_usage:
                arc_usage_history[infra_key][arc_key].append(arc_usage[arc_key])
        
        bottleneck_metrics = calculate_enhanced_bottleneck_metrics(arc_usage, capacity)
        
        cost_per_group = {}
        violations_per_intent = {"intent1": 0, "intent2": 0, "intent3": 0}
        violation_cost_per_intent = {"intent1": 0.0, "intent2": 0.0, "intent3": 0.0}
        
        for group_name, group_retailers in [("intent1", intent1_retailers), 
                                           ("intent2", intent2_retailers), 
                                           ("intent3", intent3_retailers)]:
            group_cost = 0
            group_commodities = [r["id"] for r in group_retailers if r["id"] in commodities]
            
            for comm in group_commodities:
                retailer = next(r for r in retailers_subset if r["id"] == comm)
                intent_key = f"intent{retailer['intent']}"
                
                for arc_key in arcs:
                    flow_val = solution.get((comm, arc_key[0], arc_key[1]), 0)
                    if flow_val > 0:
                        arc_cost = cost[comm, arc_key[0], arc_key[1]]
                        group_cost += arc_cost * flow_val
                        
                        if not arc_data[arc_key]["is_virtual"] and apply_intent_penalty:
                            # use intent-specific base cost for violation detection
                            intent_base = INTENT_BASE_COSTS[retailer["intent"]]
                            if arc_cost > intent_base:
                                violations_per_intent[intent_key] += 1
                                violation_cost_per_intent[intent_key] += (arc_cost - intent_base) * flow_val

            
            cost_per_group[group_name] = group_cost
        
        print(f"    FEASIBLE (cost: {m.ObjVal:.2f}, max_util: {bottleneck_metrics['max_utilization']:.1f}%)")
        
        return (m.ObjVal, solve_time, arc_usage, cost_per_group, 
                graph_metrics, violations_per_intent, violation_cost_per_intent,
                bottleneck_metrics, True)
    
    elif m.status == GRB.INFEASIBLE:
        print(f"    INFEASIBLE")
        
        if infra_key:
            for arc in infra_arcs:
                arc_key = (arc["from"], arc["to"])
                arc_usage_history[infra_key][arc_key].append(0)
        
        empty_metrics = {
            "max_utilization": 0, "p95_utilization": 0, "p90_utilization": 0,
            "p75_utilization": 0, "active_arc_max_util": 0, "active_arc_avg_util": 0,
            "num_active_arcs": 0, "num_saturated_arcs": 0, "num_near_saturated_arcs": 0,
            "num_unused_arcs": 0, "avg_utilization_pct": 0, "avg_residual_pct": 100.0,
            "total_residual_capacity": 0, "avg_residual_capacity": 0, "min_residual_capacity": 0
        }
        return (None, solve_time, {}, {}, graph_metrics,
                {"intent1": 0, "intent2": 0, "intent3": 0},
                {"intent1": 0.0, "intent2": 0.0, "intent3": 0.0},
                empty_metrics, False)
    else:
        print(f"    Solver status {m.status}")
        
        if infra_key:
            for arc in infra_arcs:
                arc_key = (arc["from"], arc["to"])
                arc_usage_history[infra_key][arc_key].append(0)
        
        empty_metrics = {
            "max_utilization": 0, "p95_utilization": 0, "p90_utilization": 0,
            "p75_utilization": 0, "active_arc_max_util": 0, "active_arc_avg_util": 0,
            "num_active_arcs": 0, "num_saturated_arcs": 0, "num_near_saturated_arcs": 0,
            "num_unused_arcs": 0, "avg_utilization_pct": 0, "avg_residual_pct": 100.0,
            "total_residual_capacity": 0, "avg_residual_capacity": 0, "min_residual_capacity": 0
        }
        return (None, solve_time, {}, {}, graph_metrics,
                {"intent1": 0, "intent2": 0, "intent3": 0},
                {"intent1": 0.0, "intent2": 0.0, "intent3": 0.0},
                empty_metrics, False)


# ============================================================================
# INFRASTRUCTURE ANALYSIS
# ============================================================================

print("="*70)
print("INFRASTRUCTURE GRAPH ANALYSIS")
print("="*70)

infra_files = {
    "Full": "infrastructure_full.json",
    "Intent 1": "infrastructure_intent1.json",
    "Intent 2": "infrastructure_intent2.json",
    "Intent 3": "infrastructure_intent3.json"
}

graph_sizes = {}
for name, file in infra_files.items():
    sizes = analyze_graph_size(file)
    graph_sizes[name] = sizes
    print(f"\n{name} Infrastructure:")
    print(f"  Nodes: {sizes['nodes']} | Arcs: {sizes['arcs']} | Commodities: {sizes['commodities']}")
    print(f"  Variables: {sizes['variables']} | Constraints: {sizes['constraints']}")
    print(f"  State space: {sizes['state_space']:,}")

print("\n" + "="*70 + "\n")

# ============================================================================
# MAIN EXPERIMENT LOOP
# ============================================================================

print("="*70)
print("STARTING EXPERIMENTS WITH COMPLETE METRICS TRACKING")
print("="*70)
print(f"Demand range: {DEMAND_RANGE.start} to {DEMAND_RANGE.stop} (step {DEMAND_RANGE.step})")
print(f"Total experiments: {len(DEMAND_RANGE)} demand levels × 4 configurations\n")

csv_rows = []
experiment_count = 0
total_experiments = len(DEMAND_RANGE) * 4

for D in DEMAND_RANGE:
    demand_history.append(D)
    
    print(f"\n{'='*60}")
    print(f"Demand = {D} ({experiment_count}/{total_experiments} completed)")
    print(f"{'='*60}")
    
    print("\n[1/4] Intent 1")
    (cost_intent1, t_i1, usage1, _, graph_metrics_i1, _, _, 
     bottleneck_i1, feasible_i1) = solve_instance_gurobi_style(
        "infrastructure_intent1.json", intent1_retailers, D,
        {a: CAPACITY for a in arc_capacity_full}, apply_intent_penalty=False,
        infra_key="intent1")
    
    if cost_intent1 is None:
        usage1 = {}
        t_i1 = 0
    
    leftover_capacity1 = {a: CAPACITY - usage1.get(a, 0) for a in arc_capacity_full}
    
    print("\n[2/4] Intent 2")
    (cost_intent2, t_i2, usage2, _, graph_metrics_i2, _, _,
     bottleneck_i2, feasible_i2) = solve_instance_gurobi_style(
        "infrastructure_intent2.json", intent2_retailers, D,
        leftover_capacity1, apply_intent_penalty=False,
        infra_key="intent2")
    
    if cost_intent2 is None:
        usage2 = {}
        t_i2 = 0
    
    leftover_capacity2 = {a: leftover_capacity1[a] - usage2.get(a, 0) for a in leftover_capacity1}
    
    print("\n[3/4] Intent 3")
    (cost_intent3, t_i3, usage3, _, graph_metrics_i3, _, _,
     bottleneck_i3, feasible_i3) = solve_instance_gurobi_style(
        "infrastructure_intent3.json", intent3_retailers, D,
        leftover_capacity2, apply_intent_penalty=False,
        infra_key="intent3")
    
    if cost_intent3 is None:
        t_i3 = 0
    
    print("\n[4/4] Full infrastructure")
    (cost_full, t_full, usage_full, cost_full_per_group, graph_metrics_full, 
     violations_full, violation_costs_full, bottleneck_full, feasible_full) = solve_instance_gurobi_style(
        "infrastructure_full.json", retailers, D,
        {a: CAPACITY for a in arc_capacity_full}, apply_intent_penalty=True,
        infra_key="full")
    
    if cost_full is None:
        continue
    
    experiment_count += 4
    
    # store results with ALL metrics
    for c in retailers:
        cid = c["id"]
        intent = c["intent"]
        
        bottleneck_intent = bottleneck_i1 if intent == 1 else (bottleneck_i2 if intent == 2 else bottleneck_i3)
        
        csv_rows.append({
            "demand": D,
            "experiment": f"intent{intent}",
            "retailer_id": cid,
            "retailer_intent": intent,
            "cost": cost_intent1 if intent == 1 else (cost_intent2 if intent == 2 else cost_intent3),
            "feasible": 1 if (feasible_i1 if intent == 1 else (feasible_i2 if intent == 2 else feasible_i3)) else 0,
            "graph_nodes": graph_metrics_i1["physical_nodes"] if intent == 1 else 
                          (graph_metrics_i2["physical_nodes"] if intent == 2 else graph_metrics_i3["physical_nodes"]),
            "graph_arcs": graph_metrics_i1["physical_arcs"] if intent == 1 else 
                         (graph_metrics_i2["physical_arcs"] if intent == 2 else graph_metrics_i3["physical_arcs"]),
            "solve_time": t_i1 if intent == 1 else (t_i2 if intent == 2 else t_i3),
            "violations_intent1": "",
            "violations_intent2": "",
            "violations_intent3": "",
            "violation_cost_intent1": "",
            "violation_cost_intent2": "",
            "violation_cost_intent3": "",
            "max_utilization": bottleneck_intent["max_utilization"],
            "p95_utilization": bottleneck_intent["p95_utilization"],
            "p90_utilization": bottleneck_intent["p90_utilization"],
            "p75_utilization": bottleneck_intent["p75_utilization"],
            "active_arc_max_util": bottleneck_intent["active_arc_max_util"],
            "active_arc_avg_util": bottleneck_intent["active_arc_avg_util"],
            "num_active_arcs": bottleneck_intent["num_active_arcs"],
            "num_saturated_arcs": bottleneck_intent["num_saturated_arcs"],
            "num_near_saturated_arcs": bottleneck_intent["num_near_saturated_arcs"],
            "num_unused_arcs": bottleneck_intent["num_unused_arcs"],  
            "avg_utilization_pct": bottleneck_intent["avg_utilization_pct"],
            "avg_residual_pct": bottleneck_intent["avg_residual_pct"],
            "total_residual_capacity": bottleneck_intent["total_residual_capacity"],
            "avg_residual_capacity": bottleneck_intent["avg_residual_capacity"],
            "min_residual_capacity": bottleneck_intent["min_residual_capacity"], 
        })
        
        csv_rows.append({
            "demand": D,
            "experiment": "full",
            "retailer_id": cid,
            "retailer_intent": intent,
            "cost": cost_full,
            "feasible": 1 if feasible_full else 0,
            "graph_nodes": graph_metrics_full["physical_nodes"],
            "graph_arcs": graph_metrics_full["physical_arcs"],
            "solve_time": t_full,
            "violations_intent1": violations_full["intent1"],
            "violations_intent2": violations_full["intent2"],
            "violations_intent3": violations_full["intent3"],
            "violation_cost_intent1": violation_costs_full["intent1"],
            "violation_cost_intent2": violation_costs_full["intent2"],
            "violation_cost_intent3": violation_costs_full["intent3"],
            "max_utilization": bottleneck_full["max_utilization"],
            "p95_utilization": bottleneck_full["p95_utilization"],
            "p90_utilization": bottleneck_full["p90_utilization"],
            "p75_utilization": bottleneck_full["p75_utilization"],
            "active_arc_max_util": bottleneck_full["active_arc_max_util"],
            "active_arc_avg_util": bottleneck_full["active_arc_avg_util"],
            "num_active_arcs": bottleneck_full["num_active_arcs"],
            "num_saturated_arcs": bottleneck_full["num_saturated_arcs"],
            "num_near_saturated_arcs": bottleneck_full["num_near_saturated_arcs"],
            "num_unused_arcs": bottleneck_full["num_unused_arcs"],  
            "avg_utilization_pct": bottleneck_full["avg_utilization_pct"],
            "avg_residual_pct": bottleneck_full["avg_residual_pct"],
            "total_residual_capacity": bottleneck_full["total_residual_capacity"],
            "avg_residual_capacity": bottleneck_full["avg_residual_capacity"],
            "min_residual_capacity": bottleneck_full["min_residual_capacity"], 
        })


# ============================================================================
# IDENTIFY BOTTLENECK ARCS
# ============================================================================

print("\n" + "="*70)
print("IDENTIFYING BOTTLENECK ARCS")
print("="*70)

bottleneck_arcs = {}

for infra_key in ["intent1", "intent2", "intent3", "full"]:
    arc_max_utilization = {}
    
    for arc_key, usage_list in arc_usage_history[infra_key].items():
        if len(usage_list) > 0:
            max_usage = max(usage_list)
            utilization = (max_usage / CAPACITY) * 100
            arc_max_utilization[arc_key] = utilization
    
    sorted_arcs = sorted(arc_max_utilization.items(), key=lambda x: x[1], reverse=True)
    
    num_bottleneck = max(5, len(sorted_arcs) // 10)
    critical_arcs = [arc for arc, util in sorted_arcs[:num_bottleneck] if util > 50]
    
    bottleneck_arcs[infra_key] = critical_arcs
    
    print(f"\n{infra_key.upper()}:")
    print(f"  Total arcs: {len(arc_max_utilization)}")
    print(f"  Identified {len(critical_arcs)} bottleneck arcs")

# ============================================================================
# CALCULATE BOTTLENECK-SPECIFIC METRICS
# ============================================================================

print("\n" + "="*70)
print("CALCULATING BOTTLENECK-SPECIFIC METRICS")
print("="*70)

bottleneck_metrics_csv = []
for demand_idx, D in enumerate(demand_history):
    for infra_key in ["intent1", "intent2", "intent3", "full"]:
        bottleneck_arc_list = bottleneck_arcs[infra_key]
        
        if len(bottleneck_arc_list) == 0:
            bottleneck_avg_util = 0
            bottleneck_avg_residual_pct = 100
        else:
            bottleneck_utils = []
            for arc in bottleneck_arc_list:
                if arc in arc_usage_history[infra_key]:
                    usage = arc_usage_history[infra_key][arc][demand_idx]
                    util = (usage / CAPACITY) * 100
                    bottleneck_utils.append(util)
            
            if bottleneck_utils:
                bottleneck_avg_util = np.mean(bottleneck_utils)
                bottleneck_avg_residual_pct = 100 - bottleneck_avg_util
            else:
                bottleneck_avg_util = 0
                bottleneck_avg_residual_pct = 100
        
        # Calculate metrics for ALL arcs
        all_utils = []
        for arc, usage_list in arc_usage_history[infra_key].items():
            if demand_idx < len(usage_list):
                usage = usage_list[demand_idx]
                util = (usage / CAPACITY) * 100
                all_utils.append(util)
        
        if all_utils:
            general_avg_util = np.mean(all_utils)
            general_avg_residual_pct = 100 - general_avg_util
        else:
            general_avg_util = 0
            general_avg_residual_pct = 100
        
        bottleneck_metrics_csv.append({
            "demand": D,
            "infrastructure": infra_key,
            "num_bottleneck_arcs": len(bottleneck_arc_list),
            "bottleneck_avg_utilization_pct": bottleneck_avg_util,
            "bottleneck_avg_residual_pct": bottleneck_avg_residual_pct,
            "general_avg_utilization_pct": general_avg_util,
            "general_avg_residual_pct": general_avg_residual_pct,
        })

print("✓ Bottleneck metrics calculated for all demands and infrastructures")

# ============================================================================
# EXPORT MAIN RESULTS CSV WITH ALL COLUMNS
# ============================================================================

csv_file = "results_final.csv"
with open(csv_file, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "demand", "experiment", "retailer_id", "retailer_intent", "cost",
            "feasible", "graph_nodes", "graph_arcs", "solve_time",
            "violations_intent1", "violations_intent2", "violations_intent3",
            "violation_cost_intent1", "violation_cost_intent2", "violation_cost_intent3",
            "max_utilization", "p95_utilization", "p90_utilization", "p75_utilization",
            "active_arc_max_util", "active_arc_avg_util", "num_active_arcs",
            "num_saturated_arcs", "num_near_saturated_arcs",
            "num_unused_arcs",  
            "avg_utilization_pct", "avg_residual_pct",
            "total_residual_capacity", "avg_residual_capacity",
            "min_residual_capacity"  
        ]
    )
    writer.writeheader()
    writer.writerows(csv_rows)

print(f"\n{'='*70}")
print(f"Main results exported to: {csv_file}")
print(f"Includes ALL metrics needed for visualization")
print(f"{'='*70}")

# ============================================================================
# EXPORT BOTTLENECK-SPECIFIC METRICS CSV
# ============================================================================

bottleneck_csv_file = "bottleneck_arc_metrics.csv"
with open(bottleneck_csv_file, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "demand",
            "infrastructure",
            "num_bottleneck_arcs",
            "bottleneck_avg_utilization_pct",
            "bottleneck_avg_residual_pct",
            "general_avg_utilization_pct",
            "general_avg_residual_pct"
        ]
    )
    writer.writeheader()
    writer.writerows(bottleneck_metrics_csv)

print(f"✓ Bottleneck-specific metrics exported to: {bottleneck_csv_file}")
print(f"{'='*70}")

# ============================================================================
# EXPORT DETAILED BOTTLENECK ARC LIST
# ============================================================================

bottleneck_arc_details = []
for infra_key, arc_list in bottleneck_arcs.items():
    for rank, arc in enumerate(arc_list, 1):
        usage_list = arc_usage_history[infra_key][arc]
        
        if len(usage_list) > 0:
            max_usage = max(usage_list)
            avg_usage = np.mean(usage_list)
            max_util = (max_usage / CAPACITY) * 100
            avg_util = (avg_usage / CAPACITY) * 100
            
            times_saturated = sum(1 for u in usage_list if (u / CAPACITY * 100) > 95)
            
            bottleneck_arc_details.append({
                "infrastructure": infra_key,
                "rank": rank,
                "arc_from": arc[0],
                "arc_to": arc[1],
                "max_usage": max_usage,
                "avg_usage": avg_usage,
                "max_utilization_pct": max_util,
                "avg_utilization_pct": avg_util,
                "times_saturated": times_saturated,
                "saturation_frequency_pct": (times_saturated / len(usage_list)) * 100
            })

bottleneck_details_file = "bottleneck_arc_details.csv"
with open(bottleneck_details_file, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "infrastructure", "rank", "arc_from", "arc_to",
            "max_usage", "avg_usage", "max_utilization_pct", "avg_utilization_pct",
            "times_saturated", "saturation_frequency_pct"
        ]
    )
    writer.writeheader()
    writer.writerows(bottleneck_arc_details)

print(f"✓ Detailed bottleneck arc information exported to: {bottleneck_details_file}")
print(f"{'='*70}")

# ============================================================================
# EXPORT PER-ARC TIME SERIES DATA
# ============================================================================

print("\nExporting per-arc time series data...")

arc_timeseries_file = "arc_utilization_timeseries.csv"
arc_timeseries_rows = []

for infra_key in ["intent1", "intent2", "intent3", "full"]:
    is_bottleneck_set = set(bottleneck_arcs[infra_key])
    
    for arc, usage_list in arc_usage_history[infra_key].items():
        for demand_idx, D in enumerate(demand_history):
            if demand_idx < len(usage_list):
                usage = usage_list[demand_idx]
                utilization = (usage / CAPACITY) * 100
                residual = CAPACITY - usage
                residual_pct = (residual / CAPACITY) * 100
                
                arc_timeseries_rows.append({
                    "demand": D,
                    "infrastructure": infra_key,
                    "arc_from": arc[0],
                    "arc_to": arc[1],
                    "is_bottleneck": 1 if arc in is_bottleneck_set else 0,
                    "usage": usage,
                    "utilization_pct": utilization,
                    "residual_capacity": residual,
                    "residual_pct": residual_pct
                })

with open(arc_timeseries_file, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "demand", "infrastructure", "arc_from", "arc_to", "is_bottleneck",
            "usage", "utilization_pct", "residual_capacity", "residual_pct"
        ]
    )
    writer.writeheader()
    writer.writerows(arc_timeseries_rows)

print(f"✓ Per-arc time series data exported to: {arc_timeseries_file}")
print(f"  (Contains {len(arc_timeseries_rows)} arc-demand observations)")

# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

print("\n" + "="*70)
print("BOTTLENECK ANALYSIS SUMMARY")
print("="*70)

for infra_key in ["intent1", "intent2", "intent3", "full"]:
    print(f"\n{infra_key.upper()} Infrastructure:")
    
    num_bottleneck = len(bottleneck_arcs[infra_key])
    total_arcs = len(arc_usage_history[infra_key])
    
    print(f"  Total arcs: {total_arcs}")
    print(f"  Bottleneck arcs identified: {num_bottleneck} ({num_bottleneck/total_arcs*100:.1f}%)")
    
    final_demand_metrics = [m for m in bottleneck_metrics_csv 
                           if m["infrastructure"] == infra_key]
    
    if final_demand_metrics:
        bottleneck_utils = [m["bottleneck_avg_utilization_pct"] for m in final_demand_metrics]
        general_utils = [m["general_avg_utilization_pct"] for m in final_demand_metrics]
        
        print(f"\n  Bottleneck Arcs Statistics:")
        print(f"    Average utilization: {np.mean(bottleneck_utils):.1f}%")
        print(f"    Peak utilization: {max(bottleneck_utils):.1f}%")
        print(f"    Final demand utilization: {bottleneck_utils[-1]:.1f}%")
        
        print(f"\n  General (All Arcs) Statistics:")
        print(f"    Average utilization: {np.mean(general_utils):.1f}%")
        print(f"    Peak utilization: {max(general_utils):.1f}%")
        print(f"    Final demand utilization: {general_utils[-1]:.1f}%")
        
        avg_gap = np.mean(bottleneck_utils) - np.mean(general_utils)
        print(f"\n  Bottleneck vs General Gap: {avg_gap:.1f}% higher")
        
        if avg_gap > 20:
            print(f"    → Significant concentration: Few arcs carry most load")
        elif avg_gap > 10:
            print(f"    → Moderate concentration: Some load imbalance")
        else:
            print(f"    → Balanced distribution: Load spread across arcs")

print("\n" + "="*70)
print("📊 EXPORT COMPLETE")
print("="*70)
print("Generated 4 CSV files:")
print(f"  1. {csv_file}")
print(f"     - Main experimental results with ALL metrics for plotting")
print(f"  2. {bottleneck_csv_file}")
print(f"     - Bottleneck vs general utilization for all demands")
print(f"  3. {bottleneck_details_file}")
print(f"     - Detailed statistics for each identified bottleneck arc")
print(f"  4. {arc_timeseries_file}")
print(f"     - Complete time series for every arc across all demands")
print("\n" + "="*70)
print(" ALL DATA EXPORTED - READY FOR VISUALIZATION!")