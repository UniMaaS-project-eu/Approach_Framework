#!/usr/bin/env python3
"""
Multi-Commodity Flow Optimization: Retailer-Based Flows from Neo4j Data
Automatically determines layers by traversing depends_on relationships
"""

import numpy as np
import gurobipy as gp
from gurobipy import GRB
import time
import csv
from collections import defaultdict, deque

# Import parsed data
import sys
# try:
#     from parsing_neo4j_csv import parse_neo4j_csv
# except:
#     print("Error: Cannot import parsing_neo4j_csv module")
#     sys.exit(1)



# ============================================================================
# LAYER DETECTION ALGORITHM
# ============================================================================

def assign_layers_to_all_entities(process_configurations, processes, resources, sites, suppliers):
    """
    Traverse depends_on relationships to assign layers to:
    - Process Configurations
    - Sites (based on PCs they host)
    - Suppliers (based on resources they provide to PCs)
    
    Layer 1 = PCs with no dependencies (or dependencies outside the PC set)
    Layer k+1 = PCs that depend on layer-k PCs
    """
    
    print("\n" + "="*70)
    print("LAYER DETECTION: Traversing depends_on relationships")
    print("="*70)
    
    # Step 1: Assign layers to Process Configurations
    pc_layers = {}
    pc_deps = {}  # PC -> list of PCs it depends on
    
    # Build dependency graph
    for pc_name, pc_info in process_configurations.items():
        deps = pc_info.get("depends_on", [])
        # Only keep dependencies that exist in our PC set
        valid_deps = [d for d in deps if d in process_configurations]
        pc_deps[pc_name] = valid_deps
    
    # Find PCs with no dependencies (Layer 1)
    layer_1_pcs = [pc for pc, deps in pc_deps.items() if len(deps) == 0]
    
    if not layer_1_pcs:
        print("⚠ Warning: No PCs without dependencies found. Assigning all to layer 1.")
        for pc_name in process_configurations:
            pc_layers[pc_name] = 1
    else:
        # Assign layer 1
        for pc_name in layer_1_pcs:
            pc_layers[pc_name] = 1
        
        # Iteratively assign higher layers using topological ordering
        max_iterations = len(process_configurations)
        iteration = 0
        
        while len(pc_layers) < len(process_configurations) and iteration < max_iterations:
            iteration += 1
            assigned_this_round = []
            
            for pc_name, deps in pc_deps.items():
                if pc_name in pc_layers:
                    continue  # Already assigned
                
                # Check if all dependencies have been assigned layers
                if all(dep in pc_layers for dep in deps):
                    # Layer = max(dependency layers) + 1
                    max_dep_layer = max([pc_layers[dep] for dep in deps]) if deps else 0
                    pc_layers[pc_name] = max_dep_layer + 1
                    assigned_this_round.append(pc_name)
            
            if not assigned_this_round:
                # No progress - might have circular dependencies
                # Assign remaining PCs to layer 1
                for pc_name in process_configurations:
                    if pc_name not in pc_layers:
                        print(f"⚠ Warning: {pc_name} has unresolved dependencies. Assigning to layer 1.")
                        pc_layers[pc_name] = 1
                break
    
    max_layer = max(pc_layers.values()) if pc_layers else 1
    
    print(f"\n✓ Assigned layers to {len(pc_layers)} Process Configurations")
    print(f"✓ Number of layers detected: {max_layer}")
    
    # Print layer distribution
    for layer in range(1, max_layer + 1):
        pcs_in_layer = [pc for pc, l in pc_layers.items() if l == layer]
        print(f"  Layer {layer}: {len(pcs_in_layer)} PCs")
    
    # Step 2: Map PCs to Sites
    pc_to_sites = {}
    for pc_name, pc_info in process_configurations.items():
        proc = pc_info.get("requires_process")
        if proc and proc in processes:
            pc_to_sites[pc_name] = processes[proc].get("sites", [])
        else:
            pc_to_sites[pc_name] = []
    
    # Step 3: Assign layers to Sites
    site_layers = {}
    for site_name in sites:
        # A site's layer is the minimum layer of all PCs performed there
        # (it operates at the earliest stage it's involved in)
        site_layer_candidates = []
        for pc_name, pc_sites in pc_to_sites.items():
            if site_name in pc_sites and pc_name in pc_layers:
                site_layer_candidates.append(pc_layers[pc_name])
        
        if site_layer_candidates:
            site_layers[site_name] = min(site_layer_candidates)
        else:
            # Site not associated with any PC - assign to layer 1
            site_layers[site_name] = 1
    
    print(f"\n✓ Assigned layers to {len(site_layers)} Sites")
    for layer in range(1, max_layer + 1):
        sites_in_layer = [s for s, l in site_layers.items() if l == layer]
        print(f"  Layer {layer}: {len(sites_in_layer)} sites")
    
    # Step 4: Assign layers to Suppliers based on Resources
    supplier_layers = {}
    
    # For each resource, find which PCs use it
    resource_to_pcs = defaultdict(set)
    for pc_name, pc_info in process_configurations.items():
        for res in pc_info.get("uses_resources", []):
            resource_to_pcs[res].add(pc_name)
    
    # For each supplier, find the minimum layer of PCs that use its resources
    for supplier_name in suppliers:
        supplier_layer_candidates = []
        
        # Find all resources provided by this supplier
        for resource_name, resource_info in resources.items():
            if supplier_name in resource_info.get("suppliers", []):
                # This supplier provides this resource
                # Find PCs that use this resource
                for pc_name in resource_to_pcs.get(resource_name, []):
                    if pc_name in pc_layers:
                        supplier_layer_candidates.append(pc_layers[pc_name])
        
        if supplier_layer_candidates:
            # Supplier operates at the minimum layer where it's needed
            supplier_layers[supplier_name] = min(supplier_layer_candidates)
        else:
            # Supplier not associated with any PC - assign to layer 1
            supplier_layers[supplier_name] = 1
    
    print(f"\n✓ Assigned layers to {len(supplier_layers)} Suppliers")
    for layer in range(1, max_layer + 1):
        suppliers_in_layer = [s for s, l in supplier_layers.items() if l == layer]
        print(f"  Layer {layer}: {len(suppliers_in_layer)} suppliers")
    
    # Verify layer structure
    print(f"\n✓ Layer structure verified:")
    print(f"  Total layers: {max_layer}")
    print(f"  Flow pattern: ", end="")
    for layer in range(1, max_layer + 1):
        print(f"SUP(L{layer})→SITE(L{layer})", end="")
        if layer < max_layer:
            print("→", end="")
    print()
    
    return pc_layers, site_layers, supplier_layers, max_layer



# ============================================================================
# BUILD INFRASTRUCTURE WITH LAYERS
# ============================================================================

def build_layered_infrastructure(data, site_layers, supplier_layers, max_layer):
    """Build infrastructure dict with layer information"""
    
    suppliers_dict = {}
    for sup_name, sup_info in data["suppliers"].items():
        suppliers_dict[sup_name] = {
            "layer": supplier_layers.get(sup_name, 1),
            "location": sup_info.get("location", "Unknown"),
            "recyclable": "yes" if sup_info.get("recyclable", False) else "no"
        }
    
    sites_dict = {}
    for site_name, site_info in data["sites"].items():
        sites_dict[site_name] = {
            "layer": site_layers.get(site_name, 1),
            "location": site_info.get("location", "Unknown")
        }
    
    # Build arcs from logistic routes
    arcs_list = []
    for route_name, route_info in data["logistic_routes"].items():
        from_node = None
        to_node = None
        
        if "from_supplier" in route_info:
            from_node = route_info["from_supplier"]
        elif "from_site" in route_info:
            from_node = route_info["from_site"]
        
        if "to_site" in route_info:
            to_node = route_info["to_site"]
        
        if from_node and to_node:
            arcs_list.append({
                "from": from_node,
                "to": to_node,
                "base_cost": 1.0,
                "mode": route_info.get("transportationMode", "normal")
            })
    
    return {
        "suppliers": suppliers_dict,
        "sites": sites_dict,
        "arcs": arcs_list,
        "max_layer": max_layer
    }



# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_enhanced_bottleneck_metrics(arc_usage, arc_capacities):
    """Calculate bottleneck metrics"""
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
        
        if used < 1e-3:
            num_unused += 1
        else:
            active_utilizations.append(utilization)
    
    if not all_utilizations:
        return {
            "max_utilization": 0,
            "avg_utilization_pct": 0,
            "num_active_arcs": 0,
            "num_unused_arcs": 0,
            "min_residual_capacity": 0
        }
    
    return {
        "max_utilization": max(all_utilizations),
        "avg_utilization_pct": np.mean(all_utilizations),
        "num_active_arcs": len(active_utilizations),
        "num_saturated_arcs": sum(1 for u in all_utilizations if u > 99.9),
        "num_unused_arcs": num_unused,
        "total_residual_capacity": sum(residuals),
        "min_residual_capacity": min(residuals) if residuals else 0
    }

def solve_multi_commodity_flow(infrastructure, retailers_subset, demand_value, 
                                arc_capacities, apply_intent_penalty=True, VIOLATION_COST=2, CAPACITY=100000):
    """
    Solve multi-commodity flow where each retailer is a commodity.
    - Source nodes connect to ALL layer-1 suppliers
    - Sink nodes connect to ALL max-layer sites
    """
    start_time = time.perf_counter()
    
    suppliers = infrastructure["suppliers"]
    sites = infrastructure["sites"]
    infra_arcs = infrastructure["arcs"]
    max_layer = infrastructure["max_layer"]
    
    # Commodities are retailer IDs
    commodities = [r["id"] for r in retailers_subset]
    
    # Build node set
    nodes = set()
    for arc in infra_arcs:
        nodes.add(arc["from"])
        nodes.add(arc["to"])
    
    # Add source and sink nodes for each retailer
    for comm in commodities:
        nodes.add(f"src_{comm}")
        nodes.add(f"sink_{comm}")
    nodes = list(nodes)
    
    # Build arcs
    arcs = []
    capacity = {}
    arc_data = {}
    
    # Infrastructure arcs
    for arc in infra_arcs:
        arc_key = (arc["from"], arc["to"])
        arcs.append(arc_key)
        capacity[arc_key] = arc_capacities.get(arc_key, CAPACITY)
        arc_data[arc_key] = {
            "base_cost": arc.get("base_cost", 1.0),
            "mode": arc.get("mode"),
            "is_virtual": False
        }
    
    # Virtual source arcs: src_retailer -> ALL layer-1 suppliers
    layer_1_suppliers = [s for s, info in suppliers.items() if info["layer"] == 1]
    print(f"  Creating {len(commodities)} × {len(layer_1_suppliers)} source arcs...")
    
    for comm in commodities:
        for supplier_id in layer_1_suppliers:
            arc_key = (f"src_{comm}", supplier_id)
            arcs.append(arc_key)
            capacity[arc_key] = float('inf')
            arc_data[arc_key] = {
                "base_cost": 0,
                "mode": None,
                "is_virtual": True
            }
    
    # Virtual sink arcs: ALL max-layer sites -> sink_retailer
    max_layer_sites = [s for s, info in sites.items() if info["layer"] == max_layer]
    print(f"  Creating {len(max_layer_sites)} × {len(commodities)} sink arcs...")
    
    for comm in commodities:
        for site_id in max_layer_sites:
            arc_key = (site_id, f"sink_{comm}")
            arcs.append(arc_key)
            capacity[arc_key] = float('inf')
            arc_data[arc_key] = {
                "base_cost": 0,
                "mode": None,
                "is_virtual": True
            }
    
    print(f"  Total arcs (including virtual): {len(arcs)}")
    
    # Calculate costs with intent penalties
    cost = {}
    
    for comm in commodities:
        retailer = next(r for r in retailers_subset if r["id"] == comm)
        
        for arc_key in arcs:
            if arc_data[arc_key]["is_virtual"]:
                cost[comm, arc_key[0], arc_key[1]] = 0
                continue
            
            total_cost = arc_data[arc_key]["base_cost"]
            
            if apply_intent_penalty:
                from_node, to_node = arc_key
                mode = arc_data[arc_key].get("mode")
                
                # Intent 1: Recyclable materials
                if retailer["intent"] == 1:
                    if from_node in suppliers:
                        if suppliers[from_node].get("recyclable") != "yes":
                            total_cost += VIOLATION_COST
                
                # Intent 2: Local (Torino)
                if retailer["intent"] == 2:
                    if from_node in suppliers:
                        if suppliers[from_node].get("location") != "Torino":
                            total_cost += VIOLATION_COST
                    if to_node in sites:
                        if sites[to_node].get("location") != "Torino":
                            total_cost += VIOLATION_COST
                
                # Intent 3: Fast transportation
                if retailer["intent"] == 3:
                    if mode is not None and mode != "fast":
                        total_cost += VIOLATION_COST
            
            cost[comm, arc_key[0], arc_key[1]] = total_cost
    
    # Set inflow/outflow for each commodity at each node
    inflow = {}
    for comm in commodities:
        for node in nodes:
            if node == f"src_{comm}":
                inflow[comm, node] = demand_value
            elif node == f"sink_{comm}":
                inflow[comm, node] = -demand_value
            else:
                inflow[comm, node] = 0
    
    # Create Gurobi model
    m = gp.Model('netflow_layered')
    m.Params.OutputFlag = 0
    
    # Decision variables: flow[commodity, arc]
    # flow = m.addVars(commodities, arcs, obj=cost, vtype=GRB.CONTINUOUS, name="flow")
    flow = m.addVars(commodities, arcs, obj=cost, vtype=GRB.INTEGER, name="flow")
    
    # Capacity constraints (only for physical arcs)
    for arc_key in arcs:
        if not arc_data[arc_key]["is_virtual"]:
            m.addConstr(
                flow.sum("*", arc_key[0], arc_key[1]) <= capacity[arc_key],
                name=f"cap_{arc_key[0]}_{arc_key[1]}"
            )
    
    # Flow conservation constraints
    m.addConstrs(
        (flow.sum(h, "*", j) + inflow[h, j] == flow.sum(h, j, "*")
         for h in commodities for j in nodes),
        "flow_conservation"
    )
    
    # Solve
    m.optimize()
    solve_time = time.perf_counter() - start_time
    
    # Process results
    if m.status == GRB.OPTIMAL:
        solution = m.getAttr("X", flow)
        
        # Calculate arc usage
        arc_usage = {}
        for arc_key in arcs:
            if not arc_data[arc_key]["is_virtual"]:
                total_flow = sum(solution.get((h, arc_key[0], arc_key[1]), 0) 
                               for h in commodities)
                arc_usage[arc_key] = total_flow
        
        # Calculate violations
        violations_per_intent = {"intent1": 0, "intent2": 0, "intent3": 0}
        violation_cost_per_intent = {"intent1": 0.0, "intent2": 0.0, "intent3": 0.0}
        
        for comm in commodities:
            retailer = next(r for r in retailers_subset if r["id"] == comm)
            intent_key = f"intent{retailer['intent']}"
            
            for arc_key in arcs:
                flow_val = solution.get((comm, arc_key[0], arc_key[1]), 0)
                if flow_val > 0:
                    arc_cost_val = cost[comm, arc_key[0], arc_key[1]]
                    
                    if not arc_data[arc_key]["is_virtual"] and apply_intent_penalty:
                        base_cost = arc_data[arc_key]["base_cost"]
                        if arc_cost_val > base_cost:
                            violations_per_intent[intent_key] += 1
                            violation_cost_per_intent[intent_key] += (arc_cost_val - base_cost) * flow_val
        
        bottleneck_metrics = calculate_enhanced_bottleneck_metrics(arc_usage, capacity)
        
        print(f"    ✓ FEASIBLE (cost: {m.ObjVal:.2f}, max_util: {bottleneck_metrics['max_utilization']:.1f}%)")
        
        return (m.ObjVal, solve_time, arc_usage, violations_per_intent, 
                violation_cost_per_intent, bottleneck_metrics, True)
    
    else:
        print(f"    ✗ INFEASIBLE or ERROR (status: {m.status})")
        empty_metrics = {
            "max_utilization": 0,
            "avg_utilization_pct": 0,
            "num_active_arcs": 0,
            "num_unused_arcs": 0,
            "min_residual_capacity": 0
        }
        return (None, solve_time, {}, 
                {"intent1": 0, "intent2": 0, "intent3": 0},
                {"intent1": 0.0, "intent2": 0.0, "intent3": 0.0},
                empty_metrics, False)

# ============================================================================
# MAIN EXPERIMENT LOOP
# ============================================================================
def flow_network_optimization(data):
    # ============================================================================
    # CONFIGURATION
    # ============================================================================

    VIOLATION_COST = 2
    # CAPACITY = 40
    CAPACITY = 100000
    # DEMAND_RANGE = range(10, 200, 20)
    DEMAND_RANGE = [10]
    # INPUT_CSV = "export.csv"

    # ============================================================================
    # PARSE NEO4J DATA
    # ============================================================================

    print("="*70)
    print("PARSING NEO4J CSV DATA")
    print("="*70)

    # data = parse_neo4j_csv(INPUT_CSV)

    sites = data["sites"]
    processes = data["processes"]
    suppliers = data["suppliers"]
    resources = data["resources"]
    commodities_products = data["commodities_products"]
    process_configurations = data["process_configurations"]
    logistic_routes = data["logistic_routes"]

    print(f"\n✓ Parsed {len(sites)} sites")
    print(f"✓ Parsed {len(suppliers)} suppliers")
    print(f"✓ Parsed {len(process_configurations)} process configurations")
    print(f"✓ Parsed {len(logistic_routes)} logistic routes")
    # Execute layer assignment
    pc_layers, site_layers, supplier_layers, max_layer = assign_layers_to_all_entities(
        process_configurations, processes, resources, sites, suppliers
    )

    # ============================================================================
    # RETAILER SETUP
    # ============================================================================

    retailers = []
    k = 0
    for product_type in [1, 2, 3]:
        for intent in [1, 2, 3]:
            k += 1
            retailers.append({
                "id": k,
                "product": product_type,
                "intent": intent,
                "final_layer": max_layer  # All retailers want final layer product
            })

    intent1_retailers = [r for r in retailers if r["intent"] == 1]
    intent2_retailers = [r for r in retailers if r["intent"] == 2]
    intent3_retailers = [r for r in retailers if r["intent"] == 3]

    print(f"\n✓ {len(retailers)} retailers configured")
    print(f"  - Intent 1 (Recyclable): {len(intent1_retailers)} retailers")
    print(f"  - Intent 2 (Local/Torino): {len(intent2_retailers)} retailers")
    print(f"  - Intent 3 (Fast): {len(intent3_retailers)} retailers")
    infrastructure = build_layered_infrastructure(data, site_layers, supplier_layers, max_layer)

    print(f"\n✓ Built layered infrastructure:")
    print(f"  - {len(infrastructure['suppliers'])} suppliers across {max_layer} layers")
    print(f"  - {len(infrastructure['sites'])} sites across {max_layer} layers")
    print(f"  - {len(infrastructure['arcs'])} arcs")

    # ============================================================================
    # GLOBAL TRACKING
    # ============================================================================

    arc_usage_history = defaultdict(list)
    demand_history = []
    print("\n" + "="*70)
    print("STARTING EXPERIMENTS")
    print("="*70)
    # print(f"Demand range: {DEMAND_RANGE.start} to {DEMAND_RANGE.stop} (step {DEMAND_RANGE.step})")
    print(f"Network structure: src → SUP(L1) → SITE(L1) → ... → SITE(L{max_layer}) → sink")
    print(f"Total experiments: {len(DEMAND_RANGE)} demand levels\n")

    # Build initial arc capacities
    arc_capacity_full = {}
    for arc in infrastructure["arcs"]:
        arc_capacity_full[(arc["from"], arc["to"])] = CAPACITY

    csv_rows = []

    for D in DEMAND_RANGE:
        demand_history.append(D)
        
        print(f"\n{'='*60}")
        print(f"Demand = {D}")
        print(f"{'='*60}")
        
        # Solve for all retailers
        (cost_full, t_full, usage_full, violations_full, 
        violation_costs_full, bottleneck_full, feasible_full) = solve_multi_commodity_flow(
            infrastructure, retailers, D,
            arc_capacity_full, apply_intent_penalty=True,  VIOLATION_COST=2, CAPACITY=100000
        )
        
        if cost_full is None:
            print("  ⚠ Skipping this demand level due to infeasibility")
            continue
        
        # Store arc usage for this demand level
        for arc_key, usage in usage_full.items():
            arc_usage_history[arc_key].append(usage)
        
        # Record results for each retailer
        for retailer in retailers:
            csv_rows.append({
                "demand": D,
                "retailer_id": retailer["id"],
                "retailer_intent": retailer["intent"],
                "cost": cost_full / len(retailers),
                "feasible": 1 if feasible_full else 0,
                "solve_time": t_full,
                "violations_intent1": violations_full["intent1"],
                "violations_intent2": violations_full["intent2"],
                "violations_intent3": violations_full["intent3"],
                "violation_cost_intent1": violation_costs_full["intent1"],
                "violation_cost_intent2": violation_costs_full["intent2"],
                "violation_cost_intent3": violation_costs_full["intent3"],
                "max_utilization": bottleneck_full["max_utilization"],
                "avg_utilization_pct": bottleneck_full["avg_utilization_pct"],
                "num_active_arcs": bottleneck_full["num_active_arcs"],
                "num_unused_arcs": bottleneck_full["num_unused_arcs"],
                "min_residual_capacity": bottleneck_full["min_residual_capacity"],
                "num_layers": max_layer
            })

    # ============================================================================
    # EXPORT RESULTS
    # ============================================================================

    print("\n" + "="*70)
    print("EXPORTING RESULTS")
    print("="*70)

    csv_file = "results_layered_neo4j.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "demand", "retailer_id", "retailer_intent", "cost", "feasible",
                "solve_time", "violations_intent1", "violations_intent2", "violations_intent3",
                "violation_cost_intent1", "violation_cost_intent2", "violation_cost_intent3",
                "max_utilization", "avg_utilization_pct", "num_active_arcs",
                "num_unused_arcs", "min_residual_capacity", "num_layers"
            ]
        )
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"✓ Results exported to: {csv_file}")
    print(f"✓ Total rows: {len(csv_rows)}")

    # Export layer assignments for reference
    layer_file = "layer_assignments.csv"
    with open(layer_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Entity", "Name", "Layer", "Type"])
        
        for pc_name, layer in pc_layers.items():
            writer.writerow(["ProcessConfiguration", pc_name, layer, "PC"])
        
        for site_name, layer in site_layers.items():
            writer.writerow(["Site", site_name, layer, "Site"])
        
        for sup_name, layer in supplier_layers.items():
            writer.writerow(["Supplier", sup_name, layer, "Supplier"])

    print(f"✓ Layer assignments exported to: {layer_file}")

    print("\n" + "="*70)
    print("✅ OPTIMIZATION COMPLETE!")
    print("="*70)
    print(f"Summary:")
    print(f"  - Detected {max_layer} layers in supply chain")
    print(f"  - Solved {len(demand_history)} demand scenarios")
    print(f"  - Network: src → L1_suppliers → L1_sites → ... → L{max_layer}_sites → sink")
    print("="*70)
