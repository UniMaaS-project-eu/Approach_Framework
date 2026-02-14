# Multi-Commodity Network Flow Optimization for Supply Chain Management



## Approach Main Experiments
### Problem Summary 
This project implements a **multi-commodity network flow (MCF) optimization system** for multi-echelon supply chain management using integer linear programming. It simulates a three-layer (multi-echelon) supply chain where multiple customers (retailers) with different operational constraints (intents) are considered as distinct commodities competing for limited shared infrastructure capacity. 
#### Supply Chain Structure
The network consists of **three echelons** (layers) and each layer has two sublayers:
- **Sublayer A**: Suppliers of that layer
- **Sublayer B**: Manufacturing sites
Products: Three distinct intermediary products routed through the network

We consider three intent types:
- **Intent 1**: Eco-Conscious --> Must use recyclable suppliers
- **Intent 2**: Locality Constrained --> Must use providers (sites and suppliers) located in the Torino region
- **Intent 3**: Speed Optimized --> Must use fast transportation Logistic Routes 
#### Core Optimization Challenge
Each arc (Logistic Route) has a limited capacity (40 units). All 9 retailers (3 products × 3 intents) share the same infrastructure. Sometimes it's impossible to satisfy all intent constraints. When an intent-specific infrastructure subnetwork (that guarantees constraint satisfaction for the corresponding intent type) becomes saturated (infeasible), then alternative intent-violating routes of the full infrastructure are allowed, incurring intent-violation penalties, which are minimized.

**Goal**: Decide how to route demand through the network to minimize total cost while respecting (or strategically violating) intent constraints under varying demand scenarios.

### Script 1: Infrastructure Generation
**File**: `generate_infrastructure.py`
#### Purpose
Generates network topology representing a three-layer (multi-echelon) supply chain and extracts Intent-specific subgraphs that enable zero-violation constraint satisfaction when feasible (for the respective intent-specific retailers).
#### What It Does
1. Creates 60 Network Nodes:   
    - 30 suppliers (3 layers × 10 suppliers per layer)   
    - 30 sites (3 layers × 10 sites per layer)
2. Node Attributes:   
    - Location: "Torino" (≥1 guaranteed per layer) or "Other" (randomly assigned)   
    - Recyclability (suppliers only): "yes" (50%) or "no" (50%)   
    - Layer Assignment: 1, 2, or 3
3. Generates ~3,000 Arcs (logistic routes):   
    - Within-layer arcs: Full connectivity from suppliers to sites in each layer and "fast" with probability 0.55, else "slow."  
    - Cross-layer arcs: Sites in layer $i$ to suppliers in layer $i+1$ (fast transport guaranteed)   
    - Arc Attributes:          
        - `mode`: "fast" (55%) or "slow" (45%)     
        - `capacity`: 40 units
4. Extracts 3 Intent-Specific Subgraphs:   
    - Intent 1 Subgraph (Eco-Conscious): Includes only suppliers with `recyclable = "yes"`   
    - Intent 2 Subgraph (Torino-Only): Includes only nodes (suppliers and sites) with `location = "Torino"`   
    - Intent 3 Subgraph (Speed-Optimized): Includes only arcs with `mode = "fast"`   
#### Input Parameters
- NUM_LAYERS = 3 (Supply chain MSC length)
- OPTIONS = 10   (Nodes per layer per type)
- RANDOM_SEED = 42   
### Outputs
4 JSON files with complete network specifications                            
- infrastructure_full.json         
- infrastructure_intent1.json       
- infrastructure_intent2.json       
- infrastructure_intent3.json

### Script 2: Experiment Runner
**File**: `netflow_run_experiments.py`
#### Purpose
Runs demand-scaling experiments by solving multi-commodity min-cost flow problems
#### What it does:
1) Creates 9 retailers (product 1-3 × intent 1-3)
2) For each of 34 demand levels (10 to 1670 units in steps of 50), it solves 4 separate optimization (MCF) problems:
- Intent 1 retailers on infrastructure_intent1.json (no violations allowed).
- Intent 2 retailers on infrastructure_intent2.json (no violations allowed), with residual capacities after step 1.
- Intent 3 retailers on infrastructure_intent3.json (no violations allowed), with leftover capacities after steps 1–2.
- All 9 retailers simultaneously on infrastructure_full.json (violations allowed + penalized).
4) Computes comprehensive metrics:
#### MCF Problem Formulation
For each commodity k:
- Adds a virtual source node, src_k, connected to all layer-1 suppliers with infinite capacity.
- Adds a virtual sink node, sink_k, connected from all sites in the retailer’s product layer with infinite capacity.

Minimize: Σ(cost_kij × flow_kij) for all commodities k and arcs (i,j)

Where:
- flow_kij integer flow of commodity k on arc (i,j)
- cost_kij = base_cost[intent_k] + violation_penalty * (constraint not met boolean), Intent 1 base cost = 4,  Intent 2 base cost = 3, Intent 3 base cost = 2, Violation penalties = 2 

Subject to:  
- Flow conservation: Σ(flow out) - Σ(flow in) = demand/supply
- Arc capacity: Σ(flow_k on arc) ≤ 40 units
- Non-negativity: flow ≥ 0
### Outputs
4 results CSV files 
 

## Requirements
- Python 3.10.12
- NumPy 2.2.6
- Gurobipy 12.0.0
- Standard libraries: json, csv, time, collections, random

## Quick Start
1) python generate_infrastructure.py
2) python netflow_run_experiments.py
