#!/usr/bin/env python3
# build_data_from_csv.py
# Reads export.csv (comma-separated, Neo4j (n)-[r]->(m) triples)
# Builds Python dictionaries: sites, processes, suppliers, resources,
# commodities_products, process_configurations, logistic_routes
# and writes them to data.py

import csv
import re
from collections import OrderedDict, defaultdict
import ast

# INPUT = "neo4j_query_table_data_2025-12-3.csv"
OUTPUT_PY = "data.py"

# Matches: <Node element_id='...' labels=frozenset({'A','B'}) properties={...}>
NODE_RE = re.compile(
    r"<Node .*?labels=frozenset\((\{.*?\})\).*?properties=(\{.*?\})>",
    re.DOTALL
)

# Matches: <Relationship ... type='DEPENDS_ON' properties={}>
REL_RE = re.compile(
    r"<Relationship .*? type='([^']+)'",
    re.DOTALL
)

prop_kv_re = re.compile(r"([A-Za-z0-9_]+)\s*:\s*([^,}]+)")

def normalize_field(s):
    """Strip outer quotes and whitespace."""
    if s is None:
        return ""
    s = s.strip()
    if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        s = s[1:-1]
    return s.strip()


def parse_node_field(text):
    """
    Parse Neo4j Python driver Node repr, for example:
    <Node ... labels=frozenset({'Process'}) properties={'name': 'X', 'location': 'Y'}>
    Returns: (labels_list, props_dict)
    """
    if not text:
        return [], {}

    text = text.strip().strip('"').strip("'")

    m = NODE_RE.search(text)
    if not m:
        return [], {}

    # Labels
    labels_raw = m.group(1)  # "{'ProcessConfiguration'}"
    try:
        labels = list(ast.literal_eval(labels_raw))
    except Exception:
        labels = []

    # Properties
    props_raw = m.group(2)  # "{'name': 'PC_...', 'run_id': '...'}"
    try:
        props = ast.literal_eval(props_raw)
    except Exception:
        props = {}

    return labels, props

# def parse_rel_field(text):
#     if not text:
#         return None
#     text = normalize_field(text)
#     m = rel_re.search(text)
#     return m.group(1) if m else None

def parse_rel_field(text):
    """
    Parse Neo4j Python driver Relationship repr, for example:
    <Relationship ... type='DEPENDS_ON' properties={}>
    Returns: 'DEPENDS_ON'
    """
    if not text:
        return None

    text = text.strip().strip('"').strip("'")
    m = REL_RE.search(text)

    if not m:
        return None

    return m.group(1)

def pretty_dict(d, indent=0):
    return repr(d)

def parse_neo4j_csv(input_file):
    sites = OrderedDict()
    processes = OrderedDict()       # name -> {"sites": [..]}
    suppliers = OrderedDict()
    resources = OrderedDict()       # name -> {"suppliers": [...]}
    commodities_products = []       # list, keep order
    commod_set = set()
    process_configurations = OrderedDict()  # name -> dict
    logistic_routes = OrderedDict() # route_name -> dict (from_site, to_site, from_supplier, props)

    # Temporary structures
    route_nodes = {}   # route_name -> props and partial fields
    pc_temp = defaultdict(lambda: {"requires_process": None, "uses_resources": set(), "outputs": set(), "depends_on": set()})
    process_performed_sites = defaultdict(set)  # Process -> set of sites

    # Read CSV robustly (commas, quoted fields)
    with open(input_file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)  # default: comma delimiter, handles quoted fields
        for row in reader:
            if not row:
                continue
            # skip header line if present
            # look for a header starting with n or "n"
            first = row[0].strip().lower()
            if first == "n" or first == '"n"' or first.startswith("n\tr\tm"):
                continue

            # Expect at least 3 columns; if more, take first 3
            if len(row) < 3:
                # skip malformed lines but print for debug
                print("Skipping malformed line (not 3 cols):", row)
                continue
            n_field = row[0]
            r_field = row[1]
            m_field = row[2]

            n_field = normalize_field(n_field)
            r_field = normalize_field(r_field)
            m_field = normalize_field(m_field)

            n_labels, n_props = parse_node_field(n_field)
            m_labels, m_props = parse_node_field(m_field)
            rel_type = parse_rel_field(r_field)

            # extract names/props if available
            n_name = n_props.get("name")
            m_name = m_props.get("name")

            # --- Process PERFORMED_AT Site
            if "Process" in n_labels and "Site" in m_labels and rel_type and rel_type.upper() == "PERFORMED_AT":
                # register site with optionally location
                if m_name:
                    loc = m_props.get("location")
                    sites.setdefault(m_name, {})
                    if loc is not None:
                        sites[m_name]["location"] = loc
                # register process and its site
                if n_name:
                    processes.setdefault(n_name, {"sites": []})
                    if m_name:
                        process_performed_sites[n_name].add(m_name)
                continue

            # --- Resource PROVIDED_BY Supplier
            if "Resource" in n_labels and ("Supplier" in m_labels or "Provider" in m_labels) and rel_type and rel_type.upper() == "PROVIDED_BY":
                if n_name:
                    resources.setdefault(n_name, {"suppliers": []})
                    if m_name:
                        # register supplier with recycled/location props
                        suppliers.setdefault(m_name, {})
                        rec = m_props.get("recyclable")
                        if rec is not None:
                            suppliers[m_name]["recyclable"] = True if rec.lower() in ("yes","true","1") else False
                        loc = m_props.get("location")
                        if loc is not None:
                            suppliers[m_name]["location"] = loc
                        if m_name not in resources[n_name]["suppliers"]:
                            resources[n_name]["suppliers"].append(m_name)
                continue

            # --- LogisticRoute (node may be on left or right)
            if "LogisticRoute" in n_labels:
                route_name = n_props.get("name")
                if not route_name:
                    continue
                # initialize entry
                info = route_nodes.setdefault(route_name, {"props": {}, "from_site": None, "to_site": None, "from_supplier": None, "to_supplier": None})
                # copy any props (transportationMode, CO2Emission) into props
                for k,v in n_props.items():
                    if k != "name":
                        info["props"][k] = v
                # relation indicates which endpoint
                if rel_type:
                    rt = rel_type.upper()
                    if rt == "TO_SITE" and "Site" in m_labels:
                        info["to_site"] = m_name
                        sites.setdefault(m_name, {})
                        if "location" in m_props:
                            sites[m_name]["location"] = m_props["location"]
                    elif rt == "FROM_SITE" and "Site" in m_labels:
                        info["from_site"] = m_name
                        sites.setdefault(m_name, {})
                        if "location" in m_props:
                            sites[m_name]["location"] = m_props["location"]
                    elif rt == "FROM_SUPPLIER" and ("Supplier" in m_labels or "Provider" in m_labels):
                        info["from_supplier"] = m_name
                        suppliers.setdefault(m_name, {})
                        if "recyclable" in m_props:
                            suppliers[m_name]["recyclable"] = True if m_props["recyclable"].lower() in ("yes","true","1") else False
                        if "location" in m_props:
                            suppliers[m_name]["location"] = m_props["location"]
                    elif rt == "TO_SUPPLIER" and ("Supplier" in m_labels or "Provider" in m_labels):
                        info["to_supplier"] = m_name
                        suppliers.setdefault(m_name, {})
                continue

            if "LogisticRoute" in m_labels:
                route_name = m_props.get("name")
                if not route_name:
                    continue
                info = route_nodes.setdefault(route_name, {"props": {}, "from_site": None, "to_site": None, "from_supplier": None, "to_supplier": None})
                # copy props from m node
                for k,v in m_props.items():
                    if k != "name":
                        info["props"][k] = v
                # relation from left node to this route node
                if rel_type:
                    rt = rel_type.upper()
                    if rt == "TO_SITE" and "Site" in n_labels:
                        info["to_site"] = n_props.get("name")
                        sites.setdefault(n_props.get("name"), {})
                        if "location" in n_props:
                            sites[n_props["name"]]["location"] = n_props["location"]
                    elif rt == "FROM_SITE" and "Site" in n_labels:
                        info["from_site"] = n_props.get("name")
                        sites.setdefault(n_props.get("name"), {})
                        if "location" in n_props:
                            sites[n_props["name"]]["location"] = n_props["location"]
                    elif rt == "FROM_SUPPLIER" and ("Supplier" in n_labels or "Provider" in n_labels):
                        info["from_supplier"] = n_props.get("name")
                        suppliers.setdefault(n_props.get("name"), {})
                continue

            # --- IntermediaryProduct INPUT_PRODUCT -> ProcessConfiguration (product is input to PC)
            if ("IntermediaryProduct" in n_labels or "IntermediaryProduct" in m_labels) and rel_type and rel_type.upper() == "INPUT_PRODUCT":
                # CSV shows product -> PC (product is input to PC)
                if "IntermediaryProduct" in n_labels and "ProcessConfiguration" in m_labels:
                    prod = n_name
                    pc = m_name
                    if prod:
                        commod_set.add(prod)
                    # make sure PC exists in temp
                    if pc:
                        # record that PC consumes this product (we'll add to depends_on if needed)
                        pc_temp[pc].setdefault("inputs", set()).add(prod)
                # sometimes reversed—handle both ways
                elif "IntermediaryProduct" in m_labels and "ProcessConfiguration" in n_labels:
                    prod = m_name
                    pc = n_name
                    if prod:
                        commod_set.add(prod)
                    if pc:
                        pc_temp[pc].setdefault("inputs", set()).add(prod)
                continue

            # --- ProcessConfiguration relations ---
            if "ProcessConfiguration" in n_labels:
                pc = n_name
                if not pc:
                    continue
                # REQUIRES_PROCESS
                if rel_type and rel_type.upper() == "REQUIRES_PROCESS" and "Process" in m_labels:
                    pc_temp[pc]["requires_process"] = m_name
                    # ensure process exists
                    processes.setdefault(m_name, {"sites": []})
                    continue
                # USES_RESOURCE
                if rel_type and rel_type.upper() in ("USES_RESOURCE", "USES_RESOURCE]") and "Resource" in m_labels:
                    pc_temp[pc]["uses_resources"].add(m_name)
                    resources.setdefault(m_name, {"suppliers": []})
                    continue
                # OUTPUT_PRODUCT or OUTPUT_END_PRODUCT
                if rel_type and rel_type.upper() in ("OUTPUT_PRODUCT","OUTPUT_END_PRODUCT","OUTPUT_END_PRODUCT]"):
                    # m is product
                    if "IntermediaryProduct" in m_labels or "IntermediaryProduct" in m_labels or "Product" in m_labels:
                        pc_temp[pc]["outputs"].add(m_name)
                        commod_set.add(m_name)
                    continue
                # DEPENDS_ON
                if rel_type and rel_type.upper() == "DEPENDS_ON" and "ProcessConfiguration" in m_labels:
                    pc_temp[pc]["depends_on"].add(m_name)
                    continue

            # --- ProcessConfiguration relations where PC is on right side (occasionally)
            if "ProcessConfiguration" in m_labels:
                pc = m_name
                if not pc:
                    continue
                # If left node is IntermediaryProduct and relation is OUTPUT_PRODUCT maybe reversed; skip because we handled common direction
                # Already handled many cases above; continue
                # But if left is Process and relation is REQUIRES_PROCESS reversed, handle:
                if rel_type and rel_type.upper() == "REQUIRES_PROCESS" and "Process" in n_labels:
                    pc_temp[pc]["requires_process"] = n_name
                    processes.setdefault(n_name, {"sites": []})
                    continue

            # --- Process -> Product (rare)
            if "Process" in n_labels and ("IntermediaryProduct" in m_labels or "Product" in m_labels) and rel_type and rel_type.upper() in ("OUTPUT_PRODUCT","OUTPUT_END_PRODUCT"):
                # process produces product
                commod_set.add(m_name)
                processes.setdefault(n_name, {"sites": []})
                continue

            # --- Supplier or Site nodes stand-alone: capture properties
            if "Supplier" in n_labels:
                suppliers.setdefault(n_name, {})
                if "recyclable" in n_props:
                    suppliers[n_name]["recyclable"] = True if n_props["recyclable"].lower() in ("yes","true","1") else False
                if "location" in n_props:
                    suppliers[n_name]["location"] = n_props["location"]
            if "Supplier" in m_labels:
                suppliers.setdefault(m_name, {})
                if "recyclable" in m_props:
                    suppliers[m_name]["recyclable"] = True if m_props["recyclable"].lower() in ("yes","true","1") else False
                if "location" in m_props:
                    suppliers[m_name]["location"] = m_props["location"]

            if "Site" in n_labels:
                sites.setdefault(n_name, {})
                if "location" in n_props:
                    sites[n_name]["location"] = n_props["location"]
            if "Site" in m_labels:
                sites.setdefault(m_name, {})
                if "location" in m_props:
                    sites[m_name]["location"] = m_props["location"]

            # --- Resource nodes present without relation
            if "Resource" in n_labels:
                resources.setdefault(n_name, {"suppliers": []})
            if "Resource" in m_labels:
                resources.setdefault(m_name, {"suppliers": []})

    # --- After reading CSV: finalize structures ---

    # Build processes dict from process_performed_sites
    for proc, site_set in process_performed_sites.items():
        processes.setdefault(proc, {"sites": []})
        processes[proc]["sites"] = sorted(list(site_set))

    # Build logistic_routes from route_nodes
    for rname, info in route_nodes.items():
        entry = {}
        if info.get("from_site"):
            entry["from_site"] = info["from_site"]
        if info.get("to_site"):
            entry["to_site"] = info["to_site"]
        if info.get("from_supplier"):
            entry["from_supplier"] = info["from_supplier"]
        # include any props (transportationMode, CO2Emission) if present
        if info.get("props"):
            entry.update(info["props"])
        logistic_routes[rname] = entry

    # Build resources dict (ensure suppliers list sorted)
    for res, val in list(resources.items()):
        # if resources were populated as set earlier, convert to list
        if isinstance(val, set):
            resources[res] = {"suppliers": sorted(list(val))}
        else:
            # ensure 'suppliers' key exists
            resources.setdefault(res, {"suppliers": []})

    # Build process_configurations final dict
    final_pcs = OrderedDict()
    for pc_name, vals in pc_temp.items():
        final_pcs[pc_name] = {
            "requires_process": vals.get("requires_process"),
            "uses_resources": sorted(list(vals.get("uses_resources", []))),
            "outputs": sorted(list(vals.get("outputs", []))),
            "depends_on": sorted(list(vals.get("depends_on", [])))
        }

    # Ensure commodities_products order
    commodities_products = sorted(commod_set)

# Some entries may still be empty because CSV may not include explicit rows in some directions.
# However for your provided CSV they should be complete.

    with open(OUTPUT_PY, "w", encoding="utf-8") as out:
        out.write("# Auto-generated data from export.csv\n\n")
        out.write("sites = " + pretty_dict(sites) + "\n\n")
        out.write("processes = " + pretty_dict(processes) + "\n\n")
        out.write("suppliers = " + pretty_dict(suppliers) + "\n\n")
        out.write("resources = " + pretty_dict(resources) + "\n\n")
        out.write("commodities_products = " + pretty_dict(commodities_products) + "\n\n")
        out.write("process_configurations = " + pretty_dict(final_pcs) + "\n\n")
        out.write("logistic_routes = " + pretty_dict(logistic_routes) + "\n\n")


    print(f"\nWrote parsed data to {OUTPUT_PY}")

    return {
        "sites": sites,
        "processes": processes,
        "suppliers": suppliers,
        "resources": resources,
        "commodities_products": commodities_products,
        "process_configurations": final_pcs,
        "logistic_routes": logistic_routes,
    }
