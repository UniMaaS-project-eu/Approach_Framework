"""this script measures the time of model building for the extracted KG after the query execution"""
import query1, query2, query3, queryFull
import csv
import os
import json
from UniMaaS_Approach_Scalability.UniMaaS_Approach_Scalability_final.network_optimization import flow_network_optimization
import sys
from UniMaaS_Approach_Scalability.UniMaaS_Approach_Scalability_final.parsing_neo4j_csv import parse_neo4j_csv
"""
neo4j_kg_generator.py

Generate Neo4j knowledge graphs that respect the MSC ontology and rules, with configurable
"length" (number of ProcessConfiguration nodes chained by DEPENDS_ON) and "width/cardinality"
(number of Sites per Process, Resources per ProcessConfiguration, Suppliers per Resource).

Usage (example):
  export NEO4J_URI=bolt://localhost:7687
  export NEO4J_USER=neo4j
  export NEO4J_PASSWORD=secret
  python3 neo4j_kg_generator.py --length 10 --sites-per-process 3 --resources-per-pc 2 --suppliers-per-resource 2

Ontology rules to respect:
  * Every ProcessConfiguration has exactly 1 Process linked by REQUIRES_PROCESS.
  * Every ProcessConfiguration has >=1 Site that performs the Process (Process -[:PERFORMED_AT]-> Site).
  * If a ProcessConfiguration USES_RESOURCE Resource, the Resource has >=1 Supplier (Resource -[:PROVIDED_BY]-> Supplier).
  * If a ProcessConfiguration USES_RESOURCE Resource, there exists at least one LogisticRoute from some Supplier of that Resource to some Site of that ProcessConfiguration's Process (Supplier <-[:FROM_SUPPLIER]- LogisticRoute -[:TO_SITE]-> Site).
  * Each consecutive pair of ProcessConfiguration nodes (pc_i DEPENDS_ON pc_{i-1}) has at least one LogisticRoute connecting some successor's site -> predecessor's site (Site <-[:FROM_SITE]- LogisticRoute -[:TO_SITE]-> Site).


Here we create LogisticRoutes:
  - Between all Suppliers and Sites of the same ProcessConfiguration
  - Between all successor Sites -> predecessor Sites for consecutive ProcessConfigurations
"""

import os
import argparse
import uuid
import time
from neo4j import GraphDatabase

def chunked(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

class KGGenerator:
    def __init__(self, uri, user, password, run_id=None, batch_size=200):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.run_id = run_id or uuid.uuid4().hex[:8]
        self.batch_size = batch_size

    def close(self):
        self.driver.close()

    def _node_name(self, base, idx):
        return f"{base}_{self.run_id}_{idx}"

    def clear_run(self):
        # OPTIONAL helper to delete everything created in this run (by run_id) - safe cleanup
        s = self.run_id
        query = (
            "MATCH (n) WHERE any(k IN keys(n) WHERE toString(n[k]) CONTAINS $run_id) "
            "DETACH DELETE n"
        )
        with self.driver.session() as ses:
            ses.run(query, run_id=s)
            
    def clear_graph(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def generate_chain(self, length=3, sites_per_process=1, resources_per_pc=0, suppliers_per_resource=1):
       
        # Create a chain of ProcessConfigurations linked by DEPENDS_ON.
        
        run_id = self.run_id
        total_created = {
            "ProcessConfiguration": 0, "Process": 0, "Site": 0,
            "Resource": 0, "Supplier": 0, "LogisticRoute": 0, "IntermediaryProduct": 0
        }

        prev_pc_sites = []  # For linking to previous PC
        prev_ip_name = None  # For INPUT_PRODUCT linking

        with self.driver.session() as session:
            # We'll create nodes in a loop, committing per PC to keep transactions reasonable.
            prev_pc_ref = None
            for i in range(length):
                pc_idx = i
                pc_name = f"PC_{run_id}_{pc_idx}"
                process_name = f"Process_{run_id}_{pc_idx}"
                ip_name = f"IntermediaryProduct_{run_id}_{pc_idx}"  # New product

                # Create Process and ProcessConfiguration nodes and link them (REQUIRES_PROCESS)
                def create_pc_tx(tx, pc_name, process_name, ip_name):
                    q = (
                        "CREATE (p:Process {name:$process_name, run_id:$run_id}) "
                        "CREATE (pc:ProcessConfiguration {name:$pc_name, run_id:$run_id}) "
                        "CREATE (ip:IntermediaryProduct {name:$ip_name, run_id:$run_id}) "
                        "CREATE (pc)-[:REQUIRES_PROCESS]->(p) "
                        "CREATE (pc)-[:OUTPUT_PRODUCT]->(ip) "
                        "RETURN elementId(pc) AS pc_id, elementId(p) AS p_id, elementId(ip) AS ip_id"
                    )
                    return tx.run(q, pc_name=pc_name, process_name=process_name, ip_name=ip_name, run_id=run_id).single()

                record = session.execute_write(create_pc_tx, pc_name, process_name, ip_name)
                pc_id = record["pc_id"]
                p_id = record["p_id"]
                total_created["ProcessConfiguration"] += 1
                total_created["Process"] += 1
                total_created["IntermediaryProduct"] += 1

                # If previous IP exists, link as INPUT_PRODUCT to current PC
                if prev_ip_name:
                    def link_input_tx(tx, pc_name, ip_name):
                        q = (
                            "MATCH (pc:ProcessConfiguration {name:$pc_name, run_id:$run_id}), "
                            "(ip:IntermediaryProduct {name:$ip_name, run_id:$run_id}) "
                            "CREATE (pc)<-[:INPUT_PRODUCT]-(ip)"
                        )
                        tx.run(q, pc_name=pc_name, ip_name=ip_name, run_id=run_id)
                    session.execute_write(link_input_tx, pc_name, prev_ip_name)

                prev_ip_name = ip_name  # update for next iteration

                # Create Sites for this Process
                site_names = []
                for s in range(sites_per_process):
                    site_name = f"Site_{run_id}_{pc_idx}_{s}"
                    def create_site_tx(tx, process_name, site_name):
                        q = (
                            "MATCH (p:Process {name:$process_name, run_id:$run_id}) "
                            "CREATE (s:Site {name:$site_name, "
                            "location: CASE WHEN rand() < 0.7 THEN 'Torino' ELSE 'Loc_' + $run_id END, "
                            "run_id:$run_id}) "
                            "CREATE (p)-[:PERFORMED_AT]->(s) "
                            "RETURN elementId(s) as s_id"
                        )
                        return tx.run(q, process_name=process_name, site_name=site_name, run_id=run_id).single()

                    rec = session.execute_write(create_site_tx, process_name, site_name)
                    site_names.append(site_name)
                    total_created["Site"] += 1

                # Create Resources and Suppliers
                all_supplier_names = []
                for r in range(resources_per_pc):
                    res_name = f"Resource_{run_id}_{pc_idx}_{r}"
                    def create_resource_tx(tx, res_name):
                        q = "CREATE (res:Resource {name:$res_name, run_id:$run_id}) RETURN elementId(res) as r_id"
                        return tx.run(q, res_name=res_name, run_id=run_id).single()
                    session.execute_write(create_resource_tx, res_name)
                    total_created["Resource"] += 1

                    # Suppliers for this resource
                    supplier_names = []
                    for sp in range(suppliers_per_resource):
                        sup_name = f"Supplier_{run_id}_{pc_idx}_{r}_{sp}"
                        def create_supplier_tx(tx, sup_name):
                            q = (
                                "CREATE (sup:Supplier {name:$sup_name, "
                                "recyclable: CASE WHEN rand() < 0.7 THEN 'yes' ELSE 'no' END, "
                                "location: CASE WHEN rand() < 0.7 THEN 'Torino' ELSE 'SupLoc_' + $run_id END, "
                                "run_id:$run_id}) "
                                "RETURN elementId(sup) as sup_id"
                            )
                            return tx.run(q, sup_name=sup_name, run_id=run_id).single()
                        session.execute_write(create_supplier_tx, sup_name)
                        total_created["Supplier"] += 1
                        supplier_names.append(sup_name)
                        all_supplier_names.append(sup_name)

                        # Link Resource -> Supplier
                        def link_resource_supplier_tx(tx, res_name, sup_name):
                            q = "MATCH (res:Resource {name:$res_name, run_id:$run_id}), (sup:Supplier {name:$sup_name, run_id:$run_id}) CREATE (res)-[:PROVIDED_BY]->(sup)"
                            tx.run(q, res_name=res_name, sup_name=sup_name, run_id=run_id)
                        session.execute_write(link_resource_supplier_tx, res_name, sup_name)

                    # Link PC -> Resource
                    def link_pc_resource_tx(tx, pc_name, res_name):
                        q = "MATCH (pc:ProcessConfiguration {name:$pc_name, run_id:$run_id}), (res:Resource {name:$res_name, run_id:$run_id}) CREATE (pc)-[:USES_RESOURCE]->(res)"
                        tx.run(q, pc_name=pc_name, res_name=res_name, run_id=run_id)
                    session.execute_write(link_pc_resource_tx, pc_name, res_name)

                # LogisticRoutes: all Suppliers -> all Sites of the same PC
                for sup_name in all_supplier_names:
                    for site_name in site_names:
                        lr_name = f"LR_{run_id}_{pc_idx}_{sup_name}_to_{site_name}"
                        def create_lr_tx(tx, sup_name, site_name, lr_name):
                            q = (
                                "MATCH (sup:Supplier {name:$sup_name, run_id:$run_id}), (s:Site {name:$site_name, run_id:$run_id}) "
                                "CREATE (lr:LogisticRoute {name:$lr_name, transportationMode: CASE WHEN rand()<=1 THEN 'fast' ELSE 'slow' END, run_id:$run_id}) "
                                "CREATE (lr)-[:FROM_SUPPLIER]->(sup) "
                                "CREATE (lr)-[:TO_SITE]->(s) "
                                "RETURN elementId(lr) as lr_id"
                            )
                            return tx.run(q, sup_name=sup_name, site_name=site_name, lr_name=lr_name, run_id=run_id).single()
                        session.execute_write(create_lr_tx, sup_name, site_name, lr_name)
                        total_created["LogisticRoute"] += 1

                # DEPENDS_ON + LogisticRoutes to previous PC
                if prev_pc_sites:
                    # DEPENDS_ON
                    prev_pc_name = f"PC_{run_id}_{i-1}"
                    def create_dep_tx(tx, pc_name, prev_pc_name):
                        q = "MATCH (pc:ProcessConfiguration {name:$pc_name, run_id:$run_id}), (prev:ProcessConfiguration {name:$prev_pc_name, run_id:$run_id}) CREATE (pc)-[:DEPENDS_ON]->(prev)"
                        tx.run(q, pc_name=pc_name, prev_pc_name=prev_pc_name, run_id=run_id)
                    session.execute_write(create_dep_tx, pc_name, prev_pc_name)

                    # LogisticRoutes: every Site of current PC -> every Site of previous PC
                    for from_site in prev_pc_sites:
                        for to_site in site_names:
                            lr_name = f"LR_{run_id}_{i}_{from_site}_to_{to_site}"
                            def create_lr_site_site_tx(tx, from_site, to_site, lr_name):
                                q = (
                                    "MATCH (fs:Site {name:$from_site, run_id:$run_id}), (ts:Site {name:$to_site, run_id:$run_id}) "
                                    "CREATE (lr:LogisticRoute {name:$lr_name, transportationMode: CASE WHEN rand()<=1 THEN 'fast' ELSE 'slow' END, run_id:$run_id}) "
                                    "CREATE (lr)-[:FROM_SITE]->(fs) "
                                    "CREATE (lr)-[:TO_SITE]->(ts) "
                                    "RETURN elementId(lr) as lr_id"
                                )
                                return tx.run(q, from_site=from_site, to_site=to_site, lr_name=lr_name, run_id=run_id).single()
                            session.execute_write(create_lr_site_site_tx, from_site, to_site, lr_name)
                            total_created["LogisticRoute"] += 1

                prev_pc_sites = site_names  # update for next iteration

        return total_created

    def export_full_graph_to_csv(self, csv_path="full_graph.csv"):
        """
        Exports the entire Neo4j knowledge graph to a CSV file in the exact
        (n, r, m) string format expected by flow_network_optimization().
        """

        cypher = "MATCH (n)-[r]->(m) RETURN n, r, m"

        with self.driver.session() as ses:
            result = ses.run(cypher)

            import csv

            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["n", "r", "m"])

                for record in result:
                    n = record["n"]
                    r = record["r"]
                    m = record["m"]

                    # Use Python driver's string representations exactly as-is
                    writer.writerow([str(n), str(r), str(m)])

        print(f"[OK] Full graph exported to {csv_path}")
        return csv_path

    
    def run_queries_and_measure(self, queries, runs=1):
        results = {}

        with self.driver.session() as ses:
            for name, cypher in queries:
                print(f"Running {name}...")

                run_stats = []

                for _ in range(runs):
                    # -----------------------------
                    # 1) Execute query to fetch subgraph
                    # -----------------------------
                    t0_query = time.perf_counter()
                    with self.driver.session() as s2:
                        result_fetch = s2.run(cypher)
                        records = list(result_fetch)
                    t1_query = time.perf_counter()
                    query_time = t1_query - t0_query

                    # -----------------------------
                    # 2) Prepare CSV
                    # -----------------------------
                    t0_csv = time.perf_counter()
                    record_count = len(records)

                    if record_count == 0:
                        headers = []
                        is_empty = True
                    else:
                        headers = records[0].keys()
                        is_empty = False

                    csv_path = os.path.join(".", f"{name}.csv")

                    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(headers)
                        for record in records:
                            row = [str(record[h]) for h in headers]
                            writer.writerow(row)

                    t1_csv = time.perf_counter()
                    csv_time = t1_csv - t0_csv

                    # -----------------------------
                    # 3) Parse CSV for model input
                    # -----------------------------
                    t0_parse = time.perf_counter()
                    data = parse_neo4j_csv(csv_path)
                    t1_parse = time.perf_counter()
                    parsing_time = t1_parse - t0_parse

                    # -----------------------------
                    # 4) Run Optimization Model
                    # -----------------------------
                    t0_model = time.perf_counter()
                    flow_network_optimization(data)
                    t1_model = time.perf_counter()
                    model_time = t1_model - t0_model

                    # -----------------------------
                    # 5) Total Time
                    # -----------------------------
                    total_time = (
                        query_time 
                        + csv_time 
                        + parsing_time 
                        + model_time
                    )

                    run_stats.append({
                        "query_time": query_time,
                        "csv_time": csv_time,
                        "parsing_time": parsing_time,
                        "model_time": model_time,
                        "total_time": total_time
                    })

                # Aggregate stats across runs
                results[name] = {
                    "runs": run_stats,
                    "empty": is_empty,
                    "record_count": record_count
                }

        return results


# def example_queries():
#     return [
#         ("count_nodes", "MATCH (n) RETURN count(n) AS cnt"),
#         ("count_relationships", "MATCH ()-[r]->() RETURN count(r) AS cnt"),
#         ("pattern_match_simple", "MATCH (pc:ProcessConfiguration)-[r:USES_RESOURCE]->(res:Resource) RETURN count(pc) AS cnt"),
#         ("path_length_2", "MATCH (pc:ProcessConfiguration)-[:DEPENDS_ON*1..]->(prev:ProcessConfiguration) RETURN count(pc) AS cnt"),
#         ("full_pattern_sample", "MATCH (s:Supplier)-[:FROM_SUPPLIER]-()-[:TO_SITE]->(site:Site)<-[:PERFORMED_AT]-(p:Process) RETURN count(distinct p) as cnt")
#     ]

def main():
    parser = argparse.ArgumentParser(description='Generate Neo4j knowledge graphs for scalability testing')
    parser.add_argument('--uri', default=os.getenv('NEO4J_URI', 'bolt://localhost:7687'))
    parser.add_argument('--user', default=os.getenv('NEO4J_USER', 'neo4j'))
    parser.add_argument('--password', default=os.getenv('NEO4J_PASSWORD', 'fVcPu!d3iV#0cT'))
    parser.add_argument('--length', type=int, default=3, help='Number of ProcessConfiguration nodes in a chain (length)')
    parser.add_argument('--sites-per-process', type=int, default=1, help='Number of Sites per Process')
    parser.add_argument('--resources-per-pc', type=int, default=0, help='Number of Resources used by each ProcessConfiguration')
    parser.add_argument('--suppliers-per-resource', type=int, default=1, help='Number of Suppliers per Resource')
    parser.add_argument('--run-id', default=None, help='Optional run id (default: autogenerated)')
    parser.add_argument('--batch-size', type=int, default=200, help='Batch size for commits (not heavily used in this simple script)')
    parser.add_argument('--measure', action='store_true', help='Run example queries and print timings after generation')

    args = parser.parse_args()

    print(f"Connecting to {args.uri} as {args.user} ...")
    gen = KGGenerator(args.uri, args.user, args.password, run_id=args.run_id, batch_size=args.batch_size)

    print("Clearing existing graph ...")
    gen.clear_graph()
    print(f"Creating chain length={args.length}, sites_per_process={args.sites_per_process}, resources_per_pc={args.resources_per_pc}, suppliers_per_resource={args.suppliers_per_resource}")
    gen.generate_chain(length=args.length, sites_per_process=args.sites_per_process, resources_per_pc=args.resources_per_pc, suppliers_per_resource=args.suppliers_per_resource)

    # print("Created:")
    # for k, v in counts.items():
    #     print(f"  {k}: {v}")

    avg_total_time = 0

    if args.measure:

        """for extracting the subgraph based on the queries and then building the optimization model"""
        print("Running queries and measuring execution time...")

        real_queries = [
            # ("query1", query1.QUERY),
            ("query2", query2.QUERY),
            # ("query3", query3.QUERY),
            # ("full_query", queryFull.QUERY)
        ]

        # real_queries = [("query", "MATCH (n)-[r]->(m) RETURN n, r, m")]

        measurements = gen.run_queries_and_measure(real_queries, runs=1)
        avg_time = None

        for name, info in measurements.items():
            run_stats = info["runs"]

            avg_query   = sum(r["query_time"] for r in run_stats) / len(run_stats)
            avg_csv     = sum(r["csv_time"] for r in run_stats) / len(run_stats)
            avg_parse   = sum(r["parsing_time"] for r in run_stats) / len(run_stats)
            avg_model   = sum(r["model_time"] for r in run_stats) / len(run_stats)
            avg_total   = sum(r["total_time"] for r in run_stats) / len(run_stats)

            print(f"\n{name}:")
            print(f"  AVG query_time   : {avg_query:.6f}")
            print(f"  AVG csv_time     : {avg_csv:.6f}")
            print(f"  AVG parsing_time : {avg_parse:.6f}")
            print(f"  AVG model_time   : {avg_model:.6f}")
            print(f"  AVG total_time   : {avg_total:.6f}")

            avg_total_time = avg_total

        """for measuring only the optimization model building time on the full graph"""
        # csv_file = gen.export_full_graph_to_csv("full_graph.csv")

        # # Measure processing only (not Cypher execution)
        # t0 = time.perf_counter()
        # flow_network_optimization(csv_file)
        # t1 = time.perf_counter()
        # avg_time = t1 - t0

        # print(f"Flow network optimization time: {t1 - t0:.4f} s")

    gen.close()
    sys.exit(int(avg_total_time * 1000))  # in milliseconds

if __name__ == '__main__':
    main()
