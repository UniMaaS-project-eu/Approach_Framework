QUERY= """/* 1) Identify LAST ProcessConfiguration in chain (no outgoing DEPENDS_ON) */
MATCH (pc_last:ProcessConfiguration)
WHERE NOT (pc_last)<-[:DEPENDS_ON]-()

MATCH (pc_last)-[:REQUIRES_PROCESS]->(proc_last:Process)
MATCH (proc_last)-[:PERFORMED_AT]->(site_last:Site)

OPTIONAL MATCH (pc_last)-[:USES_RESOURCE]->(res_last:Resource)
WITH pc_last, proc_last, collect(site_last) AS rootSites, res_last

OPTIONAL MATCH (res_last)-[:PROVIDED_BY]->(sup_last:Supplier)
OPTIONAL MATCH (sup_last)<-[:FROM_SUPPLIER]-(lr_last:LogisticRoute{transportationMode:'fast'})-[:TO_SITE]->(site_last)

WITH pc_last, proc_last, rootSites, res_last,
     [x IN collect(DISTINCT {supplier: sup_last, route: lr_last, site: site_last}) WHERE x.route IS NOT NULL] 
     AS lastFastSuppliers

WHERE res_last IS NULL OR size(lastFastSuppliers) > 0


/* 2) Traverse chain BACKWARD through DEPENDS_ON from last to first */
MATCH path = (pc_last)-[:DEPENDS_ON*]->(pc_prev:ProcessConfiguration)
WITH path, nodes(path) AS pcs, pc_last, proc_last, rootSites, res_last, lastFastSuppliers


/* 3) For each PC, collect valid sites and resources */
UNWIND pcs AS pc
MATCH (pc)-[:REQUIRES_PROCESS]->(proc:Process)
MATCH (proc)-[:PERFORMED_AT]->(site:Site)
OPTIONAL MATCH (pc)-[:USES_RESOURCE]->(res:Resource)

OPTIONAL MATCH (pc)-[:INPUT_PRODUCT]->(inProd:IntermediaryProduct)
OPTIONAL MATCH (pc)-[:OUTPUT_PRODUCT]->(outProd:IntermediaryProduct)

WITH pc_last, path, pc, proc, collect(site) AS sites, res,
     collect(DISTINCT inProd) AS inputProducts,
     collect(DISTINCT outProd) AS outputProducts

OPTIONAL MATCH (res)-[:PROVIDED_BY]->(s:Supplier)
OPTIONAL MATCH (s)<-[:FROM_SUPPLIER]-(lr:LogisticRoute{transportationMode:'fast'})-[:TO_SITE]->(site)
WHERE site IN sites

WITH pc_last, path, pc, proc, sites, res, inputProducts, outputProducts,
     [x IN collect(DISTINCT {supplier: s, route: lr, site: site}) WHERE x.route IS NOT NULL] 
     AS fastSuppliers


/* 4) Enforce HARD PER-PC constraints */
WHERE proc IS NOT NULL
  AND size(sites) > 0
  AND (res IS NULL OR size(fastSuppliers) > 0)


/* 5) Create canonical PC info */
WITH pc_last, path,
     collect({
       pc: pc,
       proc: proc,
       sites: sites,
       res: res,
       fastSuppliers: fastSuppliers,
       inProducts: inputProducts,
       outProducts: outputProducts
     }) AS pcData


/* 6) Validate adjacency logistics between consecutive PCs */
UNWIND CASE WHEN size(pcData) >= 2 THEN range(0, size(pcData)-2) ELSE [] END AS idx
WITH pcData[idx] AS prevEntry, pcData[idx+1] AS nextEntry, idx, pc_last, path, pcData

UNWIND prevEntry.sites AS prevSite
UNWIND nextEntry.sites AS nextSite

MATCH (nextSite)<-[:FROM_SITE]-(lrAdj:LogisticRoute{transportationMode:'fast'})-[:TO_SITE]->(prevSite)

WITH DISTINCT
     pc_last,
     idx,
     prevEntry,
     nextEntry,
     prevSite,
     nextSite,
     collect(DISTINCT lrAdj) AS adjacencyFastRoutes


/* 7) Build final nodes (PRODUCTS INCLUDED) */
WITH
     (
       [prevEntry.pc, nextEntry.pc, prevEntry.proc, nextEntry.proc] +
       [prevSite, nextSite] +
       prevEntry.inProducts +
       prevEntry.outProducts +
       nextEntry.inProducts +
       nextEntry.outProducts +
       CASE WHEN prevEntry.res IS NULL THEN [] ELSE [prevEntry.res] END +
       CASE WHEN nextEntry.res IS NULL THEN [] ELSE [nextEntry.res] END +
       (CASE WHEN prevEntry.fastSuppliers IS NULL THEN [] ELSE [x IN prevEntry.fastSuppliers | x.supplier] END) +
       (CASE WHEN prevEntry.fastSuppliers IS NULL THEN [] ELSE [x IN prevEntry.fastSuppliers | x.route] END) +
       (CASE WHEN nextEntry.fastSuppliers IS NULL THEN [] ELSE [x IN nextEntry.fastSuppliers | x.supplier] END) +
       (CASE WHEN nextEntry.fastSuppliers IS NULL THEN [] ELSE [x IN nextEntry.fastSuppliers | x.route] END) +
       adjacencyFastRoutes
     ) AS rowNodes


/* 8) Deduplicate */
UNWIND rowNodes AS maybeNode
WITH collect(DISTINCT maybeNode) AS finalNodes


/* 9) All relationships strictly between those nodes */
UNWIND finalNodes AS n
MATCH (n)-[r]-(m)
WHERE m IN finalNodes

WITH collect(DISTINCT n) AS allFinalNodes,
     collect(DISTINCT r) AS allRelationships

UNWIND allFinalNodes AS n
MATCH (n)-[r]->(m)
WHERE m IN allFinalNodes

RETURN n,r,m;
"""
