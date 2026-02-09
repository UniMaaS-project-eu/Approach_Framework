QUERY= """/* 1) Identify LAST ProcessConfiguration in chain (no outgoing DEPENDS_ON) */
MATCH (pc_last:ProcessConfiguration)
WHERE NOT (pc_last)<-[:DEPENDS_ON]-()

MATCH (pc_last)-[:REQUIRES_PROCESS]->(proc_last:Process)
MATCH (proc_last)-[:PERFORMED_AT]->(site_last:Site)

/* ⭐ NEW: Intermediary products related to the LAST PC */
OPTIONAL MATCH (pc_last)<-[:INPUT_PRODUCT]-(prod_in_last:IntermediaryProduct)
OPTIONAL MATCH (pc_last)-[:OUTPUT_PRODUCT]->(prod_out_last:IntermediaryProduct)

OPTIONAL MATCH (pc_last)-[:USES_RESOURCE]->(res_last:Resource)
WITH pc_last, proc_last,
     collect(site_last) AS rootSites,
     res_last,
     collect(DISTINCT prod_in_last)  AS rootProductsIn,
     collect(DISTINCT prod_out_last) AS rootProductsOut

OPTIONAL MATCH (res_last)-[:PROVIDED_BY]->(sup_last:Supplier{recyclable:'yes'})
OPTIONAL MATCH (sup_last)<-[:FROM_SUPPLIER]-(lr_last:LogisticRoute)-[:TO_SITE]->(site_last)

WITH pc_last, proc_last, rootSites, res_last,
     rootProductsIn, rootProductsOut,
     [x IN collect(DISTINCT {supplier: sup_last, route: lr_last, site: site_last}) WHERE x.route IS NOT NULL] 
     AS lastFastSuppliers

WHERE res_last IS NULL OR size(lastFastSuppliers) > 0


/* 2) Traverse chain BACKWARD through DEPENDS_ON from last to first */
MATCH path = (pc_last)-[:DEPENDS_ON*]->(pc_prev:ProcessConfiguration)
WITH path, nodes(path) AS pcs,
     pc_last, proc_last, rootSites, res_last,
     rootProductsIn, rootProductsOut, lastFastSuppliers


/* 3) For each PC, collect valid sites, resources AND products */
UNWIND pcs AS pc
MATCH (pc)-[:REQUIRES_PROCESS]->(proc:Process)
MATCH (proc)-[:PERFORMED_AT]->(site:Site)

OPTIONAL MATCH (pc)-[:USES_RESOURCE]->(res:Resource)

/* ⭐ NEW: products at each PC */
OPTIONAL MATCH (pc)<-[:INPUT_PRODUCT]-(prod_in:IntermediaryProduct)
OPTIONAL MATCH (pc)-[:OUTPUT_PRODUCT]->(prod_out:IntermediaryProduct)

WITH pc_last, path, pc, proc,
     collect(site) AS sites,
     res,
     collect(DISTINCT prod_in)  AS prod_in,
     collect(DISTINCT prod_out) AS prod_out

OPTIONAL MATCH (res)-[:PROVIDED_BY]->(s:Supplier{recyclable:'yes'})
OPTIONAL MATCH (s)<-[:FROM_SUPPLIER]-(lr:LogisticRoute)-[:TO_SITE]->(site)
WHERE site IN sites

WITH pc_last, path, pc, proc, sites, res, prod_in, prod_out,
     [x IN collect(DISTINCT {supplier: s, route: lr, site: site}) WHERE x.route IS NOT NULL] 
     AS fastSuppliers


/* 4) Enforce HARD PER-PC constraints */
WHERE proc IS NOT NULL
  AND size(sites) > 0
  AND (res IS NULL OR size(fastSuppliers) > 0)


/* 5) Create canonical PC info (now includes products) */
WITH pc_last, path,
     collect({
       pc: pc,
       proc: proc,
       sites: sites,
       res: res,
       prod_in: prod_in,
       prod_out: prod_out,
       fastSuppliers: fastSuppliers
     }) AS pcData


/* 6) Validate adjacency logistics between consecutive PCs */
UNWIND CASE WHEN size(pcData) >= 2 THEN range(0, size(pcData)-2) ELSE [] END AS idx
WITH pcData[idx] AS prevEntry,
     pcData[idx+1] AS nextEntry,
     idx, pc_last, path, pcData

UNWIND prevEntry.sites AS prevSite
UNWIND nextEntry.sites AS nextSite

MATCH (nextSite)<-[:FROM_SITE]-(lrAdj:LogisticRoute)-[:TO_SITE]->(prevSite)

WITH DISTINCT
     pc_last,
     idx,
     prevEntry,
     nextEntry,
     prevSite,
     nextSite,
     collect(DISTINCT lrAdj) AS adjacencyFastRoutes


/* 7) Build final nodes (NOW INCLUDING PRODUCTS) */
WITH
     (
       [prevEntry.pc, nextEntry.pc, prevEntry.proc, nextEntry.proc] +
       [prevSite, nextSite] +

       CASE WHEN prevEntry.res IS NULL THEN [] ELSE [prevEntry.res] END +
       CASE WHEN nextEntry.res IS NULL THEN [] ELSE [nextEntry.res] END +

       /* ⭐ NEW: Intermediary products */
       CASE WHEN prevEntry.prod_in  IS NULL THEN [] ELSE prevEntry.prod_in  END +
       CASE WHEN prevEntry.prod_out IS NULL THEN [] ELSE prevEntry.prod_out END +
       CASE WHEN nextEntry.prod_in  IS NULL THEN [] ELSE nextEntry.prod_in  END +
       CASE WHEN nextEntry.prod_out IS NULL THEN [] ELSE nextEntry.prod_out END +

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

RETURN n, r, m;
"""
