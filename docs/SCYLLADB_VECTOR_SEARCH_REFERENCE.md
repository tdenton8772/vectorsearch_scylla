# ScyllaDB Vector Search Reference Guide
**Status:** BETA  
**Last Updated:** November 17, 2025  
**Documentation Source:** https://cloud.docs.scylladb.com/stable/vector-search/index.html

---

## Table of Contents
1. [Overview](#overview)
2. [Key Concepts](#key-concepts)
3. [Getting Started](#getting-started)
4. [Deployment & Configuration](#deployment--configuration)
5. [CQL Reference](#cql-reference)
6. [API Reference](#api-reference)
7. [Limitations & Constraints](#limitations--constraints)
8. [Use Cases](#use-cases)

---

## Overview

### What is Vector Search in ScyllaDB?
Vector search allows you to store, index, and query high-dimensional vector data at scale within ScyllaDB. It's designed for AI and machine learning applications where data is represented as mathematical vectors (embeddings) for similarity-based retrieval rather than exact matches.

### Beta Status Important Notes:
- **Available via API only** (no UI in beta)
- **Not for production workloads** or performance benchmarking
- Testing and proof-of-concept purposes only
- Best-effort support, no SLA guaranteed
- **Vector search nodes have a 30-day TTL** during beta

---

## Key Concepts

### Vector Data Type
- **Syntax:** `vector<element_type, dimension>`
- **Example:** `vector<float, 768>`
- **Element types:** Typically `float`
- **Dimensions:** 1 to 16,000
- Native CQL type fully supported by protocol v5

### Embeddings
- Fixed-length numeric vectors representing data (text, images, audio)
- Capture semantic or structural meaning in high-dimensional space
- **ScyllaDB does NOT generate embeddings** - they must be created externally
- ScyllaDB stores and queries pre-generated embeddings

### Vector Index (HNSW-based)
- Based on Hierarchical Navigable Small World (HNSW) algorithm
- Uses **USearch** library by Unum for high-performance in-memory indexing
- Supports Approximate Nearest Neighbor (ANN) search
- Three similarity functions: `COSINE` (default), `DOT_PRODUCT`, `EUCLIDEAN`

### Similarity Functions
- **COSINE:** Measures angular similarity (default)
- **DOT_PRODUCT:** Measures alignment/magnitude
- **EUCLIDEAN:** Measures geometric distance

### ANN (Approximate Nearest Neighbor)
- Finds data points most similar to a query vector
- Trades small accuracy for significant speed improvements
- Returns "close enough" results efficiently
- Ideal for semantic search, recommendations, RAG applications

---

## Getting Started

### Prerequisites
1. ScyllaDB Cloud account at https://cloud.scylladb.com/
2. Personal API Token (Settings > Personal Tokens)
3. ScyllaDB version >= 2025.4.X
4. Account ID, Cluster ID, DC ID (retrieved via API)

### Basic Workflow
1. Create keyspace with tablets **disabled**
2. Create table with vector-typed column
3. Insert vector data (embeddings)
4. Create vector index on vector column
5. Perform ANN similarity searches using `ORDER BY ... ANN OF`

---

## Deployment & Configuration

### Cluster Creation

#### New Cluster with Vector Search
```bash
curl -X POST "https://api.cloud.scylladb.com/account/{ACCOUNT_ID}/cluster" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "clusterName": "my-vector-cluster",
    "cloudProviderId": 1,
    "regionId": 1,
    "scyllaVersion": "2025.4.0~rc0-0.20251001.6969918d3151",
    "numberOfNodes": 3,
    "instanceId": 62,
    "freeTier": true,
    "replicationFactor": 3,
    "vectorSearch": {
      "defaultNodes": 1,
      "defaultInstanceTypeId": 175
    }
  }'
```

### Supported Instance Types (Beta)
**AWS:**
- 175: t4g.small
- 176: t4g.medium
- 177: r7g.medium

**GCP:**
- 178: e2-small
- 179: e2-medium
- 180: n4-highmem-2

### Free Trial Instance Types
**AWS:**
- 62: i4i.large
- 63: i4i.xlarge
- 64: i4i.2xlarge

**GCP:**
- 40: n2-highmem-2
- 41: n2-highmem-4
- 42: n2-highmem-8

#### Enable on Existing Cluster
```bash
# Deploy vector search nodes
curl -X POST "https://api.cloud.scylladb.com/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/dc/{DC_ID}/vector-search" \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
     "defaultNodes": 1,
     "defaultInstanceTypeId": 175
  }'
```

---

## CQL Reference

### 1. Create Keyspace (Tablets MUST be Disabled)
```cql
CREATE KEYSPACE myapp
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'replication_factor': 3
}
AND tablets = {
   'enabled': false
};
```

### 2. Create Table with Vector Column
```cql
CREATE TABLE IF NOT EXISTS myapp.comments (
  record_id timeuuid,
  id uuid,
  commenter text,
  comment text,
  comment_vector vector<float, 64>,
  created_at timestamp,
  PRIMARY KEY (id, created_at)
);
```

### 3. Insert Vector Data
```cql
INSERT INTO myapp.comments (
    record_id, id, commenter, comment, comment_vector, created_at
) VALUES (
    now(),
    uuid(),
    'Alice',
    'I like vector search in ScyllaDB.',
    [0.12, 0.34, 0.56, 0.78, 0.91, 0.15, 0.62, 0.48, ...],
    toTimestamp(now())
);
```

### 4. Create Vector Index
```cql
CREATE CUSTOM INDEX IF NOT EXISTS ann_idx
ON myapp.comments(comment_vector)
USING 'vector_index'
WITH OPTIONS = { 
    'similarity_function': 'COSINE'
};
```

**Index Options:**
- `similarity_function`: `COSINE` (default), `DOT_PRODUCT`, `EUCLIDEAN`
- `m` (maximum_node_connections): Max connections per HNSW node
- `ef_construct`: Construction beam width
- `ef_search`: Search beam width

### 5. Query with ANN Search
```cql
SELECT id, commenter, comment, created_at
FROM myapp.comments
ORDER BY comment_vector ANN OF [0.12, 0.34, 0.56, ...]
LIMIT 5;
```

### 6. Drop Vector Index
```cql
DROP INDEX IF EXISTS ann_idx;
```

---

## API Reference

### Get Account ID
```bash
curl -X GET "https://api.cloud.scylladb.com/account/default" \
   -H "Authorization: Bearer YOUR_API_TOKEN"
```

### Get Cluster ID
```bash
curl -X GET "https://api.cloud.scylladb.com/account/{ACCOUNT_ID}/clusters" \
   -H "Authorization: Bearer YOUR_API_TOKEN"
```

### Get DC ID
```bash
curl -X GET "https://api.cloud.scylladb.com/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/dcs" \
   -H "Authorization: Bearer YOUR_API_TOKEN"
```

### Get Vector Search Nodes
```bash
curl -X GET "https://api.cloud.scylladb.com/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/dc/{DC_ID}/vector-search" \
 -H "Authorization: Bearer YOUR_API_TOKEN"
```

### Delete Vector Search Nodes
```bash
curl -X DELETE "https://api.cloud.scylladb.com/account/{ACCOUNT_ID}/cluster/{CLUSTER_ID}/dc/{DC_ID}/vector-search" \
 -H "Authorization: Bearer YOUR_API_TOKEN"
```

---

## Limitations & Constraints

### Deployment Limitations (Beta)
- ✗ Only via API (no UI support)
- ✗ One vector search node per Availability Zone
- ✗ Multi-DC deployments not supported
- ✗ **30-day TTL on vector search nodes**
- ✗ Must use ScyllaDB version >= 2025.4.X
- ✗ Cannot enable on UI-created clusters

### CQL Limitations (Beta)
- ✗ **Tablets must be disabled** in keyspace
- ✗ `ANN OF` only supported in `ORDER BY` clauses
- ✗ **No filtering** - `ANN OF` not supported with `WHERE` clauses
- ✗ `ALTER INDEX` not supported (must drop and recreate)
- ✗ **TTL not supported** on vector columns or tables with vector indexes
- ✗ Local indexes not supported

### CDC (Change Data Capture)
- ✓ CDC enabled automatically for vector indexes
- ✓ No manual CDC configuration required

---

## Use Cases

### Primary Applications
1. **Semantic Search** - Context-aware text retrieval
2. **RAG (Retrieval-Augmented Generation)** - LLM context retrieval
3. **Recommendation Systems** - Similar item discovery
4. **Image/Audio Retrieval** - Multimedia similarity search
5. **Semantic Caching** - Cache similar queries
6. **Classification** - Find similar labeled examples

### Integration Examples
- **LlamaIndex** - Document retrieval for RAG
- **LangChain** - Vector store backend
- Custom embedding models (OpenAI, Cohere, Hugging Face, etc.)

---

## Quick Reference Checklist

### Before Starting
- [ ] ScyllaDB Cloud account created
- [ ] API token generated and saved
- [ ] Account ID retrieved
- [ ] Cluster with version >= 2025.4.X

### Creating Vector Search Setup
- [ ] Create cluster with `vectorSearch` field OR deploy nodes to existing cluster
- [ ] Create keyspace with `tablets = { 'enabled': false }`
- [ ] Create table with `vector<float, N>` column
- [ ] Insert embedding data
- [ ] Create CUSTOM INDEX with 'vector_index'
- [ ] Connect via cqlsh and test ANN queries

### Important Reminders
- Vector dimensions must match table schema
- LIMIT clause required in ANN queries
- Embeddings must be generated externally
- Beta = 30-day node TTL
- No production use during beta

---

## External Resources

- **Main Docs:** https://cloud.docs.scylladb.com/stable/vector-search/index.html
- **Example Apps:** https://vector-search.scylladb.com/
- **CQL Vector Docs:** https://docs.scylladb.com/manual/stable/cql/types#vectors
- **Secondary Index Docs:** https://docs.scylladb.com/manual/stable/cql/secondary-indexes
- **ScyllaDB University:** https://university.scylladb.com
- **Community Forum:** https://forum.scylladb.com

---

## Glossary Quick Reference

- **Vector:** Ordered list of numbers representing data features
- **Embedding:** Vector generated by ML model to represent raw data numerically
- **Similarity Search:** Finding items most similar to query vector
- **Semantic Search:** Similarity search based on meaning/context
- **ANN:** Approximate Nearest Neighbor - fast similarity search with slight accuracy trade-off
- **HNSW:** Hierarchical Navigable Small World - graph-based ANN algorithm
- **USearch:** High-performance vector index library used by ScyllaDB
- **Distance Metric:** Mathematical function measuring vector similarity (Cosine, Dot Product, Euclidean)

---

**Document Created:** For re:Invent demo project  
**Project Path:** /Users/tdenton/Development/reinvent_demo
