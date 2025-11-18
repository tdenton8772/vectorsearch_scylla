#!/usr/bin/env python3
"""
Check vector search setup
"""

import os
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

load_dotenv()

# Configuration
hosts = os.getenv('SCYLLADB_HOSTS', '').split(',')
port = int(os.getenv('SCYLLADB_PORT', 9042))
username = os.getenv('SCYLLADB_USERNAME')
password = os.getenv('SCYLLADB_PASSWORD')
keyspace = os.getenv('SCYLLADB_KEYSPACE', 'vectordemo')
table = os.getenv('SCYLLADB_TABLE', 'documents')

# Connect
print("🔌 Connecting...")
auth_provider = PlainTextAuthProvider(username=username, password=password)
cluster = Cluster(
    contact_points=hosts,
    port=port,
    auth_provider=auth_provider,
    load_balancing_policy=DCAwareRoundRobinPolicy(),
    protocol_version=4
)
session = cluster.connect(keyspace)
print("✅ Connected\n")

# Check table schema
print("📋 Checking table schema...")
table_query = f"""
    SELECT column_name, type
    FROM system_schema.columns
    WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'
"""
results = session.execute(table_query)
for row in results:
    print(f"   {row.column_name}: {row.type}")

# Check indexes
print(f"\n🔍 Checking indexes...")
index_query = f"""
    SELECT index_name, kind, options
    FROM system_schema.indexes
    WHERE keyspace_name = '{keyspace}' AND table_name = '{table}'
"""
results = session.execute(index_query)
indexes = list(results)
if indexes:
    for row in indexes:
        print(f"   Index: {row.index_name}")
        print(f"   Kind: {row.kind}")
        print(f"   Options: {row.options}")
else:
    print("   ⚠️  No indexes found!")

# Check row count
print(f"\n📊 Checking row count...")
count_query = f"SELECT COUNT(*) as count FROM {keyspace}.{table}"
result = session.execute(count_query).one()
print(f"   Total rows: {result.count}")

cluster.shutdown()
