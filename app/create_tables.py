from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import subprocess

CASSANDRA_HOST = 'cassandra-server'  # Docker name
CASSANDRA_PORT = 9042         # Default Cassandra port
HDFS_OUTPUT_PATH = '/user/root/tmp/document_frequency_word_within_one_doc.output1/part-00000'
# Connect to Cassandra
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
session = cluster.connect()

# Create keyspace and tables
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

# session.execute("""
#     DROP TABLE IF EXISTS document_frequency;
#     DROP TABLE IF EXISTS word_document_frequency;
#     DROP TABLE IF EXISTS doc_id_len;
# """)

session.set_keyspace('search_engine')

session.execute("""
    CREATE TABLE IF NOT EXISTS document_frequency (
        word TEXT PRIMARY KEY,
        count INT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS word_document_counts (
        word TEXT,
        document_id INT,
        count INT,
        PRIMARY KEY (word, document_id)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS doc_id_len (
        document_id INT,
        length INT,
        PRIMARY KEY (document_id)
    )
""")

# Prepare statements
insert_df = session.prepare(
    "INSERT INTO document_frequency (word, count) VALUES (?, ?)"
)
insert_wdc = session.prepare(
    "INSERT INTO word_document_counts (word, document_id, count) VALUES (?, ?, ?)"
)
insert_dil=session.prepare(
    "INSERT INTO doc_id_len (document_id, length) VALUES (?,?)"
)

# Read Hadoop output directly from HDFS
hdfs_cat = subprocess.Popen(
    ['hdfs', 'dfs', '-cat', f'{HDFS_OUTPUT_PATH}'],
    stdout=subprocess.PIPE,
    universal_newlines=False
)

# Process and load data
for line in hdfs_cat.stdout:
    line = line.decode('ascii', errors='ignore').strip()
    if not line:
        continue
    
    if line.startswith('document_frequency'):
        _, word, count = line.split()
        session.execute(insert_df, (word, int(count)))
    elif line.startswith('word_within_one_document_count'):
        _, word, doc_id, count = line.split()
        session.execute(insert_wdc, (word, int(doc_id), int(count)))
    elif line.startswith('doc_id_len'):
        _, doc_id, doc_len=line.split()
        session.execute(insert_dil, (int(doc_id), int(doc_len)))
    
cluster.shutdown()
print("Data loading completed successfully!")