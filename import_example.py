#basic cassandra import test
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

# Initialize the Cassandra cluster with pooling
def create_session():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['10.77.0.226'], auth_provider=auth_provider)
    session = cluster.connect('my_keyspace')
    return session

# Insert data using a prepared statement
def insert_data(session):
    insert_stmt = session.prepare("INSERT INTO python_ages (id, name, age) VALUES (?, ?, ?)")
    session.execute(insert_stmt, (1, 'Alice', 30))
    print("Data inserted successfully.")

# Select data using a prepared statement
def select_data(session):
    select_stmt = session.prepare("SELECT id, name, age FROM python_ages WHERE id = ?")
    result = session.execute(select_stmt, (1,))
    for row in result:
        print(f"ID: {row.id}, Name: {row.name}, Age: {row.age}")

# Main function to execute insert and select
def main():
    session = create_session()
    try:
        insert_data(session)
        select_data(session)
    finally:
        session.cluster.shutdown()
        session.shutdown()

if __name__ == "__main__":
    main()
