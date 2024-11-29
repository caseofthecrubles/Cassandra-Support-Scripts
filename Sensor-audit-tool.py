from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import matplotlib.pyplot as plt
import numpy as np

# Connect to the Cassandra cluster
cluster = Cluster(['10.77.0.226'], auth_provider=PlainTextAuthProvider('cassandra', 'cassandra'))
session = cluster.connect('my_keyspace')  # Replace with your keyspace

# Query the Cassandra table
query = "SELECT * FROM temps_4_u_ASC"  # Replace with your table and columns
rows = session.execute(query)

values = []
# Store the data in a list
rownum = 0
data = {}
for row in rows:
    rownum = rownum ++ 1
    print(row[2],rownum)
    temp = row[2]
    data.update({rownum: temp})
    #data_list[rownum] = row # Convert each row to a dictionary if neededi

rownumbers = list(data.keys())
values = list(data.values())

# Create a scatter plot
plt.scatter(rownumbers, values)

# Close the connection
cluster.shutdown()

# Add titles and labels
plt.title('Large scale single sensor audit')
plt.xlabel('secconds')
plt.ylabel('temps')

# Display the chart
plt.show()