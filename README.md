# stmdb
Python code to manage STM images database

Usage
-----

```
# Import and open connection to the database
# (provide the path to your ssh key)
import stmdb
db = stmdb.connection(host="the_host", user="the_user", ssh_key_path="my/ssh/key/path")

# Get tags of image with ID=1
db.get_tags(1)

# Set tags of image with ID=1
db.set_tags(1, 'honeycomb')

# Append tags to image with ID=1
db.append_tags(1, 'lattice')

# Get metadata of image with ID=1
db.get_meta(1)

# Close the connection
db.disconnect()
```

Prerequisites
-------------
Dependencies needed:
``paramiko``
``scp``
