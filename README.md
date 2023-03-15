**Group ID:** G2

**Verification Code:** 49BA1E46A3917B7A048D2911A7636940

**Used Run Command:** ./start.sh

**Brief Description:** In order to allow node re-joins, we make sure to keep track of all its initial nodes as
well as its associated hashes. When a node determines which node to pull its membership list from, we specifically
allow nodes that were previously dead to allow them to re-join. Then, a node that re-joins will be re-added back to
our consistent hash. 

In the event that a node re-joins but keys were added while it was down, we implement a method such that the next node
in the consistent hash's node ring will transfer any keys that should belong to the re-joined node. This is done by
issuing multiple PUT commands in a separate thread to avoid blocking the main server thread that handles requests.