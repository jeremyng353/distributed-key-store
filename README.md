**Group ID:** G2

**Verification Code:**
2AB31C778959F28EA6DC4EB7189E63E9

**Used Run Command:**
./start.sh

**Brief Description:**
We're using chain replication to get a replication factor of four. Writes are sent to the head of the chain, while reads are sent to the tail of the chain. The only thing that differs from the slide set is the fact that we iterate backwards through the chain if the tail is missing the data.

We also tried to implement physical timestamps to ensure that the messages are parsed in sequential order. However, due to time constraints, we were unable to fully debug and integrate it with the rest of our system.