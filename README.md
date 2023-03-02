**Group ID:** G2

**Verification Code:** 

**Used Run Command:**
./start.sh

**Brief Description:** 

We use a TreeMap for consistent hashing to keep track of the hash of the address of each node. Using a TreeMap allows us to get the next entry that is naturally (alphabetically) higher than the hash of the key in the key/value.

For group membership, we implement the pull epidemic protocol by creating a timer that runs every 0.5s which pulls membership info from a randomly selected node.
Node status is represented by the time it was last seen alive, so a node is determined to be dead when the time
it was last known to be alive has exceeded a threshold. A request for membership info to a node is handled similarly
to a get/put/rem request. This was done by creating a new command with the code 0x22.

**Proof of Immediate Crash/Termination Upon Shutdown Request:**
/src/main/java/com/g2/CPEN431/A7/Memory.java
Line 106