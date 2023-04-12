#!/bin/bash

cleanup() {
        kill -HUP -$$
}

hupexit() {
        echo
        echo "Interrupted"
        exit
}

trap hupexit HUP
trap cleanup INT TERM

NODE_BASE_PORT=4445
NUM_NODES=$1
# get public IP using curl and save it to a variable
PUBLIC_IP=$(curl -s https://api.ipify.org/)
truncate -s 0 nodes.txt

for ((i = $NODE_BASE_PORT; i < $NODE_BASE_PORT + $NUM_NODES; i++)); do
    echo $PUBLIC_IP >> nodes.txt
    echo $i >> nodes.txt

done

for ((i = $NODE_BASE_PORT; i < $NODE_BASE_PORT + $NUM_NODES; i++)); do
    java -jar -Xmx64m A11.jar $PUBLIC_IP $i &
done

wait