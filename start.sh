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
trap cleanup INT
# get public IP using curl and save it to a variable
PUBLIC_IP=$(curl -s https://api.ipify.org/)
truncate -s 0 nodes.txt
truncate -s 0 servers.txt

for ((i = 4445; i < 4485; i++)); do
    echo $PUBLIC_IP >> nodes.txt
    echo $i >> nodes.txt
    echo "localhost:$i" >> servers.txt

done

for ((i = 4445; i < 4485; i++)); do
    java -jar -Xmx64m A10.jar $PUBLIC_IP $i &
done

wait