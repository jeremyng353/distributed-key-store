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

for ((i = 4445; i < 4465; i++)); do
    echo $PUBLIC_IP >> nodes.txt
    echo $i >> nodes.txt

done

for ((i = 4445; i < 4465; i++)); do
    java -jar -Xmx64m A9.jar $PUBLIC_IP $i &
done

wait