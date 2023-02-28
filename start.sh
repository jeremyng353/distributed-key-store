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

for ((i = 4445; i < 4450; i++)); do
    java -jar -Xmx64m A6.jar $i &
done

wait