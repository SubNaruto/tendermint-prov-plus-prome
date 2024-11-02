#!/bin/bash

pid=$(pgrep -f tendermint)
if [ -n "$pid" ]; then
    kill "$pid"
    echo "Terminated tendermint process with PID $pid."
else
    echo "No tendermint process is running."
fi
