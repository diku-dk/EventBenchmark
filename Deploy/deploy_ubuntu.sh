#!/bin/bash

if `echo "$*" | grep -q Silo`; then
    echo "Spawning Orleans Silo..."
    xfce4-terminal -e 'bash -c "~/.dotnet/dotnet run --project ../Silo; bash"' -T "Silo"
    echo "Waiting a few seconds for Silo startup..."
    sleep 5
fi

if `echo "$*" | grep -q Client`; then
    echo "Spawning Orleans Client (aka Driver)..."
    xfce4-terminal -e 'bash -c "~/.dotnet/dotnet run --project ../Client ../Configuration; bash"' -T "Client"
fi