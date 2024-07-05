#!/bin/bash

while :
do
    clear
    cat<<EOF
    ==============================
    Dapr benchmark driver
    ------------------------------
    Please enter your choice:

    (1) Generate Data
    (2) Ingest Data
    (3) Run Experiment
    (4) Ingest and Run
    (5) Refresh Configuration
    (6) Trim Streams
    (q) Quit
    ------------------------------
EOF
    read -n1 -s
    case "$REPLY" in
    "1")  curl -X POST localhost:8081/1 ;;
    "2")
            echo "Data ingestion requested"
            curl -X POST localhost:8081/2 ;;
    "3")    echo "Experiment run requested"
            curl -X POST localhost:8081/3 ;;
    "4")  curl -X POST localhost:8081/4 ;;
    "5") 
            echo "Enter the path of the configuration file. Remember to switch / by %2F"
            read json
            curl -X POST localhost:8081/6/$json
            ;;
    "6") 
            echo "Requesting trimming streams"
            curl -X POST localhost:8081/7
            ;;
    "Q")  exit  ;;
    "q")  exit  ;; 
     * )  echo "invalid option"     ;;
    esac
    #REPLY=
    sleep 1
done

