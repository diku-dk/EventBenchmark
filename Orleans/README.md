# Orleans Driver
======================================

## How to run
dotnet run --project Orleans Configuration/orleans_prod.json

## Too many files open

If you encounter the exception message "Too many open files in system," try to modify the soft limit
ulimit -n 4096

## Number of workers for ingestion
Should not pick a number larger than the number of CPUs, otherwise it will run into degradation

