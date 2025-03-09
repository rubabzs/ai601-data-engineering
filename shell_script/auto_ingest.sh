#!/bin/bash

for day in {01..07}; do
  ../ingest_logs.sh "2023-09-$day"
done

