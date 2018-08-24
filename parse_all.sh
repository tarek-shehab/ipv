#!/bin/bash

./etl_start.py --mode=parse --type=ipg
./etl_start.py --mode=parse --type=ipa
./etl_start.py --mode=parse --type=ad
./etl_start.py --mode=parse --type=att
./etl_start.py --mode=parse --type=fee

