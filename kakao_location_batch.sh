#!/bin/bash

cd /Users/junkiheo/PycharmProjects/SpartaBatch
source .venv/bin/activate
python kakao_main.py >> logs/kakao_batch_$(date +\%Y-\%m-\%d).log 2>&1
