#!/bin/bash

cd /app/Celebi
python -m build
pip install -e .

cd /app/Yuki
python -m build
pip install -e .

cd /app
