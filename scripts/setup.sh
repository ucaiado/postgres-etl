#!/bin/sh

pushd .
cd ..
[[ -d data ]] || unzip data.zip
python create_tables.py
popd
