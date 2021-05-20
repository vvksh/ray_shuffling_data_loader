#!/bin/bash

# Installs the package and clones this repo to run the examples

export LC_ALL=C.UTF-8 && export LANG=C.UTF-8 && export LIBRARY_PATH=/usr/local/cuda/lib64/stubs && export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/nvidia/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/michelangelo/python_code/bin:/usr/local/openmpi/bin:/opt/hadoop/latest/bin:/opt/michelangelo/entrypoints && export LD_LIBRARY_PATH=/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64:/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64:/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server:/opt/hadoop/latest/lib/native:/usr/local/cuda/extras/CUPTI/lib64:/opt/michelangelo/python_code/lib:/usr/local/openmpi/lib:/usr/local/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64:/usr/lib/jvm/java-8-openjdk-amd64/jre/lib/amd64/server:/opt/hadoop/latest/lib/native:/usr/local/cuda/extras/CUPTI/lib64:/opt/michelangelo/python_code/lib:/usr/local/openmpi/lib:/usr/local/li
echo "installing from vvksh/ray_shuffling_data_loader"
pip3 install git+https://github.com/vvksh/ray_shuffling_data_loader.git@main#egg=ray_shuffling_data_loader

# optional for data generation
pip3 install fire

echo "Cloning repo"
git clone https://github.com/vvksh/ray_shuffling_data_loader.git
