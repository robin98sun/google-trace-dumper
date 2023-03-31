#!/usr/bin/env bash

dir_base="./cluster_"
#for cluster in a b c d e f g h; do
for cluster in g ; do
    dir="${dir_base}${cluster}"
    mkdir -p ${dir}
    cd ${dir}
    echo "cluster ${cluster} started at `date`"
    for table in machine_events collection_events instance_events instance_usage; do
	echo "cluster: ${cluster}, table: ${table}, `date`"
	gsutil cp gs://clusterdata_2019_${cluster}/${table}-*.json.gz .
    done
    echo "cluster ${cluster} is done at `date`"
    cd ..
done
