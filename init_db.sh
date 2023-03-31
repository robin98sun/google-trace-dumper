#!/usr/bin/env bash


CMD="$1" # either "unzip" or "insert"

if [[ "$CMD" == "" ]];then
    echo "please specify a command, either unzip or insert"
    exit 1
fi

DB_CONN="mongodb://localhost:27017"
db="google-trace-2019"

if [[ "$CMD" == "insert" ]];then
    is_mongoimport_ready=`which mongoimport|wc -l|awk '{print $1}'`
    if [[ "$is_mongoimport_ready" -ne 1 ]];then
       echo "mongoimport is not found"
       exit 1
    fi
fi

dir_base="./cluster_"
tmp_dir="./tmp"
if [[ ! -d "${tmp_dir}" ]];then
    mkdir -p ${tmp_dir}
fi

start=`date +"%s"`
#for cluster in a b c d e f g h; do
for cluster in g ; do
    dir="${dir_base}${cluster}"
    if [[ ! -d "$dir" ]];then
        echo "raw data directory $dir does not exist"
        exit 1
    fi
    echo "cluster ${cluster} started at `date`"

    if [[ "$CMD" == "unzip" ]];then    
        echo "unzipping gz files..."
        gunzip ${dir}/*.gz
        echo "unzipping is done at `date`"
    elif [[ "$CMD" == "insert" ]];then
        
        for table in machine_events collection_events instance_events instance_usage; do
            file_base=${dir}/${table}
            ls ${file_base}*|while read file; do
                echo "cluster: ${cluster}, table: ${table}, file: ${file}; `date`"
                start_time=`date +"%s"`
                if [[ "${file}" =~ ".gz"$ ]];then
                    echo "    unzipping file $file"
                    gunzip $file

                    if [[ $? != 0 ]];then
                        exit $?
                    fi

                    unzipped_file=`echo $file|awk '{for(i=1;i<NF;i++){if(i==1){printf($i)}else{printf("."$i)}}}' FS="."`
                    echo "    unzipped to $unzipped_file"
                    file=$unzipped_file
                fi
                if [[ "${file}" =~ ".json"$ && -f $file ]];then
                    filename=`echo ${file}|awk '{print $NF}' FS='/'`
                   
                    collection=`echo $filename|awk '{print $1}' FS="-"` 
                    echo "   importing $file into database $db"
                    cmd="mongoimport --host localhost --db $db --collection $collection --file $file"
                    echo "   cmd: $cmd"
                    $cmd

                    if [[ $? != 0 ]];then 
                        exit $?
                    fi

                    tmpfile="${tmp_dir}/$filename"
                    mv $file $tmpfile
                    echo "   moved $file to $tmpfile"

                fi
                end_time=`date +"%s"`
                echo "file: ${file} imported in `expr $end_time - $start_time` seconds"
            done
        done
    fi
    echo "cluster ${cluster} is done at `date`"
done
echo "all done at `date`"
#echo "rm -rf ${tmp_dir}"
#rm -rf ${tmp_dir}
end=`date +"%s"`
echo "all done in `expr $end - $start` seconds"
