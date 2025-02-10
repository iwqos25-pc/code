#!/bin/bash
set -e

ARRAY=('10.0.0.2' '10.0.0.3' '10.0.0.5' '10.0.0.6' '10.0.0.7' '10.0.0.8' '10.0.0.9' '10.0.0.10' '10.0.0.11' 
        '10.0.0.12' '10.0.0.13' '10.0.0.14' '10.0.0.17' '10.0.0.18')
NUM=${#ARRAY[@]}
NUM=`expr $NUM - 1`
SRC_PATH1=/home/iwqos25-pc/code/run_cluster_sh/
SRC_PATH2=/home/iwqos25-pc/code/project
SRC_PATH3=/home/iwqos25-pc/wondershaper
SRC_PATH4=/home/iwqos25-pc/code/kill_proxy_datanode.sh

# DIR_NAME=run_memcached
DIS_DIR1=/home/iwqos25-pc/code
DIS_DIR2=/home/iwqos25-pc/code/storage
DIS_DIR3=/home/iwqos25-pc/wondershaper

set -e

if [ $1 == 3 ]; then
    ssh iwqos25-pc@10.0.0.17 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;sudo ./wondershaper/wondershaper/wondershaper -a ib0 -d 1000000'
    ssh iwqos25-pc@10.0.0.18 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;sudo ./wondershaper/wondershaper/wondershaper -a ib0 -d 500000'
elif [ $1 == 4 ]; then
    ssh iwqos25-pc@10.0.0.17 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;echo done'
    ssh iwqos25-pc@10.0.0.18 'sudo ./wondershaper/wondershaper/wondershaper -c -a ib0;echo done'
else
    echo "cluster_number:"${#ARRAY[@]}
    for i in $(seq 0 $NUM)
    do
    temp=${ARRAY[$i]}
        echo $temp
        if [ $1 == 0 ]; then
            if [ $temp == '10.0.0.17' ] || [ $temp == '10.0.0.18' ]; then
                ssh iwqos25-pc@$temp 'pkill -9 run_datanode;'
            else
                ssh iwqos25-pc@$temp 'pkill -9 run_datanode;pkill -9 run_proxy'
            fi
            echo 'pkill  all'
            ssh iwqos25-pc@$temp 'ps -aux | grep run_datanode | wc -l'
            ssh iwqos25-pc@$temp 'ps -aux | grep run_proxy | wc -l'
        elif [ $1 == 1 ]; then
            ssh iwqos25-pc@$temp 'cd /home/iwqos25-pc/code;bash cluster_run_proxy_datanode.sh'
            echo 'proxy_datanode process number:'
            ssh iwqos25-pc@$temp 'ps -aux |grep run_datanode | wc -l;ps -aux |grep run_proxy | wc -l'
        elif [ $1 == 2 ]; then
            ssh iwqos25-pc@$temp 'mkdir -p' ${DIS_DIR1}
            ssh iwqos25-pc@$temp 'mkdir -p' ${DIS_DIR2}
            ssh iwqos25-pc@$temp 'mkdir -p' ${DIS_DIR3}
            rsync -rtvpl ${SRC_PATH1}${i}/cluster_run_proxy_datanode.sh iwqos25-pc@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH2} iwqos25-pc@$temp:${DIS_DIR1}
            rsync -rtvpl ${SRC_PATH3} iwqos25-pc@$temp:${DIS_DIR3}
            rsync -rtvpl ${SRC_PATH4} iwqos25-pc@$temp:${DIS_DIR1}
        elif [ $1 == 5 ]; then
            ssh iwqos25-pc@$temp 'cd /home/iwqos25-pc/code/storage/;rm -r *'
        fi
    done
fi