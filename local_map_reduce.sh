#!/bin/bash
#===============================================================================
#
#          FILE:  local_map_reduce.sh
# 
#         USAGE:  ./local_map_reduce.sh 
# 
#   DESCRIPTION:  parsing the event with local map reduce
# 
#       OPTIONS:  ---
#  REQUIREMENTS:  ---
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  Maxwell Weixuan Mao (), maxwell.mao@hotmail.com
#       COMPANY:  
#       VERSION:  1.0
#       CREATED:  03/23/2014 10:05:48 AM EDT
#      REVISION:  ---
#===============================================================================

declare -a pid_list
check_pid(){
    unset pid_list
    i=0
    until [ $# -eq 0 ]
    do
        count=`ps | awk '{print $1}' | grep $1 | wc -l`
        if [ $count -gt 0 ];then
            pid_list[${#pid_list[@]}]=$1
        fi
        shift
        i=$(expr $i+1)
    done
}

monthly_parsing(){
    year=$1
    month=$2
    result_path=$3
    > $result_path
    for file in archives/${year}-${month}*.json.gz
    do
#        echo ${file##*/}
        zcat $file | python event_stat.py --map >> $result_path
    done
    awk '{a[$1, $2]+=$3};END{for(item in a){split(item, itemstr, SUBSEP);print itemstr[1], itemstr[2], a[item]}}' $result_path > ${result_path}_reduce
    rm -f $result_path
}



if [ ! -d query_result ];
then
    mkdir query_result
fi
declare -i max_pid
max_pid=20
for query_year in `ls archives/*.json.gz | awk '{split($1, a, "/");split(a[2], b, "-");print b[1]}' | sort -u`
do
    for query_month in `ls archives/${query_year}* | awk '{split($1, a, "-");print a[2]}' | sort -u`
    do
        echo $query_year-$query_month
        monthly_parsing $query_year $query_month query_result/${query_year}-${query_month} &
#       pid=$!
#       pid_list[${#pid_list[@]}]=$query_month
        pid_list[${#pid_list[@]}]=$!
        until ((${#pid_list[@]}<$max_pid))
        do
            echo "Waiting"
            sleep 60
            check_pid ${pid_list[*]}
            echo "Current running process:" ${#pid_list[@]}, "Process pool:" $max_pid
        done
        echo "Current running process:" ${#pid_list[@]}, "Process pool:" $max_pid
    done
done
until ((${#pid_list[@]}==0))
do
    sleep 10
    check_pid ${pid_list[*]}
    echo "Current running process:" ${#pid_list[@]}, "Process pool:" $max_pid
done
