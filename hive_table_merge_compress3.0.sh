#!/bin/bash
##########################################
# author: zhoukui
# date: 2019-02-28
# function: merger and compress hive table 
# describe:
# 处理集群小文件、合并和压缩集群hive表
# 清理级别：按时间合并和压缩
# 用法：bash merge-compress-hive-table3.0.sh [option]... 
# [option]:
#          -d|--database   数据库 如：--database test (注：最多支持清理3年的，年份之间需要用1个空格分割且用""标注)
#          -t|--table      表     如：--table test (注：hdfs上所有表路径下的.hive_stage临时文件将被清理，请谨慎使用！)
#          -p|--partition  分区   如：--partition p_event_date or --partition "p_event_date p_event_date2"  
#                                  (注：多个分区需要用1个空格分开且用""标注，最多支持3个分区且第一个分区必须为日期格式)
#          -k|--key        日期字段 如：--key p_event_date (注：过滤日期字段)
#          -s|--dates      日期   如：--dates 2019-03-01 or "2019-03-01 2019-03-02" (注：最多支持2个日期，日期之间用1个空格分割且用""标注)
#          -i|--internal   间隔   如：--internal 30 (注：压缩天数，表示整个表按30天为周期循环合并)
#          -j|--jdbc       hiveserver2 url 如：--jdbc "node1.ludp2.lenovo.com" (注：不写，默认为"node1.ludp2.lenovo.com")
#          -v|--version    版本
#          -h|--help       帮助 
# 使用方式（两种）：
# 1、按合并天数合并和压缩，如：bash merge-compress-hive-table3.0.sh --database test --table test --partition p_event_date  --internal 30 (注：中断使用--half 1 继续)
# 2、按日期合并和压缩，    如：bash merge-compress-hive-table3.0.sh --database test --table test --partition p_event_date  --dates "2017-12-22 2018-12-22"
# 注：如果时间分区不是第一个分区，请加上”--key 日期字段“ (仅对方式2有效)
# 如：bash merge-compress-hive-table3.0.sh --database test --table test --partition p_event_date  --dates "2017-12-22 2018-12-22" --key p_event_date
########################################## 
#set -x
show_usage()
{
        echo -e "`printf %-16s "Usage: $0"` [option]..."
        echo -e "`printf %-16s ` [-d|--database]"
        echo -e "`printf %-16s ` [-t|--table]"
        echo -e "`printf %-16s ` [-p|--partition]"
        echo -e "`printf %-16s ` [-k|--key]"
        echo -e "`printf %-16s ` [-s|--dates]"
        echo -e "`printf %-16s ` [-i|--internal]"
        echo -e "`printf %-16s ` [-j|--jdbc]"
		echo -e "`printf %-16s ` [-f|--half]"
        echo -e "`printf %-16s ` [-v|--version]"
        echo -e "`printf %-16s ` [-h|--help]"
describe=\
'''
 describe:
 处理集群小文件、合并和压缩集群hive表
 清理级别：按时间合并和压缩
 用法：bash merge-compress-hive-table3.0.sh [option]...
 [option]:
          -d|--database   数据库 如：--database test (注：最多支持清理3年的，年份之间需要用1个空格分割且用""标注)
          -t|--table      表     如：--table test (注：hdfs上所有表路径下的.hive_stage临时文件将被清理，请谨慎使用！)
          -p|--partition  分区   如：--partition p_event_date or --partition "p_event_date p_event_date2"
                                  (注：多个分区需要用1个空格分开且用""标注，最多支持3个分区且第一个分区必须为日期格式)
          -k|--key        日期字段 如：--key p_event_date (注：过滤日期字段)
          -s|--dates      日期   如：--dates 2019-03-01 or "2019-03-01 2019-03-02" (注：最多支持2个日期，日期之间用1个空格分割且用""标注)
          -i|--internal   间隔   如：--internal 30 (注：压缩天数，表示整个表按30天为周期循环合并)
          -j|--jdbc       hiveserver2 url 如：--jdbc "node1.ludp2.lenovo.com" (注：不写，默认为"node1.ludp2.lenovo.com")
	  -f|--half       中断继续 如： --half 1
          -v|--version    版本
          -h|--help       帮助
 使用方式（两种）：
 1、按合并天数合并和压缩，如：bash merge-compress-hive-table3.0.sh --database test --table test --partition p_event_date  --internal 30 (注：中断使用--half 1 继续)
 2、按日期合并和压缩，    如：bash merge-compress-hive-table3.0.sh --database test --table test --partition p_event_date  --dates "2017-12-22 2018-12-22"
 注：如果时间分区不是第一个分区，请加上”--key 日期字段“ (仅对方式2有效)
 如：bash merge-compress-hive-table3.0.sh --database test --table test --partition p_event_date  --dates "2017-12-22 2018-12-22" --key p_event_date
'''
     echo -e "\033[32m${describe}\033[0m\n"
        exit 0
}
show_version(){
echo -e "`printf %-1s$0:` 1.0"
exit 1
}
#ARGS=`getopt -a -o d:t:p:k:vh -l database:,table:,partition:,key:,version,help --  "$@" 2>/dev/null`
ARGS=`getopt -a -o d:t:p:k:i:j:f:,vh -l database:,table:,partition:,key:,dates:,internal:,jdbc:,half:,version,help -n 'ERROR' -- "$@"`
if [ $? -ne 0 ] ;then
 show_usage
fi
partition1=''
partition2=''
partition3=''
partitionedstring(){
    partstring=$1
    # split 
    OLD_IFS="$IFS"
    IFS=" ";set -- $partstring;part1=$1;part2=$2;part3=$3;IFS=$OIFS 
    echo $part1 $part2 $part3

    array=($part1 $part2 $part3)
    echo ${array[0]}
    length=`echo ${#array[@]}`
    if [ $length -eq 1 ];then
       partition1=${array[0]}
    elif [ $length -eq 2 ];then
       partition1=${array[0]}
       partition2=${array[1]}
    elif [ $length -eq 3 ];then
       partition1=${array[0]}
       partition2=${array[1]}
       partition3=${array[2]}
    else
       echo "partiton error"
       exit 0
    fi
    unset -v IFS
    return $length
}
p_field_date=''
p_event_date1=''
p_event_date2=''
datestring(){
    dtstring=$1
    
    OLD_IFS="$IFS"
    IFS=" ";set -- $dtstring;dt1=$1;dt2=$2;IFS=$OIFS 
    echo $dt1 $dt2

    array=($dt1 $dt2)
    echo ${array[0]}
    dlength=`echo ${#array[@]}`
    if [ ${dlength} -eq 1 ];then
       p_event_date1=${array[0]}
    elif [ ${dlength} -eq 2 ];then
       p_event_date1=${array[0]}
       p_event_date2=${array[1]}
    else
       echo "date error"
       exit 0
    fi
    unset -v IFS
    return ${dlength}
}
jdbc=''
internal=''
partitionnumber=0
datenumber=0
half=0
eval set -- "${ARGS}"
while true
do
        case "$1" in
        -d|--database)
                database="$2"
                shift
                ;;
        -t|--table)
                table="$2"
                shift
                ;;
        -p|--partition)
                pn="$2"
                partitionedstring "$pn"
                #checkpartition $pn 
                partitionnumber=`echo $?`
                shift
                ;;
                -k|--key)
                p_field_date="$2"
                shift
                ;;
        -s|--dates)
                dt="$2"
                datestring "$dt"
                datenumber=`echo $?`
                shift
                ;;
                -i|--internal)
                internal="$2"
                shift
                ;;
                -j|--jdbc)
                jdbc="$2"
                shift
                ;;
                -f|--half)
                half="$2"
                shift
                ;;
        -v|--version)
                show_version
                ;;
        -h|--help)
                show_usage
                ;;
              --)
                shift
                break
                ;;
               *)
               echo -e "\033[31mERROR: unrecognized option!  \033[0m\n" 
               show_usage 
                ;;
        esac
shift
done 
echo $database"."$table" "$pn" "$key


#unset -v IFS
set -o nounset   
set -o errexit   


jdbcurl="node4.tj.leap.com"
if [ a${jdbc} != a'' ];then
   jdbcurl=${jdbc}
fi
htime=`echo $[$(date +%s%N)/1000000]`
#yearmonth=`date +%Y-%m`
time_file=/tmp/merger-compress-table-time-${htime}.txt
time_file0=/tmp/merger-compress-table-time0-${htime}.txt
time_file1=/tmp/merger-compress-table-time1-${htime}.txt
time_file_log=/tmp/merger-compress-table-${htime}.txt
echo "export time_file0="${time_file0} > /tmp/time_hive_merge_compress.txt
echo "export time_file1"=${time_file1} >> /tmp/time_hive_merge_compress.txt
databasedb=${database}.db
echo ${database}
db_table=${database}'.'${table}
echo ${db_table}
function datewritefile(){
#database=`echo ${db%%.*}`
# clean env
unset -v IFS
rm -rf ${time_file}
#rm -rf ${time_file0}
#rm -rf ${time_file1}
rm -rf ${time_file_log}
#exit 0
list_dates=`sudo -u hdfs hdfs dfs -ls  /apps/hive/warehouse/${databasedb}/${table}/  |awk -F ' ' '{print $8}' |awk -F '/' '{print $7}'|awk -F '=' '{print $2}'|sort -u`
echo ${list_dates}
init=1
bool="true"
endnum=1
endnumber=1
for dn in ${list_dates}
do 
  if [ a$dn != a'' ];then
     let endnum++
  fi
done
#endnum=length()
for dt in ${list_dates}
do
  if [ ${bool} == "true" ];then
     echo ${dt}>>${time_file}
         bool="false"
  fi
  if [ a"${init}" == a"${internal}" ];then
        echo ${dt}>>${time_file}
        init=1
  fi
  if [ a"$((${endnumber}+1))" == a"${endnum}" ];then
     echo ${dt}>>${time_file}
         break
  fi
let init++
if [ a${dt} != a'' ];then

   let endnumber++
fi
done
sed -i '/^$/d' ${time_file}
#获取奇偶行
awk 'NR%2' ${time_file} >${time_file1}
awk '!(NR%2)' ${time_file} >${time_file0}
}

allpartition=''
if [ ${partitionnumber} -eq 1 ];then 
    allpartition=${partition1}
elif [ ${partitionnumber} -eq 2 ];then
    allpartition=${partition1}","${partition2}
elif [ $partitionnumber -eq 3 ];then
      allpartition=${partition1}","${partition2}","${partition3}
else 
      exit 0
fi

function table_partition(){
#exit 0
allpartitions=$1
tb_partition=$2
#exit 0

#取偶数第一行
end_date=`head -n 1 ${time_file0}`
#遍历日期
#exit 0
while [ a"${end_date}" != a"" ]
do
#取奇数第一行
init_date=`head -n 1 ${time_file1}`
#取第一行
#end_date=`head -n 1 /tmp/tmp_clean2.txt`
#第一个SQL
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=256000000 ;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat ;
set hive.merge.mapfiles = true ;
set hive.merge.mapredfiles= true ;
set hive.exec.max.dynamic.partitions.pernode=20000;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.created.files=40000;
set hive.merge.size.per.task = 256000000 ;
set hive.merge.smallfiles.avgsize=256000000 ;
set hive.exec.compress.output=true; 
set mapreduce.output.fileoutputformat.compress=true ;
set mapreduce.output.fileoutputformat.compress.type=BLOCK ;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec ;
set hive.hadoop.supports.splittable.combineinputformat=true;
insert overwrite TABLE ${db_table}
partition(${allpartitions})
select  * from ${db_table} where ${tb_partition} >='${init_date}' and  ${tb_partition} < '${end_date}';"

starttime=`date`
echo -e "\033[32m${starttime} INFO: ${sql} \033[0m\n"

#执行SQL
/usr/bin/beeline -u jdbc:hive2://${jdbcurl}:10000 -n hive -e "${sql}"
#echo "${sql}"
echo "${sql}" >>${time_file_log}
#删除第一行之前截取end date
#end_date=`head -n 1 /tmp/tmp_clean2.txt`
#删除第一行
sed -i '1d' ${time_file0}
sed -i '1d' ${time_file1}
#截取奇数文件的第一行
init_date=`head -n 1 ${time_file1}`

#第二个SQL
sql2="
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=256000000 ;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat ;
set hive.merge.mapfiles = true ;
set hive.merge.mapredfiles= true ;
set hive.exec.max.dynamic.partitions.pernode=20000;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.created.files=40000;
set hive.merge.size.per.task = 256000000 ;
set hive.merge.smallfiles.avgsize=256000000 ;
set hive.exec.compress.output=true; 
set mapreduce.output.fileoutputformat.compress=true ;
set mapreduce.output.fileoutputformat.compress.type=BLOCK ;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec ;
set hive.hadoop.supports.splittable.combineinputformat=true;

insert overwrite TABLE ${db_table}
partition(${allpartitions})
select  * from ${db_table} where ${tb_partition} >='${end_date}' and  ${tb_partition} < '${init_date}';"

starttime=`date`
echo -e "\033[32m${starttime} INFO: ${sql2} \033[0m\n"

#执行第二个SQL语句
/usr/bin/beeline -u jdbc:hive2://${jdbcurl}:10000 -n hive -e "${sql2}"
#echo "${sql2}"
echo "${sql2}" >>${time_file_log}
#截取偶数文件的第一行
end_date=`head -n 1 ${time_file0}`
done

}

function scheduleddate(){
allpartitions=$1
tb_partition=$2
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=256000000 ;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat ;
set hive.merge.mapfiles = true ;
set hive.merge.mapredfiles= true ;
set hive.exec.max.dynamic.partitions.pernode=20000;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.created.files=40000;
set hive.merge.size.per.task = 256000000 ;
set hive.merge.smallfiles.avgsize=256000000 ;
set hive.exec.compress.output=true; 
set mapreduce.output.fileoutputformat.compress=true ;
set mapreduce.output.fileoutputformat.compress.type=BLOCK ;
set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec ;
set hive.hadoop.supports.splittable.combineinputformat=true;
insert overwrite TABLE ${db_table}
partition(${allpartitions})
select  * from ${db_table} where ${tb_partition} >='${p_event_date1}' and  ${tb_partition} <= '${p_event_date2}';"

starttime=`date`
echo -e "\033[32m${starttime} INFO: ${sql} \033[0m\n"

#执行SQL
/usr/bin/beeline -u jdbc:hive2://${jdbcurl}:10000 -n hive -e "${sql}"
#echo "${sql}"
echo "${sql}" >>${time_file_log}
}
if [ a${partition1} == a"" ];then
    echo -e "\033[31m${starttime} ERROR: ${partition1} unrecognized option! \033[0m\n"
    exit 1

fi

starttime=`date`
echo -e "\033[31m${starttime} Info: start ===> \033[0m\n"
starttime=`date`
if [ ${datenumber} -eq 2 ];then
   if [ a${p_field_date} != a'' ];then
      scheduleddate "${allpartition}" "${p_field_date}"
        else 
          
          scheduleddate "${allpartition}" "${partition1}"
   fi
   
# echo -e "\033[31m${starttime} Info: merge and compress ...\033[0m\n"
elif [ ${datenumber} -eq 0 ];then
   if [ ${partitionnumber} -ge 1 ] && [ ${partitionnumber} -le 3 ];then
    
    if [ ${half} -eq 0 ];then
	  # write file for dates 
	  datewritefile
    else
      source /tmp/time_hive_merge_compress.txt
    fi
    table_partition "${allpartition}" "${partition1}"
        echo -e "\033[31m${starttime} Info: merge and compress ...\033[0m\n"
   else 
     echo -e "\033[31m${starttime} ERROR: partitionnumber ${partitionnumber} unrecognized option! \033[0m\n"
     exit 0
   fi
else
  echo -e "\033[31m${starttime} ERROR: datenumber ${datenumber} unrecognized option! \033[0m\n"
  exit 0
   
fi

endtime=`date`
echo "See "${time_file_log}" for more information."
echo -e "\033[31m${endtime} Info: end <===\033[0m\n"
