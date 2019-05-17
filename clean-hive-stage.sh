#!/bin/bash
##########################################
# author: zhoukui
# date: 2019-02-28
# function: clean hive .hive-stage... file
# describe:
# 清理集群hive-stage临时文件
# 清理级别：按年清理
# 用法：bash clean-hive-stage.sh [option]... 
# [option]:
#          --y|--year 年份 如：--year 2018 or --year "2017 2018" (注：最多支持清理3年的，年份之间需要用1个空格分割且用""标注)
#          --a|--all  所有 如：--all  all (注：hdfs上所有表路径下的.hive_stage临时文件将被清理，请谨慎使用！)
########################################## 
set -x
show_usage()
{
        echo -e "`printf %-16s "Usage: $0"` [option]..."
        echo -e "`printf %-16s ` [-y|--year]"
        echo -e "`printf %-16s ` [-a|--all]"
        echo -e "`printf %-16s ` [-v|--version]"
        echo -e "`printf %-16s ` [-h|--help]"
describe=\
'''
 describe:
 清理集群hive-stage临时文件
 清理级别：按年清理
 用法：bash clean-hive-stage.sh [option]... 
 [option]:
          --y|--year 年份 如：--year 2018 or --year "2017 2018" (注：最多支持清理3年的，年份之间需要用1个空格分割且用""标注)
          --a|--all  所有 如：--all  all (注：hdfs上所有表路径下的.hive_stage临时文件将被清理，请谨慎使用！)
'''
    echo -e "\033[32m${describe}\033[0m\n"    
    exit 0
}
show_version(){
echo -e "`printf %-1s$0:` 1.0"
exit 1
}
#ARGS=`getopt -a -o d:t:p:k:vh -l database:,table:,partition:,key:,version,help --  "$@" 2>/dev/null`
ARGS=`getopt -a -o y:a:,vh -l year:,all:,version,help -n 'ERROR' -- "$@"`
if [ $? -ne 0 ] ;then
 show_usage
fi
#year1=''
#year2=''
#year3=''
yearstring(){
    partstring=$1
    OLD_IFS="$IFS"
    IFS=" ";set -- $partstring;part1=$1;part2=$2;part3=$3;IFS=$OIFS 
    echo $part1 $part2 $part3

    array=($part1 $part2 $part3)
    echo ${array[0]}
    length=`echo ${#array[@]}`
    if [ $length -eq 1 ];then
       year1=${array[0]}
    elif [ $length -eq 2 ];then
       year1=${array[0]}
       year2=${array[1]}
    elif [ $length -eq 3 ];then
       year1=${array[0]}
       year2=${array[1]}
       year3=${array[2]}
    else
       echo "year error"
       exit 0
    fi
    return $length
}
eval set -- "${ARGS}"
while true
do
        case "$1" in
        -y|--year)
                year="$2"
		yearstring "$year"
		yearnums=`echo $?`
                shift
                ;;
        -a|--all)
                all_time="$2"
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
#echo $year" "$all_time
#set -o nounset
#set -o errexit

stime=`echo $[$(date +%s%N)/1000000]`
yearmonth=`date +%Y-%m`
#currenttime=`date`
#halfstage="/.hive-staging_hive_"${year}
filter=".hive-staging_hive_"${yearmonth}

function functionyear(){

halfstage="/.hive-staging_hive_"${year1}
#unset -v IFS
echo $halfstage
database=`sudo -u hdfs hdfs dfs -ls   /apps/hive/warehouse/ |awk -F ' ' '{print $8}' |grep -v  "^$" |grep ".db"`
#echo $database
for dbs in $database
do
   #echo "sudo -u hdfs hdfs dfs -ls -h  ${dbs}  |awk -F ' ' '{print $8}'"
   db_table=`sudo -u hdfs hdfs dfs -ls -h  ${dbs} |awk -F ' ' '{print $8}'`
#   echo $db_table
   for db_ta in ${db_table}
   do
                 echo ${db_ta} >> /tmp/table-${stime}.txt
                 #echo ${db_ta}
                 #dirs=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep -v ".hive-staging_hive_2019"|awk -F ' ' '{print $8}' `
                                 hivesg=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep ${halfstage} |grep -v "${filter}"|awk -F ' ' '{print $8}' |wc -l`
                                 if [ ${hivesg} -gt 0 ];then
						           rm_halfstage=${halfstage}"*"
                                                           currenttime=`date`
                                                           echo "#sudo -u  hive hdfs dfs -rm -r "${db_ta}${rm_halfstage} >> /tmp/clean-hive-tmp-${stime}.log 2>&1
							   echo -e "\033[31m${currenttime} Info: delete "${db_ta}${rm_halfstage}"\033[0m\n"
                                                           sleep 8
                                                           sudo -u  hive hdfs dfs -rm -r ${db_ta}${rm_halfstage}
                                                           
                                 fi
								
                                                                 
                 sleep 3
    done
done
}


function functionall(){

halfstageall="/.hive-staging_hive"
unset -v IFS
database=`sudo -u hdfs hdfs dfs -ls   /apps/hive/warehouse/ |awk -F ' ' '{print $8}' |grep -v  "^$" |grep ".db"`
echo $database
for databs in ${database}
do
   db_table=`sudo -u hdfs hdfs dfs -ls -h  ${databs}  |awk -F ' ' '{print $8}'`
   for db_ta in ${db_table}
   do
                 echo ${db_ta} >> /tmp/table-${stime}.txt
                 #echo ${db_ta}
                 #dirs=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep -v ".hive-staging_hive_2019"|awk -F ' ' '{print $8}' `
                                 hivesg=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive" |grep -v "${filter}"|awk -F ' ' '{print $8}' |wc -l`
                                 if [ ${hivesg} -gt 0 ];then
						           rm_halfstageall=${halfstageall}"*"
                                                           currenttime=`date`            
                                                           echo "#sudo -u  hive hdfs dfs -rm -r "${db_ta}${rm_halfstageall} >> /tmp/clean-hive-tmp-${stime}.log 2>&1
                                                           echo -e "\033[31mi${currenttime} Info: delete "${db_ta}${rm_halfstageall}"\033[0m\n"
							   sleep 8
                                                           sudo -u  hive hdfs dfs -rm -r ${db_ta}${rm_halfstageall}
                                                           
                                 fi
								
                                                                 
                 sleep 1
    done
done
}
function functionyear2(){
halfstage1="/.hive-staging_hive_"${year1}
halfstage2="/.hive-staging_hive_"${year2}
unset -v IFS
echo $halfstage1 $halfstage2
database=`sudo -u hdfs hdfs dfs -ls   /apps/hive/warehouse/ |awk -F ' ' '{print $8}' |grep -v  "^$" |grep ".db"`
echo $database
for databs in ${database}
do
   db_table=`sudo -u hdfs hdfs dfs -ls -h  ${databs}  |awk -F ' ' '{print $8}'`
   for db_ta in ${db_table}
   do
                 echo ${db_ta} >> /tmp/table-${stime}.txt
                 #echo ${db_ta}
                 #dirs=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep -v ".hive-staging_hive_2019"|awk -F ' ' '{print $8}' `
                                 hivesg=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep ${halfstage1} |grep -v "${filter}"|awk -F ' ' '{print $8}' |wc -l`
								 
                                 if [ ${hivesg} -gt 0 ];then
						           rm_halfstage1=${halfstage1}"*"
                                                           currenttime=`date`
                                                           echo "#sudo -u  hive hdfs dfs -rm -r "${db_ta}${rm_halfstage1} >> /tmp/clean-hive-tmp-${stime}.log 2>&1
	                                                   echo -e "\033[31m${currenttime} Info: delete "${db_ta}${rm_halfstage1}"\033[0m\n"
                                                           sleep 5
                                                           sudo -u  hive hdfs dfs -rm -r ${db_ta}${rm_halfstage1}
                                                           
                                 fi
							   sleep 5
							   hivesg2=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep ${halfstage2} |grep -v "${filter}"|awk -F ' ' '{print $8}' |wc -l`
								 
						           if [ ${hivesg2} -gt 0 ];then
						           rm_halfstage2=${halfstage2}"*"
                                                           currenttime=`date`
                                                           echo "#sudo -u  hive hdfs dfs -rm -r "${db_ta}${rm_halfstage2} >> /tmp/clean-hive-tmp-${stime}.log 2>&1
							   echo -e "\033[31m${currenttime} Info: delete "${db_ta}${rm_halfstage2}"\033[0m\n"
                                                           sleep 5
                                                           sudo -u  hive hdfs dfs -rm -r ${db_ta}${rm_halfstage2}
                                                           
                                 fi
                                                                 
                 sleep 3
    done
done

}

function functionyear3(){
halfstage1="/.hive-staging_hive_"${year1}
halfstage2="/.hive-staging_hive_"${year2}
halfstage3="/.hive-staging_hive_"${year3}
unset -v IFS
echo $halfstage1 $halfstage2 $halfstage3
database=`sudo -u hdfs hdfs dfs -ls   /apps/hive/warehouse/ |awk -F ' ' '{print $8}' |grep -v  "^$" |grep ".db"`
echo $database
for databs in ${database}
do
   db_table=`sudo -u hdfs hdfs dfs -ls -h  ${databs}  |awk -F ' ' '{print $8}'`
   for db_ta in ${db_table}
   do
                 echo ${db_ta} >> /tmp/table-${stime}.txt
                 #echo ${db_ta}
                 #dirs=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep -v ".hive-staging_hive_2019"|awk -F ' ' '{print $8}' `
                                 hivesg=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep ${halfstage1} |grep -v "${filter}"|awk -F ' ' '{print $8}' |wc -l`
								 
                                 if [ ${hivesg} -gt 0 ];then
						           rm_halfstage1=${halfstage1}"*"
                                                           currenttime=`date`
                                                           echo "#sudo -u  hive hdfs dfs -rm -r "${db_ta}${rm_halfstage1} >> /tmp/clean-hive-tmp-${stime}.log 2>&1
							   echo -e "\033[31m${currenttime} Info: delete "${db_ta}${rm_halfstage1}"\033[0m\n"
                                                           sleep 5
                                                           sudo -u  hive hdfs dfs -rm -r ${db_ta}${rm_halfstage1}
                                                           
                                 fi
								 #sleep 5
							   hivesg2=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep ${halfstage2} |grep -v "${filter}"|awk -F ' ' '{print $8}' |wc -l`
								 
							   if [ ${hivesg2} -gt 0 ];then
							             rm_halfstage2=${halfstage2}"*"
                                                                     currenttime=`date`
                                                                      echo "#sudo -u  hive hdfs dfs -rm -r "${db_ta}${rm_halfstage2} >> /tmp/clean-hive-tmp-${stime}.log 2>&1
								      echo -e "\033[31m${currenttime} Info:delete "${db_ta}${rm_halfstage2}"\033[0m\n"
                                                                      sleep 5
                                                                      sudo -u  hive hdfs dfs -rm -r ${db_ta}${rm_halfstage2}
                                                           
                                                            fi
								 #sleep 5
								 hivesg3=`sudo -u hdfs hdfs dfs -ls -h  ${db_ta}  |grep ".hive-staging_hive"|grep ${halfstage3} |grep -v "${filter}"|awk -F ' ' '{print $8}' |wc -l`
								 
								  if [ ${hivesg3} -gt 0 ];then
								     rm_halfstage3=${halfstage3}"*"
                                                                     echo "#sudo -u  hive hdfs dfs -rm -r "${db_ta}${rm_halfstage3} >> /tmp/clean-hive-tmp-${stime}.log 2>&1
								     echo -e "\033[31m${currenttime} Info: delete "${db_ta}${rm_halfstage3}"\033[0m\n"
                                                                     sleep 5
                                                                     sudo -u  hive hdfs dfs -rm -r ${db_ta}${rm_halfstage3}
                                                           
                                                                  fi
                                                                 
                 sleep 3
    done
done

}
starttime=`date`
echo -e "\033[31m${starttime} Info: start ===> \033[0m\n"$starttime
if [ a${yearnums} != a'' ];then
  set -o nounset
  if [ ${yearnums} -eq 1 ];then 
     echo -e "\033[31m${starttime} Info: delete time  $year1 \033[0m\n"
     functionyear
  elif [ ${yearnums} -eq 2 ];then
     echo -e "\033[31m${starttime} Info: delete time  $year1 $year2 \033[0m\n"
     functionyear2
  elif [ ${yearnums} -eq 3 ];then
      echo -e "\033[31m${starttime} Info: delete time  $year1 $year2 $year3 \033[0m\n"
      functionyear3
  else
     exit 0
  fi
elif [ a${all_time} == a"all" ];then
     echo -e "\033[31m${starttime} Info: delete time  all year \033[0m\n"
      set -o nounset
     functionall
else
      exit 0
fi

endtime=`date`
echo "See /tmp/table-"${stime}".txt and /tmp/clean-hive-tmp-"${stime}".log for more information."
echo -e "\033[31m${endtime} Info: end <===\033[0m\n"



