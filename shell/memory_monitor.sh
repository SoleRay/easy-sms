#!/bin/sh
#通过free -m命令或许总内存和已经使用的内存
mem_total=$(free -m | grep Mem | awk '{print $2}')
mem_used=$(free -m | grep Mem | awk '{print $3}')

echo $mem_total
echo $mem_used

#通过awk命令计算百分比（浮点型shell不能计算）
use_rate=$(awk -v mem_total=$mem_total -v mem_used=$mem_used 'BEGIN{print mem_used*1.0/mem_total}')

echo $use_rate

#通过bc命令比较（浮点型shell不能比较）
if [  $(echo "$use_rate > 0.8" | bc) -eq 1  ];then
	java -jar /usr/local/monitor_sms/system-monitor-sms-1.0.0.jar "memory" "服务器内存" "80%"
fi
