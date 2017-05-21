#!/bin/sh
#通过df -lh命令获取磁盘使用百分比
disk_use_percent=$(df -lh | awk 'NR==2{print $5}')
disk_use=${disk_use_percent%"%"*}

echo $disk_use
#通过bc命令比较（浮点型shell不能比较）
if [ "$disk_use" -gt 80 ];then
    java -jar /usr/local/monitor_sms/system-monitor-sms-1.0.0.jar "disk" "服务器磁盘" "80%"
fi
