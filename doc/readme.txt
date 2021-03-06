0.项目中产生和消亡的redis参数（不需要预先配置）
sms:alert:count:disk【磁盘预警次数】
sms:alert:count:memory 【内存预警次数】
sms:send:count:disk 【磁盘发送短信次数】
sms:send:count:memory 【内存发送短信次数】

1.redis预设参数（需要在redis中预先设置）
1.1 hmset sms:alert:freq disk 60 memory 60 【系统每60秒执行磁盘监控脚本和内存监控脚本】
1.2 sadd sms:alert:send:ref:disk 1 2 5 10 20 30 【磁盘报警提醒时间，第一次第一分钟，第二次第二分钟，第三次第五分钟...】
1.3 sadd sms:alert:send:ref:memory 1 2 5 10 20 30 【内存报警提醒时间，第一次第一分钟，第二次第二分钟，第三次第五分钟...】
1.4 sadd sms:mobile:list 1361111xxxx 1374444xxxx 【提醒短信发送的手机号】
1.5 hmset sms:send:count:expire disk 1800 memory 1800 【磁盘发送次数key和内存发送次数key的过期时间】
1.6 hmset sms:send:count:limit disk 5 memory 5【磁盘发送次数key和内存发送次数key的发送次数上限】

2.设计思路（脚本端）
2.1 编写linux脚本disk_monitor.sh和memory_monitor.sh
2.2 配置crontab，定时执行监控脚本（最小频率为1分钟）
2.3 如果发现磁盘或者内存的容量达到警报值，则执行本项目sys-monitor-sms的jar包
2.4 调用时传递三个参数：
2.5 arg0—（disk或者memory），这个值是上面key和field的组成部分，你可以改，但必须跟上面的key和field保持一致
2.6 arg1—（服务器磁盘，服务器内存），这个值是短信的组成部分，告诉用户是服务器磁盘报警还是服务器内存报警
2.7 arg2—（80%，70%）,这个值也是短信的组成部分，告诉用户预警值的多少，现在已经超过这个值了

3.设计思路（项目端）
3.1 我们设计了两个计数——alert_count和send_count
3.2 alert_count表示警报次数，即脚本执行时，发现容量达到警报值，调用本项目，该次数就会+1（sms:alert:count:disk或者sms:alert:count:memory）
3.3 send_count表示发送次数，并不是说每次调用本项目就会发送短信，那频率就太高了。所以警报次数到发送次数之间有个转换规则
3.4 转换规则如下：如果是第一次警报，则立即发送短信。
3.5 如果是第二次以上的报警，就要根据sms:alert:send:ref:disk（磁盘）或者sms:alert:send:ref:memory（内存）中设置的值来决定是否发送
3.6 sms:alert:send:ref:disk和sms:alert:send:ref:memory里面存的值是分钟数。即第一次报警之几分钟来发送短信，10就代表第一次报警后10分钟再发送短信
3.7 距第一次报警后几分钟，确切地说是第N次报警与第一次报警的次数差*监控频率。
    因为报警并不一定是连续的，有可能第一次报警后，系统可能就好了，下一次报警可能就是明天了，那这两次相差的时间是多少？假如，监控频率是60秒，那就是过了一分钟（实际上不可能，因为我们项目有清零处理）
    但通常发生报警的情况下，如果没有及时处理，系统是会连续报警的。所以，我们这里所说的距离第一次报警后几分钟，是基于连续报警的情况而言的。
3.8 在连续报警的情况下，假设监控频率是60秒，那么第3次报警距离第1次报警相差120秒，那么在sms:alert:send:ref:disk中找到了映射——2分钟，所以这次报警将发送一次短信
3.9 实际计算的时候，考虑到了更小的频率，不能整除的频率，比如7秒。那么第一次报警后发送后，第几次报警会再发送呢？7*18 = 126，所以第18次报警会发送短信。实际在计算的时候是取整的，126/60 =2
3.10 那第19次发送会不会发送呢？7*19 = 133  133/60=2，在sms:alert:send:ref:disk映射中能找到，是不是会发送呢？
3.11 答案是不会，因为126%60=6 133%60=13 只有126的时候，余数小于7，表示第2-3分钟这段时间，是第一次发送来的警报，所以只有18才发送，这就意味着，一个分钟点只能发送一次，不会发送多次
3.12 每次发送，发送次数sms:send:count:disk或者sms:send:count:disk就会+1
3.13 每次发送，如果达到了sms:send:count:limit中对应的发送上限，就会清零对应的预警次数和发送次数，从而进入下一个循环
3.14 每次发送，如果未到达sms:send:count:limit中对应的发送上限，则设置预警次数和发送次数的过期时间。确保一段时间后即便没有达到limit中的上限，也能清零，这是之前说的隔了一天的情况还累计的情况是不存在的
3.16 项目中，发送次数的过期时间在sms:send:count:expire中设置，预警次数的清零次数默认比发送次数的过期时间早100秒。目的就是让预警次数先过期。

设置技巧：建议sms:alert:send:ref:disk和sms:alert:send:ref:memory中的设置的触发数量大于sms:send:count:limit中的上限。
因为实践证明，我们有可能会手动去改系统监控频率sms:alert:freq，如果在连续报警的中途调低了频率，比如原先是30秒，我们改成了60秒，
那有可能导致在计算映射时间点的时候，跳过部分时间点，比如原先在距离第一次报警5分钟后要发送短信，因为调低频率，导致在6分钟才报警
这样就会跳过一次短信的发送，如果触发数量刚好等于发送上限，那会导致触发数量永远小于发送上限，从而导致无法及时清零（达到上限，报警次数和发送次数清零）。
不过即使出现了这种情况也不用担心，因为我们设置了key的过期时间，到了过期时间，也会清零。只不过在这期间，持续的报警将无法发送短信。

所以非常建议建议sms:alert:send:ref:disk和sms:alert:send:ref:memory中的设置的触发数量大于sms:send:count:limit中的上限。甚至可以远远大于。
这样即使跳过一些时间点，在后面的时间点依然可以找到发送短信的时间点



