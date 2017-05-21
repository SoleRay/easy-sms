import bean.redis.RedisService;
import enums.redis.RedisDBIndex;
import enums.sms.SmsRedisKey;
import org.apache.commons.lang3.StringUtils;
import util.prop.PropUtil;
import util.sms.SmsCodeUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Created by Arthur on 2017/5/18 0018.
 */
public class Application {

    private static RedisService redisService = new RedisService();


    public static void main(String[] args) {

        //提醒类型（内存、磁盘、CPU等等）
        String alertType = args[0];
        String alertTypeName = args[1];
        String alertTypeValue = args[2];

        int sendLimit = Integer.parseInt(redisService.hGet(SmsRedisKey.SEND_COUNT_LIMIT.getName(),alertType));
        int sendCount = redisService.incr(SmsRedisKey.SEND_COUNT_PREFIX.getName()+alertType).intValue();

        //如果发送次数已经达到上限，清零提醒次数
        if(sendCount > sendLimit){
            redisService.set(SmsRedisKey.ALERT_COUNT_PREFIX.getName()+alertType,"0");
            return ;
        }

        //提醒次数
        int alertCount = redisService.incr(SmsRedisKey.ALERT_COUNT_PREFIX.getName()+alertType).intValue();
        //提醒频率
        int alertFreq = Integer.parseInt(redisService.hGet(SmsRedisKey.ALERT_FREQUENCY.getName(), alertType));

        //提醒次数对应的时间（与第一次提醒间隔的时长，单位：分钟）
        int diffMinute = 1;

        boolean needSend = false;

        if(alertCount>1){
            int diffSecond = (alertCount - 1) * alertFreq;

            int costMinute = diffSecond / 60;
            int remainder = diffSecond % 60;

            //只有当时长超过2分钟以上，并且余数小于频率本身
            //比如说，频率alertCount是7，第18次的时候，costSencond就是126秒，costMinute就是2，remainder就是6
            //当remainder（6）< alertCount（7）的时候，才是查找发送映射表，
            // 因为一旦这次发送，第19次时，costMinute其实还是2，但显然不可能重复发送
            if(costMinute>1 && remainder<alertFreq){
                needSend = redisService.sIsMember(SmsRedisKey.ALERT_SEND_REF_PREFIX.getName() + alertType, String.valueOf(costMinute));
            }

        }else if(alertCount==1){
            needSend = true;
        }


        if(needSend){
            sendMsg(alertTypeName,alertTypeValue);
            long countKeyExpire = Long.parseLong(redisService.hGet(SmsRedisKey.SEND_COUNT_EXPIRE.getName(), alertType));
            redisService.expire(SmsRedisKey.SEND_COUNT_PREFIX.getName()+alertType,countKeyExpire);
        }



    }

    private static void sendMsg(String alertTypeName, String alertTypeValue) {
        Set<String> mobileSet = redisService.sMembers(SmsRedisKey.MOBILE_LIST.getName());

        for(String phone : mobileSet){
            sendMsgForEachPhone(phone,alertTypeName,alertTypeValue);
        }
    }

    private static void sendMsgForEachPhone(String phone, String alertTypeName, String alertTypeValue) {

        String content = alertTypeName+","+alertTypeValue;

        boolean result = SmsCodeUtils.sendSmsByHy(phone, content);

        if(!result){
            SmsCodeUtils.sendSmsByCL(phone,content, true);
        }
    }
}
