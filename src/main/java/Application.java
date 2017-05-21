import bean.redis.RedisService;
import enums.redis.RedisDBIndex;
import enums.sms.SmsRedisKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
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

    private static Logger logger =Logger.getLogger(Application.class);

    public static void main(String[] args) {

        //提醒类型（内存、磁盘、CPU等等）
        String alertType = args[0];
        String alertTypeName = args[1];
        String alertTypeValue = args[2];
        processAlert(alertType, alertTypeName, alertTypeValue);

    }

    private static void processAlert(String alertType, String alertTypeName, String alertTypeValue) {
        //提醒次数
        int alertCount = redisService.incr(SmsRedisKey.ALERT_COUNT_PREFIX.getName()+alertType).intValue();
        logger.info("提醒次数（alertCount）="+alertCount);

        //提醒频率
        int alertFreq = Integer.parseInt(redisService.hGet(SmsRedisKey.ALERT_FREQUENCY.getName(), alertType));
        logger.info("提醒频率（alertFreq）="+alertFreq);

        boolean needSend = false;

        if(alertCount>1){
            int diffSecond = (alertCount - 1) * alertFreq;
            logger.info("与第一次提醒间隔秒数（diffSecond）="+diffSecond);

            int costMinute = diffSecond / 60;
            logger.info("与第一次提醒间隔分钟数（costMinute）="+costMinute);

            int remainder = diffSecond % 60;
            logger.info("超过"+costMinute+"分钟的秒数（remainder）="+remainder);

            //只有当时长超过2分钟以上，并且余数小于频率本身
            //比如说，频率alertCount是7，第18次的时候，costSencond就是126秒，costMinute就是2，remainder就是6
            //当remainder（6）< alertCount（7）的时候，才是查找发送映射表，
            // 因为一旦这次发送，第19次时，costMinute其实还是2，但显然不可能重复发送
            if(costMinute>1 && remainder<alertFreq){
                needSend = redisService.sIsMember(SmsRedisKey.ALERT_SEND_REF_PREFIX.getName() + alertType, String.valueOf(costMinute));
                logger.info("符合查找对照表的条件,查找结果needSend="+needSend);
            }else{
                logger.info("时长未超过2分钟或者该时间段频率已经查找过对照表！");
            }

        }else if(alertCount==1){
            needSend = true;
        }


        if(needSend){
            int sendLimit = Integer.parseInt(redisService.hGet(SmsRedisKey.SEND_COUNT_LIMIT.getName(),alertType));
            int sendCount = redisService.incr(SmsRedisKey.SEND_COUNT_PREFIX.getName()+alertType).intValue();
            logger.info("根据对照表的查找条件，此次提醒可以发送短信，开始发送短信提醒");
            sendMsg(alertTypeName,alertTypeValue);
            long countKeyExpire = Long.parseLong(redisService.hGet(SmsRedisKey.SEND_COUNT_EXPIRE.getName(), alertType));

            //如果发送次数达到上限，则清零
            if(sendCount>=sendLimit){
                redisService.set(SmsRedisKey.SEND_COUNT_PREFIX.getName()+alertType,"0");
                redisService.set(SmsRedisKey.ALERT_COUNT_PREFIX.getName()+alertType,"0");
            }else{
                //否则设置提醒次数key、发送次数key的过期时间
                // 注意，提醒次数Key应该先过期归0，否则将出现明明可以发送，但因为提醒次数过大，而在提醒-发送对照表里找不到发送的时间
                redisService.expire(SmsRedisKey.SEND_COUNT_PREFIX.getName()+alertType,countKeyExpire);
                redisService.expire(SmsRedisKey.ALERT_COUNT_PREFIX.getName()+alertType,countKeyExpire-10);
            }
        }else {
            logger.info("发送次数（sendCount）="+redisService.get(SmsRedisKey.SEND_COUNT_PREFIX.getName()+alertType));
            logger.info("发送上限（sendLimit）="+redisService.hGet(SmsRedisKey.SEND_COUNT_LIMIT.getName(),alertType));
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
