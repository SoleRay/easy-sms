import bean.redis.RedisService;
import enums.redis.RedisDBIndex;
import org.apache.commons.lang3.StringUtils;
import util.prop.PropUtil;
import util.sms.SmsCodeUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Arthur on 2017/5/18 0018.
 */
public class Application {

    private static String redisLimitKey = "";
    private static long limit = 0;
    private static long expire = 0;

    static {
        PropUtil.loadProperties("application.properties");
        redisLimitKey = PropUtil.getProperty("redis.sms.send.limit.key");
        limit = Long.parseLong(PropUtil.getProperty("sms.send.limit"));
        expire = Long.parseLong(PropUtil.getProperty("redis.sms.send.limit.key.expire"));
    }

    public static void main(String[] args) {

        RedisService redisService = new RedisService();

        Long count = redisService.incr(RedisDBIndex.INDEX_3.getValue(), redisLimitKey);

        if(count<=limit){
            sendMsg(args);
            redisService.expire(RedisDBIndex.INDEX_3.getValue(),redisLimitKey,expire);
        }
    }

    private static void sendMsg(String[] args) {
        if(StringUtils.isNotBlank(args[0])){
            List<String> phoneList = Arrays.asList(args[0].split(","));
            for(String phone : phoneList){
                sendMsgForEachPhone(phone,args[1],args[2]);
            }

        }
    }

    private static void sendMsgForEachPhone(String phone, String checkType, String warnValue) {

        String content = checkType+","+warnValue;

        boolean result = SmsCodeUtils.sendSmsByHy(phone, content);

        if(!result){
            SmsCodeUtils.sendSmsByCL(phone,content, true);
        }
    }
}
