package enums.sms;

/**
 * Created by Administrator on 2017-5-20.
 */
public enum SmsRedisKey {

    ALERT_COUNT_PREFIX("sms:alert:count:"),
    ALERT_SEND_REF_PREFIX("sms:alert:send:ref:"),
    ALERT_FREQUENCY("sms:alert:freq"),


    SEND_COUNT_PREFIX("sms:send:count:"),
    SEND_COUNT_LIMIT("sms:send:count:limit"),
    SEND_COUNT_EXPIRE("sms:send:count:expire"),

    MOBILE_LIST("sms:mobile:list");
    private String name;

    private SmsRedisKey(String name) {
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
