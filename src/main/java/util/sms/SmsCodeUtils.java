package util.sms;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import util.http.HttpSender;
import util.prop.PropUtil;

import java.text.MessageFormat;

/**
 * 
 * ClassName: SmsCodeUtils 
 * @Description: 发送短信辅助类
 * @author likeke
 * @date 2016年5月12日
 * @version 1.0
 */
public class SmsCodeUtils {
	
	private static Logger logger = Logger.getLogger(SmsCodeUtils.class);
	
	static {
		PropUtil.loadProperties("sms.properties");
	}
	/**
	 * 
	 * @Description: 调用创蓝接口发送短信
	 *
	 * @param mobile 要发送的手机号，如果多个以英文半角逗号隔开
	 * @param needStatus 是否获取状态吗
	 * @return
	 * @return String
	 * @throws
	 * @remark create li_keke 2016年5月12日
	 */
	public static boolean sendSmsByCL(String mobile,String content, boolean needStatus){
		
		boolean statu = false;
		try {
			String returnString = "";
			
			String smsUrl = PropUtil.getProperty("smsCode.clUrl");
			String account = PropUtil.getProperty("smsCode.clAccount");
			String pwd = PropUtil.getProperty("smsCode.clPwd");
			String msg = PropUtil.getProperty("smsCode.msg");
			String extno = PropUtil.getProperty("smsCode.clExtro");
			
			//解析占位符
			String[] fmtarget = content.split(",");
			msg = MessageFormat.format(msg,fmtarget);
			msg = MessageFormat.format(msg, fmtarget);

			try {
				returnString = HttpSender.batchSend(smsUrl, account, pwd, mobile, msg, needStatus, extno);
				// TODO 处理返回值,参见HTTP协议文档
			} catch (Exception e) {
				logger.error("发送短信出错", e);
			}
			if("0".equals(returnString.split("\n")[0].split(",")[1])){
				statu = true;
			}; // 响应状态吗为0时表示提交成功
			logger.info("通过创蓝接口发送短信,向手机号:"+ mobile +",发送短信状态响应码：" + returnString.split("\n")[0].split(",")[1]);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return statu;
	}
	
	/**
	 * 
	 * @Description: 调用互亿无线短信平台接口
	 * @param @param validateCode 生成的短信验证码
	 * @param @param mobile 要发送的手机号
	 * @param @return   
	 * @return boolean  
	 * @throws
	 * @author likeke
	 * @date 2016年5月13日
	 */
	public static boolean sendSmsByHy(String mobile,String content){
		
		boolean status = false;
		
		String hyUrl = PropUtil.getProperty("smsCode.hyUrl");
		String hyAccount = PropUtil.getProperty("smsCode.hyAccount");
		String hyPwd = PropUtil.getProperty("smsCode.hyPwd");
		String msg = PropUtil.getProperty("smsCode.msg");
		
		HttpClient client = new HttpClient(); 
		PostMethod method = new PostMethod(hyUrl); 
			
		client.getParams().setContentCharset("UTF-8");
		method.setRequestHeader("ContentType","application/x-www-form-urlencoded;charset=UTF-8");

		//解析占位符
		String[] fmtarget = content.split(",");
		msg = MessageFormat.format(msg,fmtarget);

		NameValuePair[] data = {//提交短信
			    new NameValuePair("account", hyAccount), 
			    new NameValuePair("password", hyPwd), //密码可以使用明文密码或使用32位MD5加密
			    //new NameValuePair("password", util.StringUtil.MD5Encode("密码")),
			    new NameValuePair("mobile", mobile), 
			    new NameValuePair("content", msg),
		};
		
		method.setRequestBody(data);		
		
		try {
			client.executeMethod(method);	
			
			String SubmitResult =method.getResponseBodyAsString();
					
			Document doc = DocumentHelper.parseText(SubmitResult);
			Element root = doc.getRootElement();
			
			 if("2".equals(root.elementText("code"))){
				 status =  true;
			}
			 logger.info("通过互亿无线接口发送短信,状态返回值:"+root.elementText("code")+",结果描述:"+root.elementText("msg"));
		} catch (Exception e) {
			logger.error("发送短信失败", e);
		} finally{
			method.releaseConnection();
		}
		
		return status;
	}
}
