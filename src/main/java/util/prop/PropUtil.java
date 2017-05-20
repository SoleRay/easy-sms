package util.prop;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * ClassName: PropUtil.java
 * @Description: 
 * @author Terry
 * @date Sep 12, 2016 10:14:44 AM
 * @version 1.0
 */
public class PropUtil {

	private static final Properties prop = new Properties();

	public static String getProperty(String key){
		String str = prop.getProperty(key);
		return str;
	}


	public static void loadProperties(String fileName){
		try {
			InputStream inputStream =
					PropUtil.class.getClassLoader().getResourceAsStream(fileName);

			prop.load(inputStream);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
