package enums.redis;

/**
 * ClassName: RedisDBIndex.java
 * @Description: 
 * @author Terry
 * @date Jul 4, 2016 5:47:36 PM
 * @version 1.0
 */
public enum RedisDBIndex {

	INDEX_0(0),
	INDEX_1(1),
	INDEX_2(2),
	INDEX_3(3),
	INDEX_4(4),
	INDEX_5(5),
	INDEX_6(6),
	INDEX_7(7),
	INDEX_8(8),
	INDEX_9(9),
	INDEX_10(10),
	INDEX_11(11),
	INDEX_12(12),
	INDEX_13(13),
	INDEX_14(14),
	INDEX_15(15);
	
	
	private int value;
	
	private RedisDBIndex(int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}
}
