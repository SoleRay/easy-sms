package bean.redis;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.RedisSerializer;
import redis.clients.jedis.JedisPoolConfig;
import util.prop.PropUtil;

import java.util.*;

public class RedisService<K,V> {

    private RedisTemplate<K, V> redisTemplate = new RedisTemplate<K, V>();

    static {
        PropUtil.loadProperties("redis.properties");
    }

    public RedisService() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(Integer.parseInt(PropUtil.getProperty("redis.maxIdle")));
        jedisPoolConfig.setMaxTotal(Integer.parseInt(PropUtil.getProperty("redis.maxTotal")));
        jedisPoolConfig.setMaxWaitMillis(Long.parseLong(PropUtil.getProperty("redis.maxWait")));
        jedisPoolConfig.setTestOnBorrow(Boolean.parseBoolean(PropUtil.getProperty("redis.testOnBorrow")));

        JedisConnectionFactory factory = new JedisConnectionFactory(jedisPoolConfig);
        factory.setUsePool(true);
        factory.setHostName(PropUtil.getProperty("redis.host"));
        factory.setPort(Integer.parseInt(PropUtil.getProperty("redis.port")));
        factory.setPassword(PropUtil.getProperty("redis.auth"));
        factory.setTimeout(Integer.parseInt(PropUtil.getProperty("redis.timeout")));
        factory.setDatabase(Integer.parseInt(PropUtil.getProperty("redis.default.db")));
        factory.afterPropertiesSet();

        redisTemplate.setConnectionFactory(factory);
        redisTemplate.afterPropertiesSet();
    }

    public boolean rename(final String oldKey, final String newKey){
        boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
            public Boolean doInRedis(RedisConnection connection) throws org.springframework.dao.DataAccessException {
                RedisSerializer<String> serializer = getRedisSerializer();
                byte[] oldName = serializer.serialize(oldKey);
                byte[] newName = serializer.serialize(newKey);
                connection.rename(oldName,newName);
                return true;
            }
        });
        return result;
    }



    public boolean addString(final String key,final String value,final Long timeout){
        boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
            public Boolean doInRedis(RedisConnection connection) throws org.springframework.dao.DataAccessException {
                Boolean result = connection.setNX(key.getBytes(), value.getBytes());
                if (result == false)
                    return result;
                if (timeout != null && timeout > 0)
                    connection.expire(key.getBytes(), timeout);
                return result;
            }
        });
        return result;
    }

    /**
     * 设置key
     */
    public Boolean set(final String key, final String value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    byte[] values = serializer.serialize(value);
                    connection.set(keys, values);
                    return true;
                }
            });
        }
        return false;
    }

    /**
     * 设置key
     */
    public Boolean set(final int dbIndex,final String key, final String value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    byte[] values = serializer.serialize(value);
                    connection.select(dbIndex);
                    connection.set(keys, values);
                    return true;
                }
            });
        }
        return false;
    }
    /**
     * 根据key获取对象
     */
    public String get(final String key) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<String>() {
                public String doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    byte[] values = connection.get(keys);
                    if (values == null) {
                        return null;
                    }
                    String value = serializer.deserialize(values);
                    return value;
                }
            });
        }
        return null;
    }

    /**
     * 根据key获取对象
     */
    public String get(final int dbIndex, final String key) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<String>() {
                public String doInRedis(RedisConnection connection)
                        throws DataAccessException {
                	connection.select(dbIndex);
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    byte[] values = connection.get(keys);
                    if (values == null) {
                        return null;
                    }
                    String value = serializer.deserialize(values);
                    return value;
                }
            });
        }
        return null;
    }

    /**
     * 根据key删除
     * @param key
     * @return
     */
    public Long del(final String key) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Long>() {
                public Long doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.del(keys);
                }
            });
        }
        return null;
    }

    public Long del(final Set<String> keys) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Long>() {
                public Long doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    List<byte[]> bytes = new ArrayList<>();
                    for (String key : keys) {
                    	byte[] byteKey = serializer.serialize(key);
                    	bytes.add(byteKey);
					}
                    byte[][] byteArray = new byte[bytes.size()][];

                    for (int i = 0; i < byteArray.length; i++) {
                    	byteArray[i]= bytes.get(i);
					}
                    return connection.del(byteArray);
                }
            });
        }
        return null;
    }

    public Set<String> keys(int dbIndex,final String pattern) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Set<String>>() {
                public Set<String> doInRedis(RedisConnection connection)
                        throws DataAccessException {
                	connection.select(dbIndex);
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] patterns = serializer.serialize(pattern);
                    Set<String> data = new HashSet<>();
                    Set<byte[]> re = connection.keys(patterns);
                    for(byte[] by : re){
                        data.add(serializer.deserialize(by));
                    }
                    return data;
                }
            });
        }
        return null;
    }


    /**
     * 某段时间后执行
     * @param key
     * @param value
     * @return
     */
    public Boolean expire(final int dbIndex, final String key, final long value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    connection.select(dbIndex);
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.expire(keys, value);
                }
            });
        }
        return false;
    }

    public Boolean expire(final String key, final long value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.expire(keys, value);
                }
            });
        }
        return false;
    }

    /**
     * 在某个时间点失效
     * @param key
     * @param value
     * @return
     */
    public Boolean expireAt(final String key, final long value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.expireAt(keys, value);
                }
            });
        }
        return false;
    }

    /**
     * 查询剩余时间
     * @param key
     * @return
     */
    public Long ttl(final String key) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Long>() {
                public Long doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.ttl(keys);
                }
            });
        }
        return 0l;
    }

    /**
     * 判断key是否存在
     * @param key
     * @return
     */
    public Boolean exists(final String key) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.exists(keys);
                }
            });
        }
        return false;
    }

    /**
     * 返回 key 所储存的值的类型
     * @param key
     * @return
     */
    public DataType type(final String key) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<DataType>() {
                public DataType doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.type(keys);
                }
            });
        }
        return null;
    }

    /**
     * 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)
     * @param key
     * @param offset
     * @param value
     * @return
     */
    public Boolean setBit(final String key,final long offset,final boolean value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    connection.setBit(keys,offset,value);
                    return true;
                }
            });
        }
        return false;
    }

    /**
     * 对 key 所储存的字符串值，获取指定偏移量上的位(bit)
     * @param key
     * @param value
     * @return
     */
    public Boolean getBit(final String key ,final long value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.getBit(keys, value);
                }
            });
        }
        return false;
    }

    /**
     * 用 value 参数覆写(overwrite)给定 key 所储存的字符串值，从偏移量 offset 开始
     * @param key
     * @param offset
     * @param value
     * @return
     */
    public Boolean setRange(final String key,final Long offset,final String value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Boolean>() {
                public Boolean doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    byte[] values = serializer.serialize(value);
                    connection.setRange(keys,values,offset);
                    return true;
                }
            });
        }
        return false;
    }

    /**
     * 返回 key 中字符串值的子字符串，字符串的截取范围由 start 和 end 两个偏移量决定
     * @param key
     * @param startOffset
     * @param endOffset
     * @return
     */
    public byte[] getRange(final String key,final long startOffset,final long endOffset) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<byte[]>() {
                public byte[] doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    return connection.getRange(keys,startOffset,endOffset);
                }
            });
        }
        return null;
    }

    /**
     * 通配符删除key
     * @param pattern
     */
    public void removePattern(final K pattern){
        if(redisTemplate != null){
            RedisSerializer<String> serializer = getRedisSerializer();
            redisTemplate.setKeySerializer(serializer);
            Set<K> keys = redisTemplate.keys(pattern);
            if(keys.size()>0){
                redisTemplate.delete(keys);
            }
        }
    }

    /**
     * 删除对象 ,依赖key
     */
    public void delete(String key) {
        List<String> list = new ArrayList<String>();
        list.add(key);
        delete(list);
    }

    /**
     * 删除集合 ,依赖key集合
     */
    @SuppressWarnings("unchecked")
    private void delete(List<String> keys) {
        redisTemplate.delete((K) keys);
    }

    /**
     * 根据参数 count 的值，移除列表中与参数 value 相等的元素
     * @param count
     * @return
     */
    public Long lrem(final String key, final long count, final String value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Long>() {
                public Long doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    byte[] values = serializer.serialize(value);
                    return connection.lRem(keys, count, values);
                }
            });
        }
        return null;
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表头
     * @param
     * @param
     * @return
     */
    public Long lpush(final String key, final String value) {
        if (redisTemplate != null) {
            return redisTemplate.execute(new RedisCallback<Long>() {
                public Long doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    byte[] keys = serializer.serialize(key);
                    byte[] values = serializer.serialize(value);
                    return connection.lPush(keys, values);
                }
            });
        }
        return null;
    }

    public Object batchLPush(final Map<String,String> map) {

        if(redisTemplate != null){
            RedisSerializer<String> serializer = getRedisSerializer();
            //必须设置这个：使用get方法必然经过
            redisTemplate.setValueSerializer(serializer);
            return redisTemplate.executePipelined(new RedisCallback<String>() {

                @Override
                public String doInRedis(RedisConnection connection)
                        throws DataAccessException {

                    Set<Map.Entry<String, String>> entries = map.entrySet();
                    for (Map.Entry<String, String> entry : entries) {
                        String key = entry.getKey();
                        String value = entry.getValue();

                        byte[] sKey = serializer.serialize(key);
                        byte[] sValue = serializer.serialize(value);

                        connection.lPush(sKey,sValue);
                    }

                    return null;
                }
            });
        }
        return null;
    }

    public Object batchRPush(final String key,final List<String> valueList) {

        if(redisTemplate != null){
            RedisSerializer<String> serializer = getRedisSerializer();
            //必须设置这个：使用get方法必然经过
            redisTemplate.setValueSerializer(serializer);
            return redisTemplate.executePipelined(new RedisCallback<String>() {

                @Override
                public String doInRedis(RedisConnection connection)
                        throws DataAccessException {

                    byte[] sKey = serializer.serialize(key);
                    for(String value : valueList){
                        byte[] sValue = serializer.serialize(value);
                        connection.rPush(sKey,sValue);
                    }
                    return null;
                }
            });
        }
        return null;
    }

     public byte[] getSet(final String key,final String value) {
         if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<byte[]>() {
                    public byte[] doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] values = serializer.serialize(value);
                        return connection.getSet(keys, values);
                    }
                });
            }
            return null;
        }

        public Boolean setNX(final String key,final String value) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] values = serializer.serialize(value);
                        return connection.setNX(keys,values);
                    }
                });
            }
            return false;
        }

        public Boolean setEx(final String key,final Long seconds,final String value) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] values = serializer.serialize(value);
                        connection.setEx(keys, seconds, values);
                        return true;
                    }
                });
            }
            return false;
        }

        public Long decrBy(final String key,final long integer) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.decrBy(keys, integer);
                    }
                });
            }
            return null;
        }

        public Long decr(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.decr(keys);
                    }
                });
            }
            return null;
        }

        public Long incrBy(final String key,final long integer) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.incrBy(keys,integer);
                    }
                });
            }
            return null;
        }

        public Long incr(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.incr(keys);
                    }
                });
            }
            return null;
        }

        public Long incr(final int dbIndex, final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        connection.select(dbIndex);
                        return connection.incr(keys);
                    }
                });
            }
            return null;
        }

        public Long append(final String key,final String value) {
            if (redisTemplate != null) {
               return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] values = serializer.serialize(value);
                        return connection.append(keys,values);
                    }
                });
            }
            return null;
        }

        /**
         *
         * @Description: TODO
         * @param @param key key标识
         * @param @param field  属性名
         * @param @param value  值
         * @param @param timeout  过期时间(秒)
         * @param @return
         * @return Boolean
         * @throws
         * @author likeke
         * @date 2016年6月22日
         */
        public Boolean hSet(final String key,final String field,final String value,final Long timeout) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] fields = serializer.serialize(field);
                        byte[] values = serializer.serialize(value);
                        boolean result =  connection.hSet(keys, fields, values);
                        if (timeout != null && timeout > 0)
        					connection.expire(key.getBytes(), timeout);
        				return result;
                    }
                });
            }
            return false;
        }

        public String hGet(final String key,final String field) {
            try {
				if (redisTemplate != null) {
				   return new String(redisTemplate.execute(new RedisCallback<byte[]>() {
				        public byte[] doInRedis(RedisConnection connection)
				                throws DataAccessException {
				            RedisSerializer<String> serializer = getRedisSerializer();
				            byte[] keys = serializer.serialize(key);
				            byte[] fields = serializer.serialize(field);
				            return connection.hGet(keys, fields);
				        }
				    }));
				}
			} catch (NullPointerException e) {

			}
            return null;
        }

        public Boolean hSetNX(final String key,final String field,final String value) {
            if (redisTemplate != null) {
               return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] fields = serializer.serialize(field);
                        byte[] values = serializer.serialize(value);
                        return connection.hSetNX(keys, fields, values);
                    }
                });
            }
            return false;
        }

        public Boolean hMSet(final String key,final Map<String, Object> map,final Long timeout) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                    	HashMap<byte[], byte[]> finalMap = new HashMap<>();
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        map.entrySet().forEach(entry->{
                        	byte[] skey = serializer.serialize(entry.getKey());
                        	byte[] svalue = serializer.serialize(String.valueOf(entry.getValue()));
                        	finalMap.put(skey, svalue);
                        });
                        connection.hMSet(keys, finalMap);
                        if (timeout != null && timeout > 0){
                            connection.expire(key.getBytes(), timeout);
                        }
                        return true;
                    }
                });
            }
            return false;
        }

        /**
         * 返回哈希表 key 中，一个或多个给定域的值
         * @param key
         * @param fields
         * @return
         */
        public List<String> hMGet(final String key,final byte[]... fields) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<List<String>>() {
                    public List<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        List<String> data = new ArrayList<>();
                        byte[] keys = serializer.serialize(key);
                        List<byte[]> re = connection.hMGet(keys, fields);
                        for(byte[] by : re){
                            data.add(serializer.deserialize(by));
                        }
                        return data;
                    }
                });
            }
            return null;
        }
        public Map<String,Object> hMGet(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Map<String,Object>>() {
                    public Map<String,Object> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        Map<String,Object> data = new HashMap<>();
                        byte[] keys = serializer.serialize(key);
                        Map<byte[],byte[]> re = connection.hGetAll(keys);
                        for(Map.Entry<byte[], byte[]> entry : re.entrySet()){
                        	data.put(serializer.deserialize(entry.getKey()), serializer.deserialize(entry.getValue()));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Long hIncrBy(final String key,final String field,final long value) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] fields = serializer.serialize(field);
                        return connection.hIncrBy(keys, fields, value);
                    }
                });
            }
            return null;
        }

        public Boolean hexists(final String key,final String field) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] fields = serializer.serialize(field);
                        return connection.hExists(keys, fields);
                    }
                });
            }
            return false;
        }


        public Long hDel(final String key,final String field) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] fields = serializer.serialize(field);
                        return connection.hDel(keys, fields);
                    }
                });
            }
            return null;
        }

        public Long hlen(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.hLen(keys);
                    }
                });
            }
            return null;
        }

        public Set<String> hKeys(final String key) {
//            if (redisTemplate != null) {
//                return redisTemplate.execute(new RedisCallback<Set<String>>() {
//                    public Set<String> doInRedis(RedisConnection connection)
//                            throws DataAccessException {
//                        RedisSerializer<String> serializer = getRedisSerializer();
//                        byte[] keys = serializer.serialize(key);
//                        Set<String> data = new HashSet<>();
//                        Set<byte[]> re = connection.hKeys(keys);
//                        for(byte[] by : re){
//                            data.add(serializer.deserialize(by));
//                        }
//                        return data;
//                    }
//                });
//            }
//            return null;
        	return hKeys(0,key);
        }

        public Set<String> hKeys(int dbIndex,final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                    	connection.select(dbIndex);
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<String> data = new HashSet<>();
                        Set<byte[]> re = connection.hKeys(keys);
                        for(byte[] by : re){
                            data.add(serializer.deserialize(by));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public List<String> hVals(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<List<String>>() {
                    public List<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        List<String> data = new ArrayList<>();
                        List<byte[]> re = connection.hVals(keys);
                        for(byte[] by : re){
                            data.add(serializer.deserialize(by));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Map<String, Object> hGetAll(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Map<String, Object>>() {
                    public Map<String, Object> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Map<byte[], byte[]> map = connection.hGetAll(keys);
                        Map<String, Object> resultMap = new HashMap<>();
                        for(Map.Entry<byte[], byte[]> entry : map.entrySet()){
                            String resultKey = serializer.deserialize(entry.getKey());
                            String resultValue = serializer.deserialize(entry.getValue());
                            resultMap.put(resultKey,resultValue);
                        }
                        return resultMap;

                    }
                });
            }
            return null;
        }

        // ================list ====== l表示 list或 left, r表示right====================
        public Long rPush(final String key,final String value) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] values = serializer.serialize(value);
                        return connection.rPush(keys,values);

                    }
                });
            }
            return null;
        }

        public Long lLen(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.lLen(keys);
                    }
                });
            }
            return null;
        }

        public List<String> lRange(final String key,final Long start,final Long end) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<List<String>>() {
                    public List<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        List<String> data = new ArrayList<>();
                        List<byte[]> re = connection.lRange(keys, start, end);
                        for(byte[] by : re){
                            data.add(serializer.deserialize(by));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Boolean ltrim(final String key,final long start,final long end) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        connection.lTrim(keys, start, end);
                        return true;
                    }
                });
            }
            return false;
        }

        public String lIndex(final String key,final long index) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<String>() {
                    public String doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] re = connection.lIndex(keys, index);
                        return serializer.deserialize(re);
                    }
                });
            }
            return null;
        }

        public Boolean lSet(final String key,final long index,final String value) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] values = serializer.serialize(value);
                        connection.lSet(keys, index, values);
                        return true;
                    }
                });
            }
            return false;
        }


        public String lPop(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<String>() {
                    public String doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] re = connection.lPop(keys);
                        return serializer.deserialize(re);
                    }
                });
            }
            return null;
        }

        public String rPop(final String key) {
            if (redisTemplate != null) {
               return redisTemplate.execute(new RedisCallback<String>() {
                    public String doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] re = connection.rPop(keys);
                        return serializer.deserialize(re);
                    }
                });
            }
            return null;
        }

        public Long sAdd(final String key,final String member) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.sAdd(keys,members);
                    }
                });
            }
            return null;
        }

    public List<Object> batchSAdd(final String key ,final Set<String> values) {
        if (redisTemplate != null) {
            RedisSerializer<String> serializer = getRedisSerializer();
            redisTemplate.setValueSerializer(serializer);
            return redisTemplate.executePipelined(new RedisCallback<Long>() {
                public Long doInRedis(RedisConnection connection)
                        throws DataAccessException {

                    RedisSerializer<String> serializer = getRedisSerializer();

                    byte[] byteKey = serializer.serialize(key);
                    byte[][] valueArray = new byte[values.size()][];

                    int i = 0;
                    for(String value:values){
                        byte[] byteValue = serializer.serialize(value);
                        valueArray[i]=byteValue;
                        i++;
                    }
                    connection.sAdd(byteKey,valueArray);
                    return null;
                }
            });
        }
        return null;
    }


        public Set<String> sMembers(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.sMembers(keys);
                        Set<String> data = new HashSet<>();
                        for (byte[] s :set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Long sRem(final String key,final String member) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.sRem(keys,members);
                    }
                });
            }
            return null;
        }

        public String sPop(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<String>() {
                    public String doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] re = connection.sPop(keys);
                        return serializer.deserialize(re);
                    }
                });
            }
            return null;
        }

        public Long sCard(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.sCard(keys);
                    }
                });
            }
            return null;
        }

        public Boolean sIsMember(final String key,final String member) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.sIsMember(keys,members);
                    }
                });
            }
            return false;
        }

        public String sRandMember(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<String>() {
                    public String doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] re = connection.sRandMember(keys);
                        return serializer.deserialize(re);
                    }
                });
            }
            return null;
        }

        public Boolean zAdd(final String key,final double score,final String member) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Boolean>() {
                    public Boolean doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.zAdd(keys, score, members);
                    }
                });
            }
            return false;
        }

        public Set<String> zRange(final String key,final int start,final int end) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.zRange(keys, start, end);
                        Set<String> data = new HashSet<>();
                        for (byte[] s : set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Long zRem(final String key,final String member) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.zRem(keys , members);
                    }
                });
            }
            return null;
        }

        public Double zIncrBy(final String key,final double score,final String member) {
            if (redisTemplate != null) {
            	return redisTemplate.execute(new RedisCallback<Double>() {
                    public Double doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.zIncrBy(keys, score, members);
                    }
                });
            }
            return null;
        }

        public Long zRank(final String key,final String member) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.zRank(keys , members);
                    }
                });
            }
            return null;
        }

        public Long zRevRank(final String key,final String member) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.zRevRank(keys , members);
                    }
                });
            }
            return null;
        }

        public Set<String> zRevRange(final String key,final int start,final int end) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.zRevRange(keys, start, end);
                        Set<String> data = new HashSet<>();
                        for (byte[] s : set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zRangeWithScores(final String key,final int start,final int end) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRangeWithScores(keys, start, end);
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zRevRangeWithScores(final String key,final int start,final int end) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRevRangeWithScores(keys, start, end);
                    }
                });
            }
            return null;
        }

        public Long zCard(final String key) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zCard(keys);
                    }
                });
            }
            return null;
        }

        public Double zScore(final String key,final String member) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Double>() {
                    public Double doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] members = serializer.serialize(member);
                        return connection.zScore(keys , members);
                    }
                });
            }
            return null;
        }


        public List<String> sort(final String key,final SortParameters params) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<List<String>>() {
                    public List<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        List<String> data = new ArrayList<>();
                        List<byte[]> re = connection.sort(keys, params);
                        for(byte[] by : re){
                            data.add(serializer.deserialize(by));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Long zCount(final String key,final double min,final double max) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zCount(keys, min, max);
                    }
                });
            }
            return null;
        }

        public Set<String> zrevrangeByScore(final String key,final double max,final double min) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.zRevRangeByScore(keys, max, min);
                        Set<String> data = new HashSet<>();
                        for (byte[] s : set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Set<String> zrevrangeByScore(final String key, final double max, final double min, final int offset, final int count) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.zRevRangeByScore(keys, min, max, offset, count);
                        Set<String> data = new HashSet<>();
                        for (byte[] s : set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zrangeByScoreWithScores(final String key,final double min,final double max) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                         return connection.zRangeByScoreWithScores(keys, min, max);
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zrevrangeByScoreWithScores(final String key,final double max,final double min) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                         return connection.zRevRangeByScoreWithScores(keys, max, min);
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zrangeByScoreWithScores(final String key,final double min,final double max,final int offset,final int count) {
            if (redisTemplate != null) {
                redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRangeByScoreWithScores(keys, min, max,offset,count);
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zrevrangeByScoreWithScores(final String key,final double max,final double min,final int offset,final int count) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRevRangeByScoreWithScores(keys, max, min, offset, count);
                    }
                });
            }
            return null;
        }

        public Long zremrangeByScore(final String key,final double start,final double end) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRemRangeByScore(keys, start, end);
                    }
                });
            }
            return null;
        }

        public Long linsert(final String key, final Position where,final String pivot,final String value) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        byte[] pivots = serializer.serialize(pivot);
                        byte[] values = serializer.serialize(value);
                        return connection.lInsert(keys, where, pivots, values);
                    }
                });
            }
            return null;
        }

        public Set<String> zRangeByScore(final String key,final double min,final double max) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.zRangeByScore(keys, max, min);
                        Set<String> data = new HashSet<>();
                        for (byte[] s : set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Set<String> zRangeByScore(final String key,final double min,final double max,final int offset,final int count) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.zRangeByScore(keys, max, min,offset,count);
                        Set<String> data = new HashSet<>();
                        for (byte[] s : set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zRangeByScoreWithScores(final String key,final double min,final double max) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRangeByScoreWithScores(keys, max, min);
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zRangeByScoreWithScores(final String key,final double min,final double max,final int offset,final int count) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRangeByScoreWithScores(keys, max, min,offset,count);
                    }
                });
            }
            return null;
        }

        public Set<String> zRevRangeByScore(final String key,final double max,final double min) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.zRevRangeByScore(keys, max, min);
                        Set<String> data = new HashSet<>();
                        for (byte[] s : set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Set<String> zRevRangeByScore(final String key,final double max,final double min,final int offset,final int count) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<String>>() {
                    public Set<String> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        Set<byte[]> set = connection.zRevRangeByScore(keys, max, min,offset,count);
                        Set<String> data = new HashSet<>();
                        for (byte[] s : set) {
                            data.add(serializer.deserialize(s));
                        }
                        return data;
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zRevRangeByScoreWithScores(final String key,final double max,final double min) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRevRangeByScoreWithScores(keys, max, min);
                    }
                });
            }
            return null;
        }

        public Set<Tuple> zRevRangeByScoreWithScores(final String key,final  double max,final double min,final int offset,final int count) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Set<Tuple>>() {
                    public Set<Tuple> doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRevRangeByScoreWithScores(keys,max,min, offset,count );
                    }
                });
            }
            return null;
        }

        public Long zRemRangeByScore(final String key,final double start,final double end) {
            if (redisTemplate != null) {
                return redisTemplate.execute(new RedisCallback<Long>() {
                    public Long doInRedis(RedisConnection connection)
                            throws DataAccessException {
                        RedisSerializer<String> serializer = getRedisSerializer();
                        byte[] keys = serializer.serialize(key);
                        return connection.zRemRangeByScore(keys, start, end);
                    }
                });
            }
            return null;
        }

        public List<Object> batchHGet(final List<String> keyList,final String field){
        	if(redisTemplate != null){
        		RedisSerializer<String> serializer = getRedisSerializer();
        		//必须设置这个：使用get方法必然经过
        		redisTemplate.setValueSerializer(serializer);
        		return redisTemplate.executePipelined(new RedisCallback<String>() {

					@Override
					public String doInRedis(RedisConnection connection)
							throws DataAccessException {

						for(String key : keyList){
							byte[] keys = serializer.serialize(key);
					        byte[] fields = serializer.serialize(field);
					        connection.hGet(keys, fields);
						}

						return null;
					}
				});
        	}
			return null;
        }

        public List<Object> batchHSet(final List<String> keyList,final String field,final String value){
        	if(redisTemplate != null){
        		RedisSerializer<String> serializer = getRedisSerializer();
        		//必须设置这个：使用get方法必然经过
        		redisTemplate.setValueSerializer(serializer);
        		return redisTemplate.executePipelined(new RedisCallback<String>() {

					@Override
					public String doInRedis(RedisConnection connection)
							throws DataAccessException {

						for(String key : keyList){
							byte[] keys = serializer.serialize(key);
					        byte[] fields = serializer.serialize(field);
					        byte[] values = serializer.serialize(value);
					        connection.hSet(keys, fields, values);
						}

						return null;
					}
				});
        	}
			return null;
        }

    public List<Object> batchSet(final Map<String,String> map,final Long expire){
        if(redisTemplate != null){
            RedisSerializer<String> serializer = getRedisSerializer();
            //必须设置这个：使用get方法必然经过
            redisTemplate.setValueSerializer(serializer);
            return redisTemplate.executePipelined(new RedisCallback<String>() {

                @Override
                public String doInRedis(RedisConnection connection)
                        throws DataAccessException {

                    Expiration expiration = Expiration.seconds(expire);
                    for(Map.Entry<String,String> entry : map.entrySet()){
                        byte[] keys = serializer.serialize(entry.getKey());
                        byte[] values = serializer.serialize(entry.getValue());
                        connection.set(keys,values,expiration,null);
                    }
                    return null;
                }
            });
        }
        return null;
    }

        public List<Object> batchHMSet(final List<String> keyList,final List<Map<String,Object>> mapList){
        	if(redisTemplate != null){
        		RedisSerializer<String> serializer = getRedisSerializer();
        		//必须设置这个：使用get方法必然经过
        		redisTemplate.setValueSerializer(serializer);
        		return redisTemplate.executePipelined(new RedisCallback<String>() {

					@Override
					public String doInRedis(RedisConnection connection)
							throws DataAccessException {
						RedisSerializer<String> serializer = getRedisSerializer();

						for (int i = 0; i < keyList.size(); i++) {
							HashMap<byte[], byte[]> finalMap = new HashMap<>();
							byte[] keys = serializer.serialize(keyList.get(i));
							mapList.get(i).entrySet().forEach(entry->{
	                        	byte[] skey = serializer.serialize(entry.getKey());
	                        	byte[] svalue = serializer.serialize(String.valueOf(entry.getValue()));
	                        	finalMap.put(skey, svalue);
	                        });

							connection.hMSet(keys, finalMap);
						}

						return null;
					}
				});
        	}
			return null;
        }

    public List<Object> batchHMGet(final List<String> keyList){
        if(redisTemplate != null){
            RedisSerializer<String> serializer = getRedisSerializer();
            //必须设置这个：使用get方法必然经过
            redisTemplate.setValueSerializer(serializer);
            return redisTemplate.executePipelined(new RedisCallback<List<Map<String,Object>>>() {
                @Override
                public List<Map<String,Object>> doInRedis(RedisConnection connection)
                        throws DataAccessException {
                    RedisSerializer<String> serializer = getRedisSerializer();
                    List<Map<String,Object>> dataMapList = new ArrayList<Map<String, Object>>();
                    for (int i = 0; i < keyList.size(); i++) {
                        Map<String,Object> data = new HashMap<>();
                        byte[] keys = serializer.serialize(keyList.get(i));
                        connection.hGetAll(keys);
                    }
                    return null;
                }
            });
        }
        return null;
    }
         
        /**
         * 获取 RedisSerializer
         * 
         */
        protected RedisSerializer<String> getRedisSerializer() {
            return redisTemplate.getStringSerializer();
        }
 
        public RedisTemplate<K, V> getRedisTemplate() {
            return redisTemplate;
        }
 
        public void setRedisTemplate(RedisTemplate<K, V> redisTemplate) {
            this.redisTemplate = redisTemplate;
        }
}
