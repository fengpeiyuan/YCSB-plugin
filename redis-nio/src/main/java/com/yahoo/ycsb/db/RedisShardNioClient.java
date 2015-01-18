package com.yahoo.ycsb.db;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import com.jd.redis.framework.client.JdRedisShardClient;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class RedisShardNioClient extends DB {

    private JdRedisShardClient jdRedisShardClient;
    public static final String CONN_PROPERTY = "redis.conn.str";
    public static final String INDEX_KEY = "_indices";
    
    public void init() throws DBException {
        Properties props = getProperties();
        String redisConnString = props.getProperty(CONN_PROPERTY);
        if (redisConnString == null) {
            return;
        }
        jdRedisShardClient = new JdRedisShardClient(redisConnString);
        jdRedisShardClient.init();
    }

    public void cleanup() throws DBException {
    	try {
			jdRedisShardClient.destroy();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    private double hash(String key) {
        return key.hashCode();
    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
    	try{
	        if (fields == null) {
	            StringByteIterator.putAllAsByteIterators(result, jdRedisShardClient.hgetall(key));
	        }
	        else {
	            String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
	            List<String> values = jdRedisShardClient.hmget(key, fieldArray);
	
	            Iterator<String> fieldIterator = fields.iterator();
	            Iterator<String> valueIterator = values.iterator();
	
	            while (fieldIterator.hasNext() && valueIterator.hasNext()) {
	                result.put(fieldIterator.next(),
				   new StringByteIterator(valueIterator.next()));
	            }
	            assert !fieldIterator.hasNext() && !valueIterator.hasNext();
	        }
	        return result.isEmpty() ? 1 : 0;
    	}catch(Exception e){
    		e.printStackTrace();
    		return 1;
    	}
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    	try{
	        if (jdRedisShardClient.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
	        	jdRedisShardClient.zadd(INDEX_KEY, hash(key), key);
	            return 0;
	        }
	        return 1;
    	}catch(Exception e){
    		e.printStackTrace();
    		return 1;
    	}
    }

    @Override
    public int delete(String table, String key) {
    	try{
	        return jdRedisShardClient.del(key) == 0
	            && jdRedisShardClient.zrem(INDEX_KEY, key) == 0
	               ? 1 : 0;
    	}catch(Exception e){
    		e.printStackTrace();
    		return 1;
    	}
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
			return jdRedisShardClient.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
    }

    @Override
    public int scan(String table, String startkey, int recordcount,Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    	 try {
	    	List<String> keys = jdRedisShardClient.zrangebyscore(INDEX_KEY, String.valueOf(hash(startkey)),String.valueOf(Double.POSITIVE_INFINITY), 0, recordcount);
	        HashMap<String, ByteIterator> values;
	        for (String key : keys) {
	            values = new HashMap<String, ByteIterator>();
	            read(table, key, fields, values);
	            result.add(values);
	        }
	        return 0;
    	 } catch (Exception e) {
 			e.printStackTrace();
 			return 1;
 		}
    }

}
