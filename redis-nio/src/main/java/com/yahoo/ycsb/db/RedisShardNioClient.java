package com.yahoo.ycsb.db;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import com.nio.redis.framework.client.RedisShardClient;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class RedisShardNioClient extends DB {

    private RedisShardClient redisShardClient;
    public static final String CONN_PROPERTY = "redis.conn.str";
    public static final String INDEX_KEY = "_indices";
    
    public void init() throws DBException {
        Properties props = getProperties();
        String redisConnString = props.getProperty(CONN_PROPERTY);
        if (redisConnString == null) {
            return;
        }
        redisShardClient = new RedisShardClient(redisConnString);
        redisShardClient.init();
    }

    public void cleanup() throws DBException {
    	try {
			redisShardClient.destroy();
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
	            StringByteIterator.putAllAsByteIterators(result, redisShardClient.hgetall(key));
	        }
	        else {
	            String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
	            List<String> values = redisShardClient.hmget(key, fieldArray);
	
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
	        if (redisShardClient.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
	        	redisShardClient.zadd(INDEX_KEY, hash(key), key);
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
	        return redisShardClient.del(key) == 0
	            && redisShardClient.zrem(INDEX_KEY, key) == 0
	               ? 1 : 0;
    	}catch(Exception e){
    		e.printStackTrace();
    		return 1;
    	}
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
			return redisShardClient.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
    }

    @Override
    public int scan(String table, String startkey, int recordcount,Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    	 try {
	    	List<String> keys = redisShardClient.zrangebyscore(INDEX_KEY, String.valueOf(hash(startkey)),String.valueOf(Double.POSITIVE_INFINITY), 0, recordcount);
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
