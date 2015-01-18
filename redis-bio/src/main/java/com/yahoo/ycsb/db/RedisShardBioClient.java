package com.yahoo.ycsb.db;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;


import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class RedisShardBioClient extends DB {

	private ShardedJedisPool pool = null;
	//private static Logger log = Logger.getLogger(RedisShardBioClient.class);
    private Integer timeout=2000;
    private Integer maxActive=1;
    private Integer maxIdle=1;
    private Integer maxWait=1;
    private boolean testOnBorrow;
    
    public static final String CONN_PROPERTY = "redis.conn.str";
    public static final String MAXIDLE_PROPERTY = "redis.max.idle";
    public static final String MAXACTIVE_PROPERTY = "redis.max.active";
    public static final String MAXWAIT_PROPERTY = "redis.max.wait";
    public static final String INDEX_KEY = "_indices";
    
    public void init() throws DBException {
        Properties props = getProperties();
        String redisConnString = props.getProperty(CONN_PROPERTY);
        if(redisConnString == null){
        	return;
        }
        String redisMaxIdleString = props.getProperty(MAXIDLE_PROPERTY);
        if(null!=redisMaxIdleString){
        	this.maxIdle = Integer.parseInt(redisMaxIdleString);
        }
        String redisMaxActiveString = props.getProperty(MAXACTIVE_PROPERTY);
        if(null!=redisMaxActiveString){
        	this.maxActive = Integer.parseInt(redisMaxActiveString);
        }
        String redisMaxWaitString = props.getProperty(MAXWAIT_PROPERTY);
        if(null!=redisMaxWaitString){
        	this.maxWait = Integer.parseInt(redisMaxWaitString);
        }
        
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();;
        if(null == redisConnString || redisConnString.isEmpty())
        	throw new ExceptionInInitializerError("redisConnString is empty!");
		List<String> confList = Arrays.asList(redisConnString.split("(?:\\s|,)+"));
		if (null == confList || confList.isEmpty()) 
			throw new ExceptionInInitializerError("confList is empty!");
		for (String address : confList) {
            if (address != null) {
                String[] wAddressArr = address.split(":");
                if (wAddressArr.length == 1) 
                    throw new ExceptionInInitializerError(wAddressArr + " is not include host:port or host:port:passwd after split \":\"");
                String host = wAddressArr[0];
                int port = Integer.valueOf(wAddressArr[1]);
                JedisShardInfo jedisShardInfo = new JedisShardInfo(host, port, this.timeout);
                //log.info("confList:" + jedisShardInfo.toString());
                if (wAddressArr.length == 3 && !wAddressArr[2].isEmpty()) {
                    jedisShardInfo.setPassword(wAddressArr[2]);
                }
                shards.add(jedisShardInfo);
            }
        }
		
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(this.maxActive);
		config.setMaxIdle(this.maxIdle);
		config.setMaxWait(this.maxWait);
		config.setTestOnBorrow(this.testOnBorrow);
		
        this.pool = new ShardedJedisPool(config, shards);
        
    }

    public void cleanup() throws DBException {
    	this.pool.destroy();
    }

    private double hash(String key) {
        return key.hashCode();
    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
    	try{
	        if (fields == null) {
	            StringByteIterator.putAllAsByteIterators(result, this.hgetAll(key));
	        }
	        else {
	            String[] fieldArray = (String[])fields.toArray(new String[fields.size()]);
	            List<String> values = this.hmget(key, fieldArray);
	
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
	        if (this.hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
	        	this.zadd(INDEX_KEY, hash(key), key);
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
	        return this.del(key) == 0
	            && this.zrem(INDEX_KEY, key) == 0
	               ? 1 : 0;
    	}catch(Exception e){
    		e.printStackTrace();
    		return 1;
    	}
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
			return this.hmset(key, StringByteIterator.getStringMap(values)).equals("OK") ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		}
    }

    @Override
    public int scan(String table, String startkey, int recordcount,Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    	 try {
	    	Set<String> keys = this.zrangeByScore(INDEX_KEY, String.valueOf(hash(startkey)),String.valueOf(Double.POSITIVE_INFINITY), 0, recordcount);
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
    
    
    /**
     * 
     * @param key
     * @param member
     * @return
     * @throws Exception
     */
    private Long sadd(String key, String member) throws Exception {
        boolean flag = true;
        ShardedJedis j = null;
        Long result = null;
        try {
            j = pool.getResource();
            result = j.sadd(key, member);
        } catch (Exception ex) {
            flag = false;
            pool.returnBrokenResource(j);
            throw new Exception(ex + "," + j.getShardInfo(key).toString());
        } finally {
            if (flag) {
            	pool.returnResource(j);
            }
        }
        return result;
    }
    
    /**
     * 
     * @param key
     * @return
     * @throws Exception
     */
    private Map<String, String> hgetAll(String key) throws Exception {
        boolean flag = true;
        ShardedJedis j = null;
        Map<String, String> result = null;
        try {
            j = pool.getResource();
            result = j.hgetAll(key);
        } catch (Exception ex) {
            flag = false;
            pool.returnBrokenResource(j);
            throw new Exception(ex + "," + j.getShardInfo(key).toString());
        } finally {
            if (flag) {
            	pool.returnResource(j);
            }
        }
        return result;
    }
    
    
    /**
     * 
     * @param key
     * @param fields
     * @return
     * @throws Exception
     */
    private List<String> hmget(String key, String... fields) throws Exception {
        boolean flag = true;
        ShardedJedis j = null;
        List<String> result = null;
        try {
            j = pool.getResource();
            result = j.hmget(key, fields);
        } catch (Exception ex) {
            flag = false;
            pool.returnBrokenResource(j);
            throw new Exception(ex + "," + j.getShardInfo(key).toString());
        } finally {
            if (flag) {
            	pool.returnResource(j);
            }
        }
        return result;
    }
    
    /**
     * 
     * @param key
     * @param hash
     * @return
     * @throws Exception
     */
    private String hmset(String key, Map<String, String> hash) throws Exception {
        boolean flag = true;
        ShardedJedis j = null;
        String result = null;
        try {
            j = pool.getResource();
            result = j.hmset(key, hash);
        } catch (Exception ex) {
            flag = false;
            pool.returnBrokenResource(j);
            throw new Exception(ex + "," + j.getShardInfo(key).toString());
        } finally {
            if (flag) {
            	pool.returnResource(j);
            }
        }
        return result;
    }
    
    /**
     * 
     * @param key
     * @param score
     * @param member
     * @return
     * @throws Exception
     */
    private Long zadd(String key, double score, String member) throws Exception {
        boolean flag = true;
        ShardedJedis j = null;
        Long result = null;
        try {
            j = pool.getResource();
            result = j.zadd(key, score, member);
        } catch (Exception ex) {
            flag = false;
            pool.returnBrokenResource(j);
            throw new Exception(ex + "," + j.getShardInfo(key).toString());
        } finally {
            if (flag) {
            	pool.returnResource(j);
            }
        }
        return result;
    }

    /**
     * 
     * @param key
     * @param members
     * @return
     * @throws Exception
     */
    private Long zrem(String key, String... members) throws Exception {
        boolean flag = true;
        ShardedJedis j = null;
        Long result = null;
        try {
            j = pool.getResource();
            result = j.zrem(key, members);
        } catch (Exception ex) {
            flag = false;
            pool.returnBrokenResource(j);
            throw new Exception(ex + "," + j.getShardInfo(key).toString());
        } finally {
            if (flag) {
            	pool.returnResource(j);
            }
        }
        return result;
    }
    
    
    /**
     * 
     * @param key
     * @return
     * @throws Exception
     */
    private Long del(String key) throws Exception {
        boolean flag = true;
        ShardedJedis j = null;
        Long result = null;
        try {
            j = pool.getResource();
            result = j.del(key);
        } catch (Exception ex) {
            flag = false;
            pool.returnBrokenResource(j);
            throw new Exception(ex + "," + j.getShardInfo(key).toString());
        } finally {
            if (flag) {
            	pool.returnResource(j);
            }
        }
        return result;
    }
    
    
    /**
     * 
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return
     * @throws Exception
     */
    public Set<String> zrangeByScore(String key, String min, String max,int offset, int count) throws Exception {
        boolean flag = true;
        ShardedJedis j = null;
        Set<String> result = null;
        try {
            j = pool.getResource();
            result = j.zrangeByScore(key, min, max, offset, count);
        } catch (Exception ex) {
            flag = false;
            pool.returnBrokenResource(j);
            throw new Exception(ex + "," + j.getShardInfo(key).toString());
        } finally {
            if (flag) {
            	pool.returnResource(j);
            }
        }
        return result;
    }
    	    
    	    
    	    
}
