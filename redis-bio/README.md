## Quick Start

This section describes how to run YCSB of redis nio client. 

### 1. Start Redis

### 2. Install Java and Maven

### 3. Set Up YCSB-mini

Git clone YCSB and compile:

    git clone http://github.com/fengpeiyuan/YCSB-mini.git
    cd YCSB-mini

### 4. Provide Redis Connection Parameters
    
Set the host, port, and password (do not redis auth is not turned on) in the 
workload you plan to run.

- `redis.conn.str`
- `redis.max.active,redis.max.idle,redis.max.wait` is for bio client used in connection pool

Or, you can set configs with the shell command, EG:

	bin/ycsb load redis-nio -P workloads/workloada -p "redis.conn.str=192.168.151.94:6381:12345"

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load redis-nio -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run redis-nio -s -P workloads/workloada > outputRun.txt
    
