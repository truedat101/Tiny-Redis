module tinyredis.redis;

/**
 * Authors: Adil Baig, adil.baig@aidezigns.com
 */
/**
 * Portions by truedat101 (github) 2015
 */
private:
    import std.array : appender;
    import std.socket : TcpSocket, InternetAddress, SocketShutdown;
    import std.traits;
    import core.thread;
    import core.atomic;

public :
    import tinyredis.connection;
    import tinyredis.encoder;
    import tinyredis.response;
    import tinyredis.parser : RedisResponseException;
    
    

    class subscriberContext {
        public int totalremaining;
        public string channel;
    }

    //
    // XXX Is there any difference in writing it different ways?
    //
    alias void function(Object context, Response resp) RedisSubCBFunc;
    alias void delegate(Object context, Response resp) RedisSubCBDelegate;
    // alias RedisSubCBFunc2 = void function(Object context, Response resp);

    class Redis
    {
        private:
            TcpSocket conn;
            RedisSubCBFunc[string] subscribers; // Key by channel
            Thread subworker;
            shared(bool) isSubworkerDone;
            bool isConnValid;

            void f1() { // XXX Maybe extend Thread instead
                auto keys = subscribers.keys();
                auto channels = "";

                //
                // Only subscribe once!!!
                //
                for (int i = 0; i < keys.length; i++) {
                    // XXX Is there an easier way to get this from a list?
                    channels = channels ~ keys[i] ~ " ";
                }

                // XXX We should double check that we have enough sub channels to make a command (at least one)
                auto cmd = "SUBSCRIBE " ~ channels;
                debug{ writeln(escape(toMultiBulk(cmd)));}
                conn.send(toMultiBulk(cmd));

                while(!isSubworkerDone) {
                    writeln("TICK*******************");
                    writeln("f1 handling subscriber: ", channels);

                    Response[] r = receiveResponses(conn, 1);

                    for (int j = 0; j < r.length; j++) {
                        // writeln(r[j]);
                        writeln("Processing ", (r.length-j), " more responses");
                        subscriberContext context = new subscriberContext();
                        context.totalremaining = cast(int) r.length-j-1;
                        context.channel = r[j].values[1].value;
                        subscribers[context.channel](context, r[j]);
                    }
                    Thread.sleep(200.msecs);
                }

                //
                // Unsubscribe you jerk
                //
                cmd = "UNSUBSCRIBE " ~ channels;
                writeln("unsubsribe all");
                conn.send(toMultiBulk(cmd));
                // XXX Do we really need to check the response?
                writeln("subworker is done, returning");
                return;
            };
        public:
        
        
        /**
         * Create a new connection to the Redis server
         */
        this(string host = "127.0.0.1", ushort port = 6379)
        {
            conn = new TcpSocket(new InternetAddress(host, port));
            isConnValid = true;
        }
        
        bool isRedisConnValid() {
            return isConnValid;
        }

        void shutdown() {
            if (conn !is null) {
                conn.shutdown(SocketShutdown.BOTH);
                conn.close();
                isConnValid = false;
                conn = null;
            }
        }
        /**
         * Call Redis using any type T that can be converted to a string
         *
         * Examples:
         *
         * ---
         * send("SET name Adil")
         * send("SADD", "myset", 1)
         * send("SADD", "myset", 1.2)
         * send("SADD", "myset", true)
         * send("SADD", "myset", "Batman")
         * send("SREM", "myset", ["$3", "$4"])
         * send("SADD", "myset", object) //provided 'object' implements toString()
         * send("GET", "*") == send("GET *")
         * send("ZADD", "my_unique_json", 1, json.toString());
         * send("EVAL", "return redis.call('set','lua','LUA')", 0); 
         * ---
         */
        R send(R = Response, T...)(string key, T args)
        {
        	//Implement a write queue here.
        	// All encoded responses are put into a write queue and flushed
        	// For a send request, flush the queue and listen to a response
        	// For async calls, just flush the queue
        	// This automatically gives us PubSub
        	 
        	debug{ writeln(escape(toMultiBulk(key, args)));}
        	 
        	conn.send(toMultiBulk(key, args));
        	Response[] r = receiveResponses(conn, 1);
            return cast(R)(r[0]);
        }
        
        R send(R = Response)(string cmd)
        {
        	debug{ writeln(escape(toMultiBulk(cmd)));}
        	
        	conn.send(toMultiBulk(cmd));
        	Response[] r = receiveResponses(conn, 1);
            return cast(R)(r[0]);
        }


        /*
            channels are separated by space
            XXX This blocks forever, run from another thread, or put a timeout and unsubscribe at timeout
        */
        void subBlock(string channels, RedisSubCBFunc cb) {
            auto cmd = "SUBSCRIBE " ~ channels; // XXX TODO validate input
            conn.send(toMultiBulk(cmd));
            Response[] r = receiveResponses(conn, 1);
            while (true) { // XXX Need a way to break out
                for (int i = 0; i < r.length; i++) {
                    writeln("GOT SUB RESP: ", r[i]);
                    // XXX Not convinced this is a good idea
                    subscriberContext context = new subscriberContext();
                    context.totalremaining = cast(int) r.length-1;
                    context.channel = r[i].values[1].value; 
                    cb(context, r[i]);
                }

                Thread.sleep(200.msecs);
                r = receiveResponses(conn, 1);
            }
        }

        /*
            channels are separated by space
            XXX This blocks forever, run from another thread, or put a timeout and unsubscribe at timeout
        */
        void subBlock(string channels, RedisSubCBDelegate cb) {
            auto cmd = "SUBSCRIBE " ~ channels; // XXX TODO validate input
            conn.send(toMultiBulk(cmd));
            Response[] r = receiveResponses(conn, 1);
            while (true) { // XXX Need a way to break out
                for (int i = 0; i < r.length; i++) {
                    writeln("GOT SUB RESP: ", r[i]);
                    // XXX Not convinced this is a good idea
                    subscriberContext context = new subscriberContext();
                    context.totalremaining = cast(int) r.length-1;
                    context.channel = r[i].values[1].value; 
                    cb(context, r[i]);
                }

                Thread.sleep(200.msecs);
                r = receiveResponses(conn, 1);
            }
        }
        /*
             Insert the subscriber for the channel, and replace it if it exists already
             Only use one channel at a time

             XXX TODO: Implement multichannel signature ... also, multiple channels with a single cb
        */
        void subscribe(string channel, RedisSubCBFunc cb) {
            subscribers[channel] = cb;
        }

        void unsubscribe(string channel) {
            subscribers.remove(channel);
        }
        
        int getSubscriberCount() {
            return cast(int) subscribers.length;
        }

       bool areSubscriptionsStarted() {
            synchronized {
            if (isSubworkerDone) {
                return false;
            } else {
                return true;
            }
            }
        }

        int startSubscriptions() {
            int status = -1;

            if (subworker !is null && subworker.isRunning()) {
                return status;
            } else {
                try {
                    subworker = new Thread(&f1);
                    subworker.start();
                    status = 0;
                    return status;
                } catch(ThreadException te) {
                    return status;
                }
            }
        }

        int stopSubscriptions() {
            int status = -1;
            isSubworkerDone = true;

            //
            // XXX Need a better way to be sure the subscription thread is done and not stuck
            // 
            if (subworker is null) {
                writeln("subworker is null");
                return status;
            } else {
                subworker = null;
                // we should remove all subscribers and null it out only when we are sure
            }

            status = 0;

            return status;
        }

        /**
         * Send a string that is already encoded in the Redis protocol
         */
        R sendRaw(R = Response)(string cmd)
        {
        	debug{ writeln(escape(cmd));}
        	
        	conn.send(cmd);
        	Response[] r = receiveResponses(conn, 1);
            return cast(R)(r[0]);
        }
        
        /**
         * Send a series of commands as a pipeline
         *
         * Examples:
         *
         * ---
         * pipeline(["SADD shopping_cart Shirt", "SADD shopping_cart Pant", "SADD shopping_cart Boots"])
         * ---
         */
        Response[] pipeline(C)(C[][] commands) if (isSomeChar!C)
        {
        	auto appender = appender!(C[])();
            foreach(c; commands) {
                appender ~= encode(c);
            }
            
            conn.send(appender.data);
        	return receiveResponses(conn, commands.length);
        }
        
        /**
         * Execute commands in a MULTI/EXEC block.
         * 
         * @param all - (Default: false) - By default, only the results of a transaction are returned. If set to "true", the results of each queuing step is also returned. 
         *
         * Examples:
         *
         * ---
         * transaction(["SADD shopping_cart Shirt", "INCR shopping_cart_ctr"])
         * ---
         */
        Response[] transaction(string[] commands, bool all = false)
        {
            auto cmd = ["MULTI"];
            cmd ~= commands;
            cmd ~= "EXEC";
            auto rez = pipeline(cmd);
            
            if(all) {
                return rez;
            }
            
            auto resp = rez[$ - 1];
            if(resp.isError()) {
                throw new RedisResponseException(resp.value);
            }
            
            return resp.values;
        }
        
        /**
         * Simplified call to EVAL
         *
         * Examples:
         *
         * ---
         * Response r = eval("return redis.call('set','lua','LUA_AGAIN')");
         * r.value == "LUA_AGAIN";
         * 
         * Response r1 = redis.eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", ["key1", "key2"], ["first", "second"]);
         * writeln(r1); // [key1, key2, first, second]
         *
         * Response r1 = redis.eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", [1, 2]);
         * writeln(r1); // [1, 2]
         * ---
         */
        Response eval(K = string, A = string)(string lua_script, K[] keys = [], A[] args = [])
        {
            conn.send(toMultiBulk("EVAL", lua_script, keys.length, keys, args));
        	Response[] r = receiveResponses(conn, 1);
            return (r[0]);
        }
        
        Response evalSha(K = string, A = string)(string sha1, K[] keys = [], A[] args = [])
        {
            conn.send(toMultiBulk("EVALSHA", sha1, keys.length, keys, args));
            Response[] r = receiveResponses(conn, 1);
            return (r[0]);
        }
        
    }
   
   
   
unittest
{
    auto redis = new Redis();
    auto redis2 = new Redis();

    assert(redis.isRedisConnValid() == true);
    auto response = redis.send("LASTSAVE");
    assert(response.type == ResponseType.Integer);
    
    assert(redis.send!(bool)("SET", "name", "adil baig"));
    
    redis.send("SET emptystring ''");
    response = redis.send("GET emptystring");
    assert(response.value == "");
    
    response = redis.send("GET name");
    assert(response.type == ResponseType.Bulk);
    assert(response.value == "adil baig");
    
    /* START Test casting byte[] */
    assert(cast(byte[])response == "adil baig"); //Test casting to byte[]
    assert(cast(byte[])response == [97, 100, 105, 108, 32, 98, 97, 105, 103]);
    
    redis.send("SET mykey 10");
    response = redis.send("INCR mykey");
    assert(response.type == ResponseType.Integer);
    assert(response.intval == 11);
    auto bytes = (cast(ubyte[])response);
    assert(bytes.length == response.intval.sizeof);
    assert(bytes[0] == 11);
    /* END Test casting byte[] */
    
    assert(redis.send!(string)("GET name") == "adil baig");
    
    response = redis.send("GET nonexistentkey");
    assert(response.type == ResponseType.Nil);
    assert(cast(ubyte[])response == []); 
    
    redis.send("DEL myset");
    redis.send("SADD", "myset", 1.2);
    redis.send("SADD", "myset", 1);
    redis.send("SADD", "myset", true);
    redis.send("SADD", "myset", "adil");
    redis.send("SADD", "myset", 350001939);
    redis.send("SADD", ["myset","$4"]);
    auto r = redis.send("SMEMBERS myset");
    assert(r.type == ResponseType.MultiBulk);
    assert(r.values.length == 6);
    
    //Check pipeline
    redis.send("DEL ctr");
    auto responses = redis.pipeline(["SET ctr 1", "INCR ctr", "INCR ctr", "INCR ctr", "INCR ctr"]);
    
    assert(responses.length == 5);
    assert(responses[0].type == ResponseType.Status);
    assert(responses[1].intval == 2);
    assert(responses[2].intval == 3);
    assert(responses[3].intval == 4);
    assert(responses[4].intval == 5);
    
    redis.send("DEL buddies");
    auto buddiesQ = ["SADD buddies Batman", "SADD buddies Spiderman", "SADD buddies Hulk", "SMEMBERS buddies"];
    Response[] buddies = redis.pipeline(buddiesQ);
    assert(buddies.length == buddiesQ.length);
    assert(buddies[0].type == ResponseType.Integer);
    assert(buddies[1].type == ResponseType.Integer);
    assert(buddies[2].type == ResponseType.Integer);
    assert(buddies[3].type == ResponseType.MultiBulk);
    assert(buddies[3].values.length == 3);
    
    //Check transaction
    redis.send("DEL ctr");
    responses = redis.transaction(["SET ctr 1", "INCR ctr", "INCR ctr"], true);
    assert(responses.length == 5);
    assert(responses[0].type == ResponseType.Status);
    assert(responses[1].type == ResponseType.Status);
    assert(responses[2].type == ResponseType.Status);
    assert(responses[3].type == ResponseType.Status);
    assert(responses[4].type == ResponseType.MultiBulk);
    assert(responses[4].values[0].type == ResponseType.Status);
    assert(responses[4].values[1].intval == 2);
    assert(responses[4].values[2].intval == 3);
    
    redis.send("DEL ctr");
    responses = redis.transaction(["SET ctr 1", "INCR ctr", "INCR ctr"]);
    assert(responses.length == 3);
    assert(responses[0].type == ResponseType.Status);
    assert(responses[1].intval == 2);
    assert(responses[2].intval == 3);
    
    response = redis.send("EVAL", "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", 2, "key1", "key2", "first", "second");
    assert(response.values.length == 4);
    assert(response.values[0].value == "key1");
    assert(response.values[1].value == "key2");
    assert(response.values[2].value == "first");
    assert(response.values[3].value == "second");
    
    //Same as above, but simpler
    response = redis.eval("return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}", ["key1", "key2"], ["first", "second"]);
    assert(response.values.length == 4);
    assert(response.values[0].value == "key1");
    assert(response.values[1].value == "key2");
    assert(response.values[2].value == "first");
    assert(response.values[3].value == "second");
    
    response = redis.eval("return redis.call('set','lua','LUA_AGAIN')");
    assert(cast(string)redis.send("GET lua") == "LUA_AGAIN");
    
    // A BLPOP times out to a Nil multibulk
    response = redis.send("BLPOP nonExistentList 1");
    assert(response.isNil());

    // Verify subscriber apis
    // static alias void function(Object context, Response resp) RedisSubCBFunc;
    RedisSubCBFunc cb1 = function(context, resp) {
        writeln("cb1 invoked with resp:", resp.toDiagnosticString(), " context.totalremaining=", (cast(subscriberContext) context).totalremaining, " context.channel=", (cast(subscriberContext) context).channel);
    };

    RedisSubCBFunc cb2 = function(context, resp) {
        writeln("cb2 invoked with resp:", resp.toDiagnosticString(), " context.totalremaining=", (cast(subscriberContext) context).totalremaining, " context.channel=", (cast(subscriberContext) context).channel);
    };

    //
    // Test the blocking call for sub ... XXX TODO, add this to a thread
    // 
    /*
    redis.subBlock("foochan foochan2", cb1);
    */
    

    redis.subscribe("foochan", cb1);
    redis.subscribe("baaasim", cb2);
    assert(redis.getSubscriberCount() == 2);

    redis.unsubscribe("baaasim");
    redis.unsubscribe("babasim");
    assert(redis.getSubscriberCount() == 1);
    assert(redis.startSubscriptions() == 0);
    assert(redis.startSubscriptions() == -1);
    
    /* for (int i = 0; i < 100000; i++) {
        float x = 10*10.0*10;
    } */

    if (redis.areSubscriptionsStarted()) {
        //
        // Note we must do this on a different redis connection or else we clog the toilet
        // #2 style
        //
        redis2.send("PUBLISH foochan again1");
        redis2.send("PUBLISH foochan2 againagain2");
        redis2.send("PUBLISH foochan againagainagain3");
        redis2.send("PUBLISH foochan again4");
        redis2.send("PUBLISH foochan againagain5");
        redis2.send("PUBLISH foochan2 againagainagain6");
        redis2.send("PUBLISH foochan again7");
        redis2.send("PUBLISH foochan againagain8");
        redis2.send("PUBLISH foochan againagainagain9");
        redis2.send("PUBLISH foochan again10");
        redis2.send("PUBLISH foochan againagain11");
        redis2.send("PUBLISH foochan againagainagain12");
        redis2.send("PUBLISH foochan again13");
        redis2.send("PUBLISH foochan againagain14");
        redis2.send("PUBLISH foochan againagainagain15");
        redis2.send("PUBLISH foochan2 again16");
        redis2.send("PUBLISH foochan againagain17");
        redis2.send("PUBLISH foochan againagainagain18");
        redis2.send("PUBLISH foochan again19");
        redis2.send("PUBLISH foochan againagain20");
        redis2.send("PUBLISH foochan againagainagain21");
        redis2.send("PUBLISH foochan again22");
        redis2.send("PUBLISH foochan againagain23");
        redis2.send("PUBLISH foochan againagainagain24");
        redis2.send("PUBLISH foochan again25");
        redis2.send("PUBLISH foochan againagain26");
        redis2.send("PUBLISH foochan2 againagainagain27");
    }
    Thread.sleep(5.seconds);



    assert(redis.stopSubscriptions() == 0);
    assert(redis.stopSubscriptions() == -1);

    redis.shutdown();
    assert(redis.isRedisConnValid() == false);
    redis2.shutdown();
    writeln("DONE WITH TESTS");
    //
    // XXX NOTE, thread is stuck waiting here, fix this ... subscriber should release thread
    //
}