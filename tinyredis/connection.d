module tinyredis.connection;

/**
 * Authors: Adil Baig, adil.baig@aidezigns.com
 */

public:
    import std.socket : TcpSocket, SocketOption, SocketOptionLevel;
	    
private:
    import std.array : appender, back, popBack;
    import tinyredis.parser;
    import tinyredis.response;
    import core.time;

debug {
	import std.stdio : writeln;
	import tinyredis.encoder : escape;
}    

public:

	/**
     * Sends a pre-encoded string
     *
     * Params:
     *   conn     	 = Connection to redis server.
     *   encoded_cmd = The command to be sent.
     *
     * Throws: $(D ConnectionException) if sending fails.
     */
	void send(TcpSocket conn, string encoded_cmd)
    {
        debug { writeln("Request : '", escape(encoded_cmd) ~ "'"); }
        
        auto sent = conn.send(encoded_cmd);
        if (sent != (cast(byte[])encoded_cmd).length)
            throw new ConnectionException("Error while sending request");
    }

    /**
     * Receive responses from redis server
     *
     * Params:
     *   conn    	  = Connection to redis server.
     *   minResponses = The number of multibulks you expect
     *
     * Throws: $(D ConnectionException) if there is a socket error or server closes the connection.
     */
    Response[] receiveResponses(TcpSocket conn, size_t minResponses = 0)
    {
        return receiveResponses(conn, minResponses, -1);
    }

    /**
     * Receive responses from redis server
     *
     * Params:
     *   conn         = Connection to redis server.
     *   minResponses = The number of multibulks you expect
     *   d = The direction in milliseconds
     *
     * Throws: $(D ConnectionException) if there is a socket error or server closes the connection.
     */
    Response[] receiveResponses(TcpSocket conn, size_t minResponses = 0, int d = -1)
    {
        byte[] buffer;
        Response[] responses;
        Response*[] MultiBulks; //Stack of pointers to multibulks
        Response[]* stackPtr = &responses;
        
        while(true)
        {
            receive(conn, buffer, d);

            debug(2){ writeln("BUFFER : ", escape(cast(string)buffer)); } 
            
            while(buffer.length > 0)
            {
                auto r = parseResponse(buffer);
                if(r.type == ResponseType.Invalid)
                     break;
               
                *stackPtr ~= r;
                if(r.type == ResponseType.MultiBulk)
                {
                    auto mb = &((*stackPtr)[$-1]);
                    if(mb.count > 0)
                    {
                        MultiBulks ~= mb;
                        stackPtr = &((*mb).values);
                    }
                }
                else
                    while(MultiBulks.length > 0)
                    {
                        auto mb = *(MultiBulks.back);
                        
                        if(mb.count == mb.values.length)
                        {
                            MultiBulks.popBack();
                            
                            if(MultiBulks.length > 0)
                                stackPtr = &((*MultiBulks.back).values);
                            else
                                stackPtr = &responses;
                        }
                        else
                            break;
                    }
            }
            
            if(buffer.length == 0 && MultiBulks.length == 0) //Make sure all the multi bulks got their data
            {
                debug(2) {
                    if(minResponses > 1 && responses.length < minResponses)
                        writeln("WAITING FOR MORE RESPONSES ... ");
                }
                    
                if(responses.length < minResponses)
                    continue;
                    
                break;
            }
                
        }
        
        return responses;
    }
    
   /* -------- EXCEPTIONS ------------- */
 
    class ConnectionException : Exception {
        this(string msg) { super(msg); }
    }


private :
    
    void receive(TcpSocket conn, ref byte[] buffer, int d)
    {
        byte[1024 * 16] buff;

        if (d > -1) { 
        conn.setOption(SocketOptionLevel.SOCKET,
            SocketOption.RCVTIMEO, d.msecs); // dur!"seconds"(1)
        }
        size_t len = conn.receive(buff);

        
        if(len == 0)
            throw new ConnectionException("Server closed the connection!");
        else if(len == TcpSocket.ERROR && d > 0)
            writeln("Timed out in ", d, "msecs");
        else if(len == TcpSocket.ERROR && d == -1)
            throw new ConnectionException("A socket error occurred!");
        else {
            buffer ~= buff[0 .. len];
            debug(2) { writeln("Response : ", "'" ~ escape(cast(string)buff) ~ "'", " Length : ", len); }
        }
    }
