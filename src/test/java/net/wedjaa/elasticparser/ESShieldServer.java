/****
 *
 * Copyright 2013-2016 Wedjaa <http://www.wedjaa.net/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

/**
 * @author Fabio Torchetti
 *
 */

package net.wedjaa.elasticparser;

import com.google.common.io.BaseEncoding;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ESShieldServer implements Runnable{

    private int listenPort;
    private String request;

    static Logger logger = Logger.getLogger(ESSearchTest.class);

    public ESShieldServer(int port) {
        logger.debug("Creating Listening Server on port " + port);
        this.listenPort = port;
    }

    public boolean authenticated(String username, String password) {

        String authenticationString = username + ":" + password;

        try {
            String authenticationToken = BaseEncoding.base64().encode(authenticationString.getBytes("UTF-8"));
            if (request.indexOf(authenticationToken) >= 0 ) {
                return true;
            }
        } catch (UnsupportedEncodingException ex) {
            logger.warn("Error creating token for comparison: " + ex.getLocalizedMessage());
        }

        return false;
    }

    public void run()
    {
        try {
            ServerSocket server = new ServerSocket(this.listenPort);
            logger.debug("Waiting for connection...");
            Socket incoming = server.accept();
            logger.debug("...client connected!");
            InputStreamReader reader = new InputStreamReader(incoming.getInputStream());
            CharArrayWriter writer = new CharArrayWriter();
            int readBytes;
            char [] chunk = new char[4096];
            while ((readBytes = reader.read(chunk, 0, chunk.length)) != -1) {
                writer.write(chunk, 0, readBytes);
            }
            request = new String(writer.toCharArray());
            logger.debug("Received: " + request);
        } catch( Exception ex) {
            logger.error("Error reading request: " + ex.getLocalizedMessage());
        }
    }
}
