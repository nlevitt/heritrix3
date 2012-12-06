/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.archive.modules.fetcher;

import static org.archive.modules.CrawlURI.FetchType.HTTP_POST;
import static org.archive.modules.fetcher.FetchErrors.LENGTH_TRUNC;
import static org.archive.modules.fetcher.FetchErrors.TIMER_TRUNC;
import static org.archive.modules.fetcher.FetchStatusCodes.S_CONNECT_LOST;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.protocol.BasicHttpContext;
import org.archive.io.RecorderLengthExceededException;
import org.archive.io.RecorderTimeoutException;
import org.archive.modules.CrawlURI;
import org.archive.modules.CrawlURI.FetchType;
import org.archive.util.Recorder;

public class FetchHTTPModifiedHttpClient extends FetchHTTPHttpClient {

    private static Logger logger = Logger.getLogger(FetchHTTPModifiedHttpClient.class.getName());

    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        threadActiveCrawlURI.set(curi);
        
        // could call after fetch in finally block, but right here feels even more sure
        resetHttpClient();
        
        // Note begin time
        curi.setFetchBeginTime(System.currentTimeMillis());

        // Get a reference to the HttpRecorder that is set into this ToeThread.
        final Recorder rec = curi.getRecorder();

        // Shall we get a digest on the content downloaded?
        boolean digestContent = getDigestContent();
        String algorithm = null;
        if (digestContent) {
            algorithm = getDigestAlgorithm();
            rec.getRecordedInput().setDigest(algorithm);
        } else {
            // clear
            rec.getRecordedInput().setDigest((MessageDigest)null);
        }

        String curiString = curi.getUURI().toString();
        HttpRequestBase request = null;
        if (curi.getFetchType() == FetchType.HTTP_POST) {
            request = new HttpPost(curiString);
            curi.setFetchType(FetchType.HTTP_POST);
        } else {
            request = new HttpGet(curiString);
            curi.setFetchType(FetchType.HTTP_GET);
        }
        
        threadActiveRequest.set(request);
        configureRequest(curi, request);

        HttpHost targetHost = URIUtils.extractHost(request.getURI());
        
        // Populate credentials. Set config so auth. is not automatic.
        BasicHttpContext contextForAuth = new BasicHttpContext();
        boolean addedCredentials = populateTargetCredentials(curi, request, targetHost, contextForAuth);
        populateHttpProxyCredential(curi, request, contextForAuth);
        
        // set hardMax on bytes (if set by operator)
        long hardMax = getMaxLengthBytes();
        // set overall timeout (if set by operator)
        long timeoutMs = 1000 * getTimeoutSeconds();
        // Get max fetch rate (bytes/ms). It comes in in KB/sec
        long maxRateKBps = getMaxFetchKBSec();
        rec.getRecordedInput().setLimits(hardMax, timeoutMs, maxRateKBps);

        HttpResponse response = null;
        try {
            response = httpClient().execute(targetHost, request, contextForAuth);
            addResponseContent(response, curi);
        } catch (ClientProtocolException e) {
            failedExecuteCleanup(request, curi, e);
            return;
        } catch (IOException e) {
            failedExecuteCleanup(request, curi, e);
            return;
        }
        
        // set softMax on bytes to get (if implied by content-length)
        long softMax = -1l;
        Header h = response.getLastHeader("content-length");
        if (h != null) {
            softMax = Long.parseLong(h.getValue());
        }
        try {
            if (!request.isAborted()) {
                // Force read-to-end, so that any socket hangs occur here,
                // not in later modules.
                rec.getRecordedInput().readFullyOrUntil(softMax); 
            }
        } catch (RecorderTimeoutException ex) {
            doAbort(curi, request, TIMER_TRUNC);
        } catch (RecorderLengthExceededException ex) {
            doAbort(curi, request, LENGTH_TRUNC);
        } catch (IOException e) {
            cleanup(curi, e, "readFully", S_CONNECT_LOST);
            return;
        } catch (ArrayIndexOutOfBoundsException e) {
            // For weird windows-only ArrayIndex exceptions from native code
            // see http://forum.java.sun.com/thread.jsp?forum=11&thread=378356
            // treating as if it were an IOException
            cleanup(curi, e, "readFully", S_CONNECT_LOST);
            return;
        } finally {
            rec.close();
            // ensure recording has stopped
            rec.closeRecorders();
            if (!request.isAborted()) {
                request.reset();
            }
            // Note completion time
            curi.setFetchCompletedTime(System.currentTimeMillis());
            
            // finished with these (midfetch abort would have happened already)
            threadActiveCrawlURI.set(null);
            threadActiveRequest.set(null);

            // Set the response charset into the HttpRecord if available.
            setCharacterEncoding(curi, rec, response);
            setSizes(curi, rec);
            setOtherCodings(curi, rec, response); 
        }

        if (digestContent) {
            curi.setContentDigest(algorithm, 
                rec.getRecordedInput().getDigestValue());
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(((curi.getFetchType() == HTTP_POST) ? "POST" : "GET")
                    + " " + curi.getUURI().toString() + " "
                    + response.getStatusLine().getStatusCode() + " "
                    + rec.getRecordedInput().getSize() + " "
                    + curi.getContentType());
        }

        if (isSuccess(curi) && addedCredentials) {
            // Promote the credentials from the CrawlURI to the CrawlServer
            // so they are available for all subsequent CrawlURIs on this
            // server.
            promoteCredentials(curi);
        } else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
            // 401 is not 'success'.
            handle401(response, curi);
        } else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_PROXY_AUTHENTICATION_REQUIRED) {
            // 407 - remember Proxy-Authenticate headers for later use 
            kp.put("proxyAuthChallenges", 
                    extractChallenges(response, curi, httpClient().getProxyAuthenticationStrategy()));
        }

        if (rec.getRecordedInput().isOpen()) {
            logger.severe(curi.toString() + " RIS still open. Should have"
                    + " been closed by method release: "
                    + Thread.currentThread().getName());
            try {
                rec.getRecordedInput().close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "second-chance RIS close failed", e);
            }
        }    }
}
