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

import static org.archive.modules.fetcher.FetchStatusCodes.S_CONNECT_FAILED;
import static org.archive.modules.fetcher.FetchStatusCodes.S_DOMAIN_PREREQUISITE_FAILURE;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_REFERENCE_LENGTH;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.apache.commons.httpclient.URIException;
import org.archive.httpclient.ConfigurableX509TrustManager;
import org.archive.httpclient.ConfigurableX509TrustManager.TrustLevel;
import org.archive.modules.CrawlURI;
import org.archive.modules.credential.Credential;
import org.archive.modules.credential.CredentialStore;
import org.archive.modules.deciderules.AcceptDecideRule;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.deciderules.DecideRule;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.CrawlServer;
import org.archive.modules.net.ServerCache;
import org.archive.util.Recorder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.Lifecycle;

/**
 * HTTP fetcher that uses <a href="http://hc.apache.org/">Apache HttpCore</a>.
 * @contributor nlevitt
 */
public class FetchHTTPJavaNetURL extends FetchHTTPBase implements Lifecycle {

    private static Logger logger = Logger.getLogger(FetchHTTPJavaNetURL.class.getName());

    public static final String REFERER = "Referer";
    public static final String RANGE = "Range";
    public static final String RANGE_PREFIX = "bytes=0-";
    public static final String HTTP_SCHEME = "http";
    public static final String HTTPS_SCHEME = "https";

    protected ServerCache serverCache;
    public ServerCache getServerCache() {
        return this.serverCache;
    }
    /**
     * Used to do DNS lookups.
     */
    @Autowired
    public void setServerCache(ServerCache serverCache) {
        this.serverCache = serverCache;
    }

    {
        setDigestContent(true);
    }
    public boolean getDigestContent() {
        return (Boolean) kp.get("digestContent");
    }
    /**
     * Whether or not to perform an on-the-fly digest hash of retrieved
     * content-bodies.
     */
    public void setDigestContent(boolean digest) {
        kp.put("digestContent",digest);
    }
 
    protected String digestAlgorithm = "sha1";
    public String getDigestAlgorithm() {
        return digestAlgorithm;
    }
    /**
     * Which algorithm (for example MD5 or SHA-1) to use to perform an
     * on-the-fly digest hash of retrieved content-bodies.
     */
    public void setDigestAlgorithm(String digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    public UserAgentProvider getUserAgentProvider() {
        return (UserAgentProvider) kp.get("userAgentProvider");
    }
    @Autowired
    public void setUserAgentProvider(UserAgentProvider provider) {
        kp.put("userAgentProvider",provider);
    }


    {
        setSendConnectionClose(true);
    }
    public boolean getSendConnectionClose() {
        return (Boolean) kp.get("sendConnectionClose");
    }
    /**
     * Send 'Connection: close' header with every request.
     */
    public void setSendConnectionClose(boolean sendClose) {
        kp.put("sendConnectionClose",sendClose);
    }
    
    {
        setDefaultEncoding("ISO-8859-1");
    }
    public String getDefaultEncoding() {
        return getDefaultCharset().name();
    }
    /**
     * The character encoding to use for files that do not have one specified in
     * the HTTP response headers. Default: ISO-8859-1.
     */
    public void setDefaultEncoding(String encoding) {
        kp.put("defaultEncoding",Charset.forName(encoding));
    }
    public Charset getDefaultCharset() {
        return (Charset)kp.get("defaultEncoding");
    }

    {
        setUseHTTP11(false);
    }
    public boolean getUseHTTP11() {
        return (Boolean) kp.get("useHTTP11");
    }
    /**
     * Use HTTP/1.1. Note: even when offering an HTTP/1.1 request, 
     * Heritrix may not properly handle persistent/keep-alive connections, 
     * so the sendConnectionClose parameter should remain 'true'. 
     */
    public void setUseHTTP11(boolean useHTTP11) {
        kp.put("useHTTP11",useHTTP11);
    }

    {
        setIgnoreCookies(false);
    }
    public boolean getIgnoreCookies() {
        return (Boolean) kp.get("ignoreCookies");
    }
    /**
     * Disable cookie handling.
     */
    public void setIgnoreCookies(boolean ignoreCookies) {
        kp.put("ignoreCookies",ignoreCookies);
    }

    {
        setSendReferer(true);
    }
    public boolean getSendReferer() {
        return (Boolean) kp.get("sendReferer");
    }
    /**
     * Send 'Referer' header with every request.
     * <p>
     * The 'Referer' header contans the location the crawler came from, the page
     * the current URI was discovered in. The 'Referer' usually is logged on the
     * remote server and can be of assistance to webmasters trying to figure how
     * a crawler got to a particular area on a site.
     */
    public void setSendReferer(boolean sendReferer) {
        kp.put("sendReferer",sendReferer);
    }

    {
        setAcceptCompression(false);
    }
    public boolean getAcceptCompression() {
        return (Boolean) kp.get("acceptCompression");
    }
    /**
     * Set headers to accept compressed responses. 
     */
    public void setAcceptCompression(boolean acceptCompression) {
        kp.put("acceptCompression", acceptCompression);
    }
    
    {
        setAcceptHeaders(Arrays.asList("Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"));
    }
    @SuppressWarnings("unchecked")
    public List<String> getAcceptHeaders() {
        return (List<String>) kp.get("acceptHeaders");
    }
    /**
     * Accept Headers to include in each request. Each must be the complete
     * header, e.g., 'Accept-Language: en'. (Thus, this can also be used to
     * other headers not beginning 'Accept-' as well.) By default heritrix sends
     * an Accept header similar to what a typical browser would send (the value
     * comes from Firefox 4.0).
     */
    public void setAcceptHeaders(List<String> headers) {
        kp.put("acceptHeaders",headers);
    }
    
    protected AbstractCookieStore cookieStore;
    @Autowired(required=false)
    public void setCookieStore(AbstractCookieStore store) {
        this.cookieStore = store; 
    }
    public AbstractCookieStore getCookieStore() {
        return cookieStore;
    }
    
    {
        // initialize with empty store so declaration not required
        setCredentialStore(new CredentialStore());
    }
    public CredentialStore getCredentialStore() {
        return (CredentialStore) kp.get("credentialStore");
    }
    /**
     * Used to store credentials.
     */
    @Autowired(required=false)
    public void setCredentialStore(CredentialStore credentials) {
        kp.put("credentialStore",credentials);
    }
    
    public String getHttpBindAddress(){
        return (String) kp.get(HTTP_BIND_ADDRESS);
    }
    /**
     * Local IP address or hostname to use when making connections (binding
     * sockets). When not specified, uses default local address(es).
     */
    public void setHttpBindAddress(String address) {
        kp.put(HTTP_BIND_ADDRESS, address);
    }
    public static final String HTTP_BIND_ADDRESS = "httpBindAddress";
    
    public String getHttpProxyHost() {
        return (String) kp.get("httpProxyHost");
    }
    /**
     * Proxy host IP (set only if needed).
     */
    public void setHttpProxyHost(String host) {
        kp.put("httpProxyHost",host);
    }

    public Integer getHttpProxyPort() {
        return (Integer) kp.get("httpProxyPort");
    }
    /**
     * Proxy port (set only if needed).
     */
    public void setHttpProxyPort(int port) {
        kp.put("httpProxyPort",port);
    }

    public String getHttpProxyUser() {
        return (String) kp.get("httpProxyUser");
    }
    /**
     * Proxy user (set only if needed).
     */
    public void setHttpProxyUser(String user) {
        kp.put("httpProxyUser",user);
    }

    public String getHttpProxyPassword() {
        return (String) kp.get("httpProxyPassword");
    }
    /**
     * Proxy password (set only if needed).
     */
    public void setHttpProxyPassword(String password) {
        kp.put("httpProxyPassword",password);
    }

    {
        setMaxFetchKBSec(0); // no limit
    }
    public int getMaxFetchKBSec() {
        return (Integer) kp.get("maxFetchKBSec");
    }
    /**
     * The maximum KB/sec to use when fetching data from a server. The default
     * of 0 means no maximum.
     */
    public void setMaxFetchKBSec(int rate) {
        kp.put("maxFetchKBSec",rate);
    }
    
    {
        setTimeoutSeconds(20*60); // 20 minutes
    }
    public int getTimeoutSeconds() {
        return (Integer) kp.get("timeoutSeconds");
    }
    /**
     * If the fetch is not completed in this number of seconds, give up (and
     * retry later).
     */
    public void setTimeoutSeconds(int timeout) {
        kp.put("timeoutSeconds",timeout);
    }

    {
        setSoTimeoutMs(20*1000); // 20 seconds
    }
    public int getSoTimeoutMs() {
        return (Integer) kp.get("soTimeoutMs");
    }
    /**
     * If the socket is unresponsive for this number of milliseconds, give up.
     * Set to zero for no timeout (Not. recommended. Could hang a thread on an
     * unresponsive server). This timeout is used timing out socket opens and
     * for timing out each socket read. Make sure this value is &lt;
     * {@link #TIMEOUT_SECONDS} for optimal configuration: ensures at least one
     * retry read.
     */
    public void setSoTimeoutMs(int timeout) {
        kp.put("soTimeoutMs",timeout);
    }

    {
        setMaxLengthBytes(0L); // no limit
    }
    public long getMaxLengthBytes() {
        return (Long) kp.get("maxLengthBytes");
    }
    /**
     * Maximum length in bytes to fetch. Fetch is truncated at this length. A
     * value of 0 means no limit.
     */
    public void setMaxLengthBytes(long timeout) {
        kp.put("maxLengthBytes",timeout);
    }

    /**
     * Send 'Range' header when a limit ({@link #MAX_LENGTH_BYTES}) on
     * document size.
     * <p>
     * Be polite to the HTTP servers and send the 'Range' header, stating that
     * you are only interested in the first n bytes. Only pertinent if
     * {@link #MAX_LENGTH_BYTES} &gt; 0. Sending the 'Range' header results in a
     * '206 Partial Content' status response, which is better than just cutting
     * the response mid-download. On rare occasion, sending 'Range' will
     * generate '416 Request Range Not Satisfiable' response.
     */
    {
        setSendRange(false);
    }
    public boolean getSendRange() {
        return (Boolean) kp.get("sendRange");
    }
    public void setSendRange(boolean sendRange) {
        kp.put("sendRange",sendRange);
    }

    {
        // XXX default to false?
        setSendIfModifiedSince(true);
    }
    public boolean getSendIfModifiedSince() {
        return (Boolean) kp.get("sendIfModifiedSince");
    }
    /**
     * Send 'If-Modified-Since' header, if previous 'Last-Modified' fetch
     * history information is available in URI history.
     */
    public void setSendIfModifiedSince(boolean sendIfModifiedSince) {
        kp.put("sendIfModifiedSince",sendIfModifiedSince);
    }

    {
        // XXX default to false?
        setSendIfNoneMatch(true);
    }
    public boolean getSendIfNoneMatch() {
        return (Boolean) kp.get("sendIfNoneMatch");
    }
    /**
     * Send 'If-None-Match' header, if previous 'Etag' fetch history information
     * is available in URI history.
     */
    public void setSendIfNoneMatch(boolean sendIfNoneMatch) {
        kp.put("sendIfNoneMatch",sendIfNoneMatch);
    }

    {
        setShouldFetchBodyRule(new AcceptDecideRule());
    }
    public DecideRule getShouldFetchBodyRule() {
        return (DecideRule) kp.get("shouldFetchBodyRule");
    }
    /**
     * DecideRules applied after receipt of HTTP response headers but before we
     * start to download the body. If any filter returns FALSE, the fetch is
     * aborted. Prerequisites such as robots.txt by-pass filtering (i.e. they
     * cannot be midfetch aborted.
     */
    public void setShouldFetchBodyRule(DecideRule rule) {
        kp.put("shouldFetchBodyRule", rule);
    }
    
    protected TrustLevel sslTrustLevel = TrustLevel.OPEN;
    public TrustLevel getSslTrustLevel() {
        return sslTrustLevel;
    }
    /**
     * SSL certificate trust level. Range is from the default 'open' (trust all
     * certs including expired, selfsigned, and those for which we do not have a
     * CA) through 'loose' (trust all valid certificates including selfsigned),
     * 'normal' (all valid certificates not including selfsigned) to 'strict'
     * (Cert is valid and DN must match servername).
     */
    public synchronized void setSslTrustLevel(TrustLevel trustLevel) {
        this.sslTrustLevel = trustLevel;
    }

    protected transient SSLContext sslContext;
    protected synchronized SSLContext sslContext() {
        if (sslContext == null) {
            try {
                TrustManager trustManager = new ConfigurableX509TrustManager(
                        getSslTrustLevel());
                sslContext = SSLContext.getInstance("SSL");
                sslContext.init(null, new TrustManager[] {trustManager}, null);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed configure of ssl context "
                        + e.getMessage(), e);
            }
        }

        return sslContext;
    }


    /**
     * Can this processor fetch the given CrawlURI. May set a fetch status
     * if this processor would usually handle the CrawlURI, but cannot in
     * this instance.
     * 
     * @param curi
     * @return True if processor can fetch.
     */
    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        String scheme = curi.getUURI().getScheme();
        if (!(scheme.equals(HTTP_SCHEME) || scheme.equals(HTTPS_SCHEME))) {
            // handles only plain http and https
            return false;
        }

        CrawlHost host = getServerCache().getHostFor(curi.getUURI());
        if (host.getIP() == null && host.hasBeenLookedUp()) {
            curi.setFetchStatus(S_DOMAIN_PREREQUISITE_FAILURE);
            return false;
        }

        return true;
    }
    
    protected boolean checkMidfetchAbort(CrawlURI curi) {
        if (curi.isPrerequisite()) {
            return false;
        }
        DecideResult r = getShouldFetchBodyRule().decisionFor(curi);
        if (r != DecideResult.REJECT) {
            return false;
        }
        return true;
    }
    
    @Override
    protected void innerProcess(final CrawlURI curi) throws InterruptedException {
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
        
        boolean addedCredentials = false;
        
        // set hardMax on bytes (if set by operator)
        long hardMax = getMaxLengthBytes();
        // set overall timeout (if set by operator)
        long timeoutMs = 1000 * getTimeoutSeconds();
        // Get max fetch rate (bytes/ms). It comes in in KB/sec
        long maxRateKBps = getMaxFetchKBSec();
        rec.getRecordedInput().setLimits(hardMax, timeoutMs, maxRateKBps);



        if (rec.getRecordedInput().isOpen()) {
            logger.severe(curi.toString() + " RIS still open. Should have"
                    + " been closed by method release: "
                    + Thread.currentThread().getName());
            try {
                rec.getRecordedInput().close();
            } catch (IOException e) {
                logger.log(Level.SEVERE, "second-chance RIS close failed", e);
            }
        }
    }
    
    /**
     * Promote successful credential to the server.
     * 
     * @param curi
     *            CrawlURI whose credentials we are to promote.
     */
    protected void promoteCredentials(final CrawlURI curi) {
        Set<Credential> credentials = curi.getCredentials();
        for (Iterator<Credential> i = credentials.iterator(); i.hasNext();) {
            Credential c = i.next();
            i.remove();
            // The server to attach to may not be the server that hosts
            // this passed curi. It might be of another subdomain.
            // The avatar needs to be added to the server that is dependent
            // on this precondition. Find it by name. Get the name from
            // the credential this avatar represents.
            String cd = c.getDomain();
            if (cd != null) {
                CrawlServer cs = serverCache.getServerFor(cd);
                if (cs != null) {
                    cs.addCredential(c);
                    cs.setHttpAuthChallenges(curi.getHttpAuthChallenges());
                }
            }
        }
    }

    /**
     * @param curi
     *            CrawlURI that got a 401.
     * @param type
     *            Class of credential to get from curi.
     * @return Set of credentials attached to this curi.
     */
    protected Set<Credential> getCredentials(CrawlURI curi, Class<?> type) {
        Set<Credential> result = null;

        if (curi.hasCredentials()) {
            for (Credential c : curi.getCredentials()) {
                if (type.isInstance(c)) {
                    if (result == null) {
                        result = new HashSet<Credential>();
                    }
                    result.add(c);
                }
            }
        }
        return result;
    }

    /**
     * Get a value either from inside the CrawlURI instance, or from
     * settings (module attributes).
     * 
     * @param curi
     *            CrawlURI to consult
     * @param key
     *            key to lookup
     * @return value from either CrawlURI (preferred) or settings
     */
    protected Object getAttributeEither(CrawlURI curi, String key) {
        Object r = curi.getData().get(key);
        if (r != null) {
            return r;
        }
        return kp.get(key);
    }

    /**
     * Update CrawlURI internal sizes based on current transaction (and
     * in the case of 304s, history) 
     * 
     * @param curi CrawlURI
     * @param rec HttpRecorder
     */
    protected void setSizes(CrawlURI curi, Recorder rec) {
        // set reporting size
        curi.setContentSize(rec.getRecordedInput().getSize());
        // special handling for 304-not modified
        if (curi.getFetchStatus() == 304
                && curi.getFetchHistory() != null) {
            Map<String, Object>[] history = curi.getFetchHistory();
            if (history[0] != null && history[0].containsKey(A_REFERENCE_LENGTH)) {
                long referenceLength = (Long) history[0].get(A_REFERENCE_LENGTH);
                // carry-forward previous 'reference-length' for future
                curi.getData().put(A_REFERENCE_LENGTH, referenceLength);
                // increase content-size to virtual-size for reporting
                curi.setContentSize(rec.getRecordedInput().getSize()
                        + referenceLength);
            }
        }
    }

    /**
     * Cleanup after a failed method execute.
     * 
     * @param curi
     *            CrawlURI we failed on.
     * @param request
     *            Method we failed on.
     * @param exception
     *            Exception we failed with.
     */
    protected void failedExecuteCleanup(final CrawlURI curi, final Exception exception) {
        cleanup(curi, exception, "executeMethod", S_CONNECT_FAILED);
    }
    
    /**
     * Cleanup after a failed method execute.
     * 
     * @param curi
     *            CrawlURI we failed on.
     * @param exception
     *            Exception we failed with.
     * @param message
     *            Message to log with failure. FIXME: Seems ignored
     * @param status
     *            Status to set on the fetch.
     */
    protected void cleanup(final CrawlURI curi, final Exception exception,
            final String message, final int status) {
        if (logger.isLoggable(Level.FINER)) {
            logger.log(Level.FINER, message + ": " + exception, exception);
        } else if (logger.isLoggable(Level.FINE)) {
            logger.fine(message + ": " + exception);
        }
        
        curi.getNonFatalFailures().add(exception);
        curi.setFetchStatus(status);
        curi.getRecorder().close();
    }
    
    public void start() {
        if(isRunning()) {
            return; 
        }
        
        super.start();
        
        if (getCookieStore() != null) {     
            getCookieStore().start();
        }

        // setSSLFactory();
    }
    
    public void stop() {
        if (!isRunning()) {
            return;
        }
        super.stop();
        // At the end save cookies to the file specified in the order file.
        if (cookieStore != null) {
            cookieStore.saveCookies();
            cookieStore.stop();
        }
        disposeHttpClient(); // XXX happens at finish; move to teardown?
    }

    protected void disposeHttpClient() {
        sslContext = null;
    }
    
    protected static String getServerKey(CrawlURI uri) {
        try {
            return CrawlServer.getServerKey(uri.getUURI());
        } catch (URIException e) {
            logger.severe(e.getMessage() + ": " + uri);
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        for (String urlStr: new String[] {"http://archive.org/", "http://archive.org/404-me-homie"}) {
            System.out.println("\n----- urlStr -----\n");
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            InputStream in = conn.getInputStream();
            for (int b = in.read(); b != -1; b = in.read()) {
                System.out.write(b);
            }
        }
    }
}
