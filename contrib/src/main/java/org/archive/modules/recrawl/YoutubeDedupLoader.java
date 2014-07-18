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
package org.archive.modules.recrawl;

import java.nio.charset.Charset;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.util.TextUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class YoutubeDedupLoader extends Processor {
    
    private static final Logger logger = 
            Logger.getLogger(YoutubeDedupLoader.class.getName());

    protected AbstractContentDigestHistory contentDigestHistory;
    @Autowired
    public void setContentDigestHistory(
            AbstractContentDigestHistory contentDigestHistory) {
        this.contentDigestHistory = contentDigestHistory;
    }

    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        try {
            return curi.getFetchStatus() == 200
                    && curi.getContentLength() > 10000
                    && TextUtils.matches("(?i).*\\.(youtube|googlevideo)\\.com$", curi.getUURI().getHost())
                    && "/videoplayback".equals(curi.getUURI().getPath());
        } catch (URIException e) {
            logger.log(Level.WARNING, "problem parsing url " + curi.getUURI().toString(), e);
            return false;
        }
    }

    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        String itag = null;
        try {
            List<NameValuePair> queryParams = URLEncodedUtils.parse(curi.getUURI().getQuery(), Charset.forName("UTF-8"));
            for (NameValuePair nv: queryParams) {
                if ("itag".equals(nv.getName())) {
                    itag = nv.getValue();
                    break;
                }
            }
        } catch (URIException e) {
            logger.log(Level.WARNING, "problem parsing url " + curi.getUURI().toString(), e);
            return;
        }
        
        if (itag == null) {
            logger.warning("itag not found in url " + curi.getUURI());
            return;
        }
    }
}
