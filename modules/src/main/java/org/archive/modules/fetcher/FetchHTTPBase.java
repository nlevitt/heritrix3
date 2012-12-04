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

import java.util.List;

import org.archive.httpclient.ConfigurableX509TrustManager.TrustLevel;
import org.archive.modules.Processor;
import org.archive.modules.credential.CredentialStore;
import org.archive.modules.deciderules.DecideRule;

public abstract class FetchHTTPBase extends Processor {
    public abstract void setAcceptHeaders(List<String> headers);
    public abstract void setIgnoreCookies(boolean b);
    public abstract CredentialStore getCredentialStore();
    public abstract void setAcceptCompression(boolean b);
    public abstract void setHttpBindAddress(String addr);
    public abstract void setHttpProxyHost(String string);
    public abstract void setHttpProxyPort(int i);
    public abstract void setHttpProxyUser(String string);
    public abstract void setHttpProxyPassword(String string);
    public abstract void setMaxFetchKBSec(int i);
    public abstract void setMaxLengthBytes(long n);
    public abstract void setSendRange(boolean b);
    public abstract void setSendIfModifiedSince(boolean b);
    public abstract void setSendIfNoneMatch(boolean b);
    public abstract void setShouldFetchBodyRule(DecideRule rule);
    public abstract void setTimeoutSeconds(int i);
    public abstract void setSoTimeoutMs(int i);
    public abstract void setSslTrustLevel(TrustLevel normal);
    public abstract void setUseHTTP11(boolean b);
    public abstract void setSendConnectionClose(boolean b);
}
