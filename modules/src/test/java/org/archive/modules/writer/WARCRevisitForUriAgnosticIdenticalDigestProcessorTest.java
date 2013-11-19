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

package org.archive.modules.writer;

import static org.archive.format.warc.WARCConstants.HEADER_KEY_PROFILE;
import static org.archive.format.warc.WARCConstants.PROFILE_REVISIT_IDENTICAL_DIGEST;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_CONTENT_DIGEST_COUNT;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_ORIGINAL_DATE;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_ORIGINAL_URL;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_WARC_RECORD_ID;

import java.util.LinkedHashMap;
import java.util.Map;

import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessorTestBase;
import org.archive.modules.extractor.ContentExtractorTestBase;
import org.archive.net.UURIFactory;
import org.archive.util.ArchiveUtils;
import org.archive.util.Recorder;

public class WARCRevisitForUriAgnosticIdenticalDigestProcessorTest extends ProcessorTestBase {

    @Override
    protected WARCRevisitForUriAgnosticIdenticalDigestProcessor makeModule() throws Exception {
        return (WARCRevisitForUriAgnosticIdenticalDigestProcessor) super.makeModule();
    }

    public void testNotDup() throws Exception {
        CrawlURI curi = new CrawlURI(UURIFactory.getInstance("http://example.org/"));
        curi.setFetchStatus(200);

        assertNull(curi.getData().get(WARCRecordsWriterProcessor.A_WARC_RECORDS));

        WARCRevisitForUriAgnosticIdenticalDigestProcessor p = makeModule();
        p.process(curi);

        assertEquals(null, curi.getData().get(WARCRecordsWriterProcessor.A_WARC_RECORDS));
    }

    public void testDup() throws Exception {
        CrawlURI curi = new CrawlURI(UURIFactory.getInstance("http://example.org/1"));
        curi.setFetchBeginTime(1380000000000l); // "2013-09-24T05:20:00Z"
        String httpResponse = "HTTP/1.1 200 OK\r\n"
                + "Content-Type: text/plain\r\n"
                + "Content-Length: 11\r\n"
                + "\r\n"
                + "0123456789\n";
        Recorder rec = ContentExtractorTestBase.createRecorder(httpResponse, "UTF-8", httpResponse.length() - 11);
        curi.setRecorder(rec);
        assertEquals(httpResponse.length() - 11, curi.getRecorder().getRecordedInput().getContentBegin());

        curi.getContentDigestHistory().put(A_ORIGINAL_URL, "http://example.org/");
        curi.getContentDigestHistory().put(A_WARC_RECORD_ID, "urn:uuid:00000000-0000-0000-0000-00000000000000");
        curi.getContentDigestHistory().put(A_ORIGINAL_DATE, ArchiveUtils.getLog14Date(1370000000000l));
        curi.getContentDigestHistory().put(A_CONTENT_DIGEST_COUNT, 1);

        WARCRevisitForUriAgnosticIdenticalDigestProcessor p = makeModule();
        p.process(curi);

        assertTrue(curi.getData().get(WARCRecordsWriterProcessor.A_WARC_RECORDS) instanceof LinkedHashMap);

        @SuppressWarnings("unchecked")
        Map<String,WARCRecordInfo> recordsMap = (Map<String, WARCRecordInfo>) curi.getData().get(WARCRecordsWriterProcessor.A_WARC_RECORDS);
        assertEquals(1, recordsMap.size());

        assertTrue(recordsMap.get(WARCRecordsWriterProcessor.A_PRINCIPAL_RECORD) instanceof WARCRecordInfo);
        WARCRecordInfo record = recordsMap.get(WARCRecordsWriterProcessor.A_PRINCIPAL_RECORD);

        assertEquals(WARCRecordType.revisit, record.getType());
        assertEquals(curi.toString(), record.getUrl());
        assertNotNull(record.getRecordId());
        assertEquals("2013-09-24T05:20:00Z", record.getCreate14DigitDate());
        assertEquals("application/http; msgtype=response", record.getMimetype());
        assertEquals(httpResponse.length() - 11, record.getContentLength());

        assertNotNull(record.getExtraHeaders());
        Map<String, String> extraHeaders = record.getExtraHeaders().asMap(); 
        assertEquals(PROFILE_REVISIT_IDENTICAL_DIGEST, extraHeaders.get(HEADER_KEY_PROFILE));
        assertEquals("<urn:uuid:00000000-0000-0000-0000-00000000000000>", extraHeaders.get("WARC-Refers-To"));
        assertEquals("http://example.org/", extraHeaders.get("WARC-Refers-To-Target-URI"));
        assertEquals(ArchiveUtils.getLog14Date(1370000000000l), extraHeaders.get("WARC-Refers-To-Date"));
        assertEquals("length", extraHeaders.get("WARC-Truncated"));

        assertTrue(curi.getAnnotations().contains("warcRevisit:uriAgnosticDigest"));
    }
}
