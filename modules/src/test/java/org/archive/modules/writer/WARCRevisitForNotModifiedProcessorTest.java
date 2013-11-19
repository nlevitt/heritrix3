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
import static org.archive.format.warc.WARCConstants.PROFILE_REVISIT_NOT_MODIFIED;

import java.util.LinkedHashMap;
import java.util.Map;

import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessorTestBase;
import org.archive.modules.extractor.ContentExtractorTestBase;
import org.archive.net.UURIFactory;
import org.archive.util.Recorder;

public class WARCRevisitForNotModifiedProcessorTest extends ProcessorTestBase {
    
    @Override
    protected WARCRevisitForNotModifiedProcessor makeModule() throws Exception {
        return (WARCRevisitForNotModifiedProcessor) super.makeModule();
    }
    
    public void testNot304() throws Exception {
        CrawlURI curi = new CrawlURI(UURIFactory.getInstance("http://example.org/"));
        curi.setFetchStatus(200);
        
        WARCRevisitForNotModifiedProcessor p = makeModule();

        assertNull(curi.getData().get(WARCRecordsWriterProcessor.A_WARC_RECORDS));
        
        p.process(curi);
        
        assertEquals(null, curi.getData().get(WARCRecordsWriterProcessor.A_WARC_RECORDS));
    }
    
    public void test304() throws Exception {
        CrawlURI curi = new CrawlURI(UURIFactory.getInstance("http://example.org/"));
        curi.setFetchStatus(304);
        curi.setFetchBeginTime(1380000000000l); // "2013-09-24T05:20:00Z"
        
        Recorder rec = ContentExtractorTestBase.createRecorder("HTTP/1.1 304 Not Modified\r\nETag: test-etag\r\n\r\n", "UTF-8");
        curi.setRecorder(rec);;
        
        WARCRevisitForNotModifiedProcessor p = makeModule();

        assertEquals(null, curi.getData().get(WARCRecordsWriterProcessor.A_WARC_RECORDS));
        
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
        
        assertNotNull(record.getExtraHeaders());
        Map<String, String> extraHeaders = record.getExtraHeaders().asMap(); 
        assertEquals(PROFILE_REVISIT_NOT_MODIFIED, extraHeaders.get(HEADER_KEY_PROFILE));
        
        assertTrue(curi.getAnnotations().contains("warcRevisit:notModified"));
    }
}
