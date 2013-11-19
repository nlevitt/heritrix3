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

import static org.archive.format.warc.WARCConstants.HEADER_KEY_ETAG;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_LAST_MODIFIED;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_PROFILE;
import static org.archive.format.warc.WARCConstants.HTTP_RESPONSE_MIMETYPE;
import static org.archive.format.warc.WARCConstants.PROFILE_REVISIT_NOT_MODIFIED;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_ETAG_HEADER;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_LAST_MODIFIED_HEADER;
import static org.archive.modules.writer.WARCRecordsWriterProcessor.A_PRINCIPAL_RECORD;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.uid.RecordIDGenerator;
import org.archive.uid.UUIDGenerator;
import org.archive.util.ArchiveUtils;
import org.archive.util.anvl.ANVLRecord;

public class WARCRevisitForNotModifiedProcessor extends Processor {
    
//    private static final Logger logger = 
//            Logger.getLogger(WARCRevisitForNotModifiedProcessor.class.getName());

    protected RecordIDGenerator generator = new UUIDGenerator();

    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        return curi.getFetchStatus() == HttpStatus.SC_NOT_MODIFIED;
    }

    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        WARCRecordInfo recordInfo = new WARCRecordInfo();
        recordInfo.setType(WARCRecordType.revisit);
        recordInfo.setUrl(curi.toString());
        String timestamp = ArchiveUtils.getLog14Date(curi.getFetchBeginTime());
        recordInfo.setCreate14DigitDate(timestamp);
        recordInfo.setRecordId(generator.getRecordID());

        // XXX WARCWriterProcessor doesn't write response?
        recordInfo.setMimetype(HTTP_RESPONSE_MIMETYPE);
        recordInfo.setContentLength(curi.getRecorder().getRecordedInput().getSize());
        recordInfo.setEnforceLength(true);
        
        ANVLRecord namedFields = new ANVLRecord();
        namedFields.addLabelValue(
                HEADER_KEY_PROFILE, PROFILE_REVISIT_NOT_MODIFIED);
        // save just enough context to understand basis of not-modified
        recordInfo.setExtraHeaders(namedFields);
        
        if(curi.isHttpTransaction()) {
            HttpMethod method = curi.getHttpMethod();
            
            Header header = method.getResponseHeader(A_ETAG_HEADER);
            if (header != null) {
                namedFields.addLabelValue(HEADER_KEY_ETAG, header.getValue());
            }

            header = method.getResponseHeader(A_LAST_MODIFIED_HEADER);
            if (header != null) {
                namedFields.addLabelValue(HEADER_KEY_LAST_MODIFIED, header.getValue());
            }
        }
        
        curi.getAnnotations().add("warcRevisit:notModified");

        WARCRecordsWriterProcessor.addRecord(curi, A_PRINCIPAL_RECORD, recordInfo);
    }

}
