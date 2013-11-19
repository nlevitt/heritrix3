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
import static org.archive.format.warc.WARCConstants.HEADER_KEY_REFERS_TO;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_REFERS_TO_DATE;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_REFERS_TO_TARGET_URI;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_TRUNCATED;
import static org.archive.format.warc.WARCConstants.HTTP_RESPONSE_MIMETYPE;
import static org.archive.format.warc.WARCConstants.NAMED_FIELD_TRUNCATED_VALUE_LENGTH;
import static org.archive.format.warc.WARCConstants.PROFILE_REVISIT_IDENTICAL_DIGEST;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_ORIGINAL_DATE;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_ORIGINAL_URL;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_WARC_RECORD_ID;
import static org.archive.modules.writer.WARCRecordsWriterProcessor.A_PRINCIPAL_RECORD;

import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.uid.RecordIDGenerator;
import org.archive.uid.UUIDGenerator;
import org.archive.util.ArchiveUtils;
import org.archive.util.anvl.ANVLRecord;

public class WARCRevisitForUriAgnosticIdenticalDigestProcessor extends Processor {

    protected RecordIDGenerator generator = new UUIDGenerator();

    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        return curi.hasContentDigestHistory()
                && curi.getContentDigestHistory().get(A_ORIGINAL_URL) != null;
    }

    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        WARCRecordInfo recordInfo = new WARCRecordInfo();
        recordInfo.setType(WARCRecordType.revisit);
        recordInfo.setUrl(curi.toString());
        String timestamp = ArchiveUtils.getLog14Date(curi.getFetchBeginTime());
        recordInfo.setCreate14DigitDate(timestamp);
        recordInfo.setRecordId(generator.getRecordID());

        recordInfo.setMimetype(HTTP_RESPONSE_MIMETYPE);
        recordInfo.setContentLength(curi.getRecorder().getRecordedInput().getContentBegin());
        recordInfo.setEnforceLength(true);

        ANVLRecord headers = new ANVLRecord();
        headers.addLabelValue(
                HEADER_KEY_PROFILE, PROFILE_REVISIT_IDENTICAL_DIGEST);

        headers.addLabelValue(
                HEADER_KEY_TRUNCATED, NAMED_FIELD_TRUNCATED_VALUE_LENGTH);

        /*
         * ISO 28500 WARC ISO standard draft says: "The WARC-Refers-To field may
         * also be used to associate a record of type 'revisit' or 'conversion'
         * with the preceding record which helped determine the present record
         * content."
         */
        headers.addLabelValue(HEADER_KEY_REFERS_TO, 
                "<" + curi.getContentDigestHistory().get(A_WARC_RECORD_ID) + ">");
        headers.addLabelValue(HEADER_KEY_REFERS_TO_TARGET_URI, 
                curi.getContentDigestHistory().get(A_ORIGINAL_URL).toString());
        headers.addLabelValue(HEADER_KEY_REFERS_TO_DATE, 
                curi.getContentDigestHistory().get(A_ORIGINAL_DATE).toString());

        recordInfo.setExtraHeaders(headers);

        curi.getAnnotations().add("warcRevisit:uriAgnosticDigest");

        WARCRecordsWriterProcessor.addRecord(curi, A_PRINCIPAL_RECORD, recordInfo);
    }

}
