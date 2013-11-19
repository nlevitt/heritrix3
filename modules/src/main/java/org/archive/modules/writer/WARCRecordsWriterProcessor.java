package org.archive.modules.writer;

import java.util.LinkedHashMap;

import org.archive.io.warc.WARCRecordInfo;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;

public class WARCRecordsWriterProcessor extends Processor {
    
    public static final String A_WARC_RECORDS = "warc-records";
    /**
     * Response, resource or revisit record are principal records; request and
     * metadata records are concurrent-to principal record.
     */
    public static final String A_PRINCIPAL_RECORD = "principal-record";
    public static final String A_REQUEST_RECORD = "request-record";
    public static final String A_METADATA_RECORD = "metadata-record";

    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        return curi.getData().get(A_WARC_RECORDS) != null;
    }

    @Override
    protected void innerProcess(CrawlURI uri) throws InterruptedException {
    }

    public static void addRecord(CrawlURI curi, String whichRecord,
            WARCRecordInfo record) {
        @SuppressWarnings("unchecked")
        LinkedHashMap<String,WARCRecordInfo> records = (LinkedHashMap<String, WARCRecordInfo>) curi.getData().get(A_WARC_RECORDS);
        if (records == null) {
            records = new LinkedHashMap<String, WARCRecordInfo>();
            curi.getData().put(A_WARC_RECORDS, records);
        }
        records.put(whichRecord, record);
    }

}
