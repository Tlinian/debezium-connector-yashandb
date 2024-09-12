package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamDml;

public class YStreamDmlRecord extends YStreamRecord {
    private YstreamDml ystreamDml;

    public YStreamDmlRecord(YstreamDml ystreamDml, TableMetadata tableMetadata) {
        super(ystreamDml, tableMetadata);
        this.ystreamDml = ystreamDml;
    }

    public YstreamDml getYstreamDml() {
        return ystreamDml;
    }
}
