package org.apache.orc.tools.convert;

import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertEquals;

public class TestJsonReader {
    @Test
    public void testCustomTimestampFormat() throws Exception {
        String tsFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS";

        String s = "{\"a\":\"2018-03-21 12:23:34.123456\"}\n" +
                "{\"a\":\"2018-02-03 18:04:51.456789\"}\n";
        StringReader input = new StringReader(s);
        TypeDescription schema = TypeDescription.fromString(
                "struct<a:timestamp>");
        JsonReader reader = new JsonReader(input, null, 1, schema, tsFormat);
        VectorizedRowBatch batch = schema.createRowBatch(2);
        assertEquals(true, reader.nextBatch(batch));
        assertEquals(2, batch.size);
        TimestampColumnVector cv = (TimestampColumnVector) batch.cols[0];
        assertEquals("2018-03-21 12:23:34.123456", cv.asScratchTimestamp(0).toString());
        assertEquals("2018-02-03 18:04:51.456789", cv.asScratchTimestamp(1).toString());
    }

    @Test
    public void testTimestampOffByOne() throws Exception {
        String tsFormat = "yyyy-MM-dd HH:mm:ss.SSSS";

        String s = "{\"a\": \"1970-01-01 00:00:00.0001\"}\n" +
                "{\"a\": \"1970-01-01 00:00:00.0000\"}\n" +
                "{\"a\": \"1969-12-31 23:59:59.9999\"}\n" +
                "{\"a\": \"1969-12-31 23:59:59.0001\"}\n" +
                "{\"a\": \"1969-12-31 23:59:59.0000\"}\n" +
                "{\"a\": \"1969-12-31 23:59:58.9999\"}";
        StringReader input = new StringReader(s);
        TypeDescription schema = TypeDescription.fromString(
                "struct<a:timestamp>");
        JsonReader reader = new JsonReader(input, null, 1, schema, tsFormat);
        VectorizedRowBatch batch = schema.createRowBatch(6);
        assertEquals(true, reader.nextBatch(batch));
        assertEquals(6, batch.size);
        TimestampColumnVector cv = (TimestampColumnVector) batch.cols[0];
        assertEquals("1970-01-01 00:00:00.0001", cv.asScratchTimestamp(0).toString());
        assertEquals("1970-01-01 00:00:00.0", cv.asScratchTimestamp(1).toString());
        assertEquals("1969-12-31 23:59:59.9999", cv.asScratchTimestamp(2).toString());
        assertEquals("1969-12-31 23:59:59.0001", cv.asScratchTimestamp(3).toString());
        assertEquals("1969-12-31 23:59:59.0", cv.asScratchTimestamp(4).toString());
        assertEquals("1969-12-31 23:59:58.9999", cv.asScratchTimestamp(5).toString());
    }

}
