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
}
