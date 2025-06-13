/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.io.File;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class TestOrcGeospatial implements TestConf {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
          "target" + File.separator + "test" + File.separator + "tmp"));
  FileSystem fs;
  Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcGeospatial." +
            testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testGeometryWriterWithNulls() throws Exception {
    // Create a geometry schema and ORC file writer
    TypeDescription schema = TypeDescription.createGeometry();
    Writer writer = OrcFile.createWriter(testFilePath,
            OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
                    .bufferSize(10000));
    GeometryFactory geometryFactory = new GeometryFactory();
    WKBWriter wkbWriter = new WKBWriter();
    WKBReader wkbReader = new WKBReader();

    // Add data
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector geos = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) {
        byte[] bytes = wkbWriter.write(geometryFactory.createPoint(new Coordinate(i, i)));
        geos.setVal(batch.size++, bytes);
      } else {
        geos.noNulls = false;
        geos.isNull[batch.size++] = true;
      }
    }
    writer.addRowBatch(batch);
    writer.close();

    // Verify reader schema
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals("geometry(OGC:CRS84)", reader.getSchema().toString());
    assertEquals(100, reader.getNumberOfRows());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    geos = (BytesColumnVector) batch.cols[0];

    // Verify statistics
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(1, stats.length);
    assertEquals(50, stats[0].getNumberOfValues());
    assertTrue(stats[0].hasNull());
    assertInstanceOf(GeospatialColumnStatistics.class, stats[0]);
    assertTrue(((GeospatialColumnStatistics) stats[0]).getBoundingBox().isXYValid());
    assertFalse(((GeospatialColumnStatistics) stats[0]).getBoundingBox().isZValid());
    assertFalse(((GeospatialColumnStatistics) stats[0]).getBoundingBox().isMValid());
    assertEquals("BoundingBox{xMin=0.0, xMax=98.0, yMin=0.0, yMax=98.0, zMin=NaN, zMax=NaN, mMin=NaN, mMax=NaN}", ((GeospatialColumnStatistics) stats[0]).getBoundingBox().toString());
    assertEquals("GeospatialTypes{types=[Point (XY)]}", ((GeospatialColumnStatistics) stats[0]).getGeospatialTypes().toString());

    // Verify data
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        if (idx % 2 == 0) {
          Geometry geom = wkbReader.read(Arrays.copyOfRange(geos.vector[r], geos.start[r], geos.start[r] + geos.length[r]));
          assertEquals("Point", geom.getGeometryType());
          assertEquals(geom, geometryFactory.createPoint(new Coordinate(idx, idx)));
        } else {
          assertTrue(geos.isNull[r]);
        }
        idx += 1;
      }
    }
    rows.close();
  }

  @Test
  public void testGeographyWriterWithNulls() throws Exception {
    // Create geography schema and ORC file writer
    TypeDescription schema = TypeDescription.createGeography();
    Writer writer = OrcFile.createWriter(testFilePath,
            OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
                    .bufferSize(10000));
    GeometryFactory geometryFactory = new GeometryFactory();
    WKBWriter wkbWriter = new WKBWriter();
    WKBReader wkbReader = new WKBReader();

    // Add data
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector geos = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) {
        byte[] bytes = wkbWriter.write(geometryFactory.createPoint(new Coordinate(i, i)));
        geos.setVal(batch.size++, bytes);
      } else {
        geos.noNulls = false;
        geos.isNull[batch.size++] = true;
      }
    }
    writer.addRowBatch(batch);
    writer.close();

    // Verify reader schema
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals("geography(OGC:CRS84,SPHERICAL)", reader.getSchema().toString());
    assertEquals(100, reader.getNumberOfRows());

    // Verify statistics, make sure there are no bounding box and geospatial types
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(1, stats.length);
    assertEquals(50, stats[0].getNumberOfValues());
    assertTrue(stats[0].hasNull());
    assertInstanceOf(GeospatialColumnStatistics.class, stats[0]);
    assertNull(((GeospatialColumnStatistics) stats[0]).getBoundingBox());
    assertNull(((GeospatialColumnStatistics) stats[0]).getGeospatialTypes());

    // Verify Data
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    geos = (BytesColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        if (idx % 2 == 0) {
          Geometry geom = wkbReader.read(Arrays.copyOfRange(geos.vector[r], geos.start[r], geos.start[r] + geos.length[r]));
          assertEquals("Point", geom.getGeometryType());
          assertEquals(geom, geometryFactory.createPoint(new Coordinate(idx, idx)));
        } else {
          assertTrue(geos.isNull[r]);
        }
        idx += 1;
      }
    }
    rows.close();
  }
}
