/**
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

package org.apache.flume.sink.hdfs;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests that exercise HDFSEventSink on an actual instance of HDFS.
 * TODO: figure out how to unit-test Kerberos-secured HDFS.
 */
public class TestHDFSEventSinkOnMiniCluster {

  private static final Logger logger =
      LoggerFactory.getLogger(TestHDFSEventSinkOnMiniCluster.class);

  private static final boolean KEEP_DATA = false;
  private static final String DFS_DIR = "target/test/dfs";
  private static final Configuration CONF = new Configuration();
  private static final String TEST_BUILD_DATA_KEY = "test.build.data";

  private static MiniDFSCluster cluster = null;
  private static String oldTestBuildDataProp = null;

  @BeforeClass
  public static void setup() throws IOException {
    // set up data dir for HDFS
    File dfsDir = new File(DFS_DIR);
    if (!dfsDir.isDirectory()) {
      dfsDir.mkdirs();
    }
    // save off system prop to restore later
    oldTestBuildDataProp = System.getProperty(TEST_BUILD_DATA_KEY);
    System.setProperty(TEST_BUILD_DATA_KEY, DFS_DIR);

    cluster = new MiniDFSCluster(CONF, 1, true, null);
    cluster.waitActive();
  }

  private static String getNameNodeURL(MiniDFSCluster cluster) {
    int nnPort = cluster.getNameNode().getNameNodeAddress().getPort();
    return "maprfs:///";
  }

  /**
   * This is a very basic test that writes one event to HDFS and reads it back.
   */
  @Test
  public void simpleHDFSTest() throws EventDeliveryException, IOException {
    String outputDir = "/flume/simpleHDFSTest";
    Path outputDirPath = new Path(outputDir);

    logger.info("Running test with output dir: {}", outputDir);

    FileSystem fs = cluster.getFileSystem();
    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    String nnURL = getNameNodeURL(cluster);
    logger.info("Namenode address: {}", nnURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nnURL + outputDir);
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.DataStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    String EVENT_BODY = "yarg!";
    channel.getTransaction().begin();
    try {
      channel.put(EventBuilder.withBody(EVENT_BODY, Charsets.UTF_8));
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store event to HDFS
    sink.process();

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);
    Assert.assertEquals("Only one file expected", 1, statuses.length);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertEquals(EVENT_BODY, line);
    }

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }
  }

  /**
   * Writes two events in GZIP-compressed format.
   */
  @Test
  public void simpleHDFSGZipCompressedTest() throws EventDeliveryException, IOException {
    String outputDir = "/flume/simpleHDFSGZipCompressedTest";
    Path outputDirPath = new Path(outputDir);

    logger.info("Running test with output dir: {}", outputDir);

    FileSystem fs = cluster.getFileSystem();
    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    String nnURL = getNameNodeURL(cluster);
    logger.info("Namenode address: {}", nnURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nnURL + outputDir);
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.CompStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));
    sinkCtx.put("hdfs.codeC", "gzip");

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    String EVENT_BODY_1 = "yarg1";
    String EVENT_BODY_2 = "yarg2";
    channel.getTransaction().begin();
    try {
      channel.put(EventBuilder.withBody(EVENT_BODY_1, Charsets.UTF_8));
      channel.put(EventBuilder.withBody(EVENT_BODY_2, Charsets.UTF_8));
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store event to HDFS
    sink.process();

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);
    Assert.assertEquals("Only one file expected", 1, statuses.length);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          new GZIPInputStream(stream)));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertEquals(EVENT_BODY_1, line);

      // The rest of this test is commented-out (will fail) for 2 reasons:
      //
      // (1) At the time of this writing, Hadoop has a bug which causes the
      // non-native gzip implementation to create invalid gzip files when
      // finish() and resetState() are called. See HADOOP-8522.
      //
      // (2) Even if HADOOP-8522 is fixed, the JDK GZipInputStream is unable
      // to read multi-member (concatenated) gzip files. See this Sun bug:
      // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4691425
      //
      //line = reader.readLine();
      //logger.info("Second line in file {}: {}", filePath, line);
      //Assert.assertEquals(EVENT_BODY_2, line);
    }

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }
  }

  @AfterClass
  public static void teardown() {
    cluster.shutdown();
    cluster = null;

    // restore system state, if needed
    if (oldTestBuildDataProp != null) {
      System.setProperty(TEST_BUILD_DATA_KEY, oldTestBuildDataProp);
    }

    if (!KEEP_DATA) {
      FileUtils.deleteQuietly(new File(DFS_DIR));
    }
  }

}
