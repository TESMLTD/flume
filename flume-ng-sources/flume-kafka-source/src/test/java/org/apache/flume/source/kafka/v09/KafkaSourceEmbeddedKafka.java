/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.kafka.v09;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaSourceEmbeddedKafka {
  KafkaServerStartable kafkaServer;
  KafkaSourceEmbeddedZookeeper zookeeper;
  int zkPort = 21818; // none-standard
  int serverPort = 18922;

  KafkaProducer<String, String> producer;
  File dir;

  public KafkaSourceEmbeddedKafka(Properties properties) {
    zookeeper = new KafkaSourceEmbeddedZookeeper(zkPort);
    dir = new File(System.getProperty("java.io.tmpdir"), "kafka_log-" + UUID.randomUUID());
    try {
      FileUtils.deleteDirectory(dir);
    } catch (IOException e) {
      e.printStackTrace();
    }
    Properties props = new Properties();
    props.put("zookeeper.connect",zookeeper.getConnectString());
    props.put("broker.id","1");
    props.put("host.name", "localhost");
    props.put("port", String.valueOf(serverPort));
    props.put("log.dir", dir.getAbsolutePath());
    if (properties != null)
      props.putAll(props);
    KafkaConfig config = new KafkaConfig(props);
    kafkaServer = new KafkaServerStartable(config);
    kafkaServer.startup();
    initProducer();
  }

  public void stop() throws IOException {
    producer.close();
    kafkaServer.shutdown();
    zookeeper.stopZookeeper();
  }

  public String getZkConnectString() {
    return zookeeper.getConnectString();
  }

  public String getBrockers() {
    return "127.0.0.1:" + serverPort;
  }

  private void initProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers","127.0.0.1:" + serverPort);
    props.put("acks", "1");
    producer = new KafkaProducer<String,String>(props,
            new StringSerializer(), new StringSerializer());
  }

  public void produce(String topic, String k, String v) {
    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, k, v);
    try {
      producer.send(rec).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void produce(String topic, int partition, String k, String v) {
    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, partition, k, v);
    try {
      producer.send(rec).get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void createTopic(String topicName, int numPartitions) {
    // Create a ZooKeeper client
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = ZkUtils.createZkClient("127.0.0.1:" + zkPort, sessionTimeoutMs, connectionTimeoutMs);
    ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
    int replicationFactor = 1;
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(zkUtils, topicName, numPartitions,
            replicationFactor, topicConfig);
  }

}