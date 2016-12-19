/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.xml.bind.DatatypeConverter;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAvroSource {

  /* key, self signed for "CN=localhost, ..." */
  private static final String RESOURCES_SERVER_SELF_SIGNED_KEY_P12 = "src/test/resources/server.p12";
  /* key, self signed for "CN=unknown, ..." */
  private static final String RESOURCES_SERVER_SELF_SIGNED_KEYSTORE = "src/test/resources/keystorefile.jks";

  /* keys with certs signed by "root-ca" key */
  private static final String RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12 = "src/test/resources/server_n.p12";
  private static final String RESOURCES_CLIENT_SIGNED_KEYSTORE = "src/test/resources/client-keystore.jks";
  private static final String CLIENT_KEY_ALIAS = "client";
  private static final String CLIENT_KEY_DN = "CN=Client, OU=OpsDirector, O=TESM Limited, L=London, ST=London, C=GB";

  /* the "root-ca" certificate */
  private static final String RESOURCES_ROOT_CA_CERT_KEYSTORE = "src/test/resources/root-ca-cert.jks";

  private static final String KEYSTORE_PASSWORD = "password";

  private static final Logger logger = LoggerFactory
      .getLogger(TestAvroSource.class);

  private int selectedPort;
  private AvroSource source;
  private Channel channel;
  private InetAddress localhost;

  @Before
  public void setUp() throws UnknownHostException {
    localhost = InetAddress.getByName("127.0.0.1");
    source = new AvroSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();

        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("bind", "0.0.0.0");

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }

  @Test
  public void testRequestWithNoCompression() throws InterruptedException, IOException {

    doRequest(false, false, 6);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel0() throws InterruptedException, IOException {

    doRequest(true, true, 0);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel1() throws InterruptedException, IOException {

    doRequest(true, true, 1);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel6() throws InterruptedException, IOException {

    doRequest(true, true, 6);
  }

  @Test
  public void testRequestWithCompressionOnClientAndServerOnLevel9() throws InterruptedException, IOException {

    doRequest(true, true, 9);
  }

  @Test(expected=org.apache.avro.AvroRemoteException.class)
  public void testRequestWithCompressionOnServerOnly() throws InterruptedException, IOException {
    //This will fail because both client and server need compression on
    doRequest(true, false, 6);
  }

  @Test(expected=org.apache.avro.AvroRemoteException.class)
  public void testRequestWithCompressionOnClientOnly() throws InterruptedException, IOException {
    //This will fail because both client and server need compression on
    doRequest(false, true, 6);
  }

  @Test
  public void testRequestCannotSetEventSslDnHeader() throws InterruptedException, IOException {
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();

    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-dn", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro".getBytes()));

    Event event = doRequest(false, false, 6, avroEvent);
    Assert.assertNull(event.getHeaders().get("ssl-dn"));
  }

  @Test
  public void testRequestCannotSetEventSslCertHeader() throws InterruptedException, IOException {
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();

    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-cert", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro".getBytes()));

    Event event = doRequest(false, false, 6, avroEvent);
    Assert.assertNull(event.getHeaders().get("ssl-cert"));
  }

  private Event doRequest(boolean serverEnableCompression, boolean clientEnableCompression, int compressionLevel) throws InterruptedException, IOException {
    return doRequest(serverEnableCompression, clientEnableCompression, compressionLevel, null);
  }

  private Event doRequest(boolean serverEnableCompression, boolean clientEnableCompression, int compressionLevel, AvroFlumeEvent avroEvent) throws InterruptedException, IOException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();
        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("bind", "0.0.0.0");
        context.put("threads", "50");
        if (serverEnableCompression) {
          context.put("compression-type", "deflate");
        } else {
          context.put("compression-type", "none");
        }

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    AvroSourceProtocol client;
    NettyTransceiver nettyTransceiver;
    if (clientEnableCompression) {

      nettyTransceiver = new NettyTransceiver(new InetSocketAddress(
          selectedPort), new CompressionChannelFactory(compressionLevel));

      client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, nettyTransceiver);
    } else {
      nettyTransceiver = new NettyTransceiver(new InetSocketAddress(selectedPort));

      client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, nettyTransceiver);
    }

    if (avroEvent == null) {
      avroEvent = new AvroFlumeEvent();

      avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
      avroEvent.setBody(ByteBuffer.wrap("Hello avro".getBytes()));
    }

    Status status = client.append(avroEvent);

    Assert.assertEquals(Status.OK, status);

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Channel contained our event", "Hello avro",
        new String(event.getBody()));
    transaction.commit();
    transaction.close();

    logger.debug("Round trip event:{}", event);


    nettyTransceiver.close();
    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());

    return event;
  }

  private static class CompressionChannelFactory extends
      NioClientSocketChannelFactory {
    private int compressionLevel;

    public CompressionChannelFactory( int compressionLevel) {
      super();
      this.compressionLevel = compressionLevel;
    }

    @Override
    public SocketChannel newChannel(ChannelPipeline pipeline) {
      try {

        ZlibEncoder encoder = new ZlibEncoder(compressionLevel);
        pipeline.addFirst("deflater", encoder);
        pipeline.addFirst("inflater", new ZlibDecoder());
        return super.newChannel(pipeline);
      } catch (Exception ex) {
        throw new RuntimeException("Cannot create Compression channel", ex);
      }
    }
  }

  @Test
  public void testSslRequest() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("keystore", RESOURCES_SERVER_SELF_SIGNED_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    prepareSslServer(context);

    doSslRequest(true, null);
  }

  @Test
  public void testSslRequestWithClientAuthNeededSent() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);
    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    doSslRequest(true, client);
  }

  @Test
  public void testSslRequestWithClientAuthSetsEventSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    Event event = doSslRequest(true, client);
    Assert.assertEquals(CLIENT_KEY_DN, event.getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthSetsEventSslCertHeader() throws InterruptedException, IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    String expectedCertificate = getClientCertBase64();

    Event event = doSslRequest(true, client);
    Assert.assertEquals(expectedCertificate, event.getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthBatchSetsEventSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    List<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
    avroEvents.add(avroEvent);
    avroEvents.add(avroEvent);

    List<Event> events = doSslRequestList(true, client, avroEvents);
    Assert.assertEquals(CLIENT_KEY_DN, events.get(0).getHeaders().get("ssl-dn"));
    Assert.assertEquals(CLIENT_KEY_DN, events.get(1).getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthBatchSetsEventSslCertHeader() throws InterruptedException, IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    List<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
    avroEvents.add(avroEvent);
    avroEvents.add(avroEvent);

    String expectedCertificate = getClientCertBase64();

    List<Event> events = doSslRequestList(true, client, avroEvents);
    Assert.assertEquals(expectedCertificate, events.get(0).getHeaders().get("ssl-cert"));
    Assert.assertEquals(expectedCertificate, events.get(1).getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthCannotOverrideSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-dn", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));

    Event event = doSslRequest(true, client, avroEvent);
    Assert.assertEquals(CLIENT_KEY_DN, event.getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthCannotOverrideSslCertHeader() throws InterruptedException, IOException, CertificateEncodingException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-cert", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));

    String expectedCertificate = getClientCertBase64();

    Event event = doSslRequest(true, client, avroEvent);
    Assert.assertEquals(expectedCertificate, event.getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthBatchCannotOverrideEventSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    List<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-dn", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
    avroEvents.add(avroEvent);
    avroEvents.add(avroEvent);

    List<Event> events = doSslRequestList(true, client, avroEvents);
    Assert.assertEquals(CLIENT_KEY_DN, events.get(0).getHeaders().get("ssl-dn"));
    Assert.assertEquals(CLIENT_KEY_DN, events.get(1).getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthBatchCannotOverrideEventSslCertHeader() throws InterruptedException, IOException, CertificateEncodingException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    List<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-cert", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
    avroEvents.add(avroEvent);
    avroEvents.add(avroEvent);

    String expectedCertificate = getClientCertBase64();

    List<Event> events = doSslRequestList(true, client, avroEvents);

    Assert.assertEquals(expectedCertificate, events.get(0).getHeaders().get("ssl-cert"));
    Assert.assertEquals(expectedCertificate, events.get(1).getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthNeededMissing() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);

    prepareSslServer(context);
    doSslRequest(false, null);
  }

  @Test
  public void testSslRequestWithClientAuthNeededInvalid() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "need");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_SERVER_SELF_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    doSslRequest(false, client);
  }

  @Test
  public void testSslRequestWithClientAuthWantedSent() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    /** client auth supplied - "want" should accept the connection */
    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
        selectedPort), new SSLChannelFactory(
            RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
            RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    doSslRequest(true, client);
  }

  @Test
  public void testSslRequestWithClientAuthWantedSetsSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    /** client auth supplied - "want" should accept the connection and record the certificate provided */
    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
        selectedPort), new SSLChannelFactory(
            RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
            RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    Event event = doSslRequest(true, client);
    Assert.assertEquals(CLIENT_KEY_DN, event.getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedSetsSslCertHeader() throws InterruptedException, IOException, CertificateEncodingException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    /** client auth supplied - "want" should accept the connection and record the certificate provided */
    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
        selectedPort), new SSLChannelFactory(
            RESOURCES_CLIENT_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
            RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    String expectedCertificate = getClientCertBase64();

    Event event = doSslRequest(true, client);
    Assert.assertEquals(expectedCertificate, event.getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissing() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    doSslRequest(true, null);
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissingDoesNotSetSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    Event event = doSslRequest(true, null);
    Assert.assertNull(event.getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissingDoesNotSetSslCertHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    Event event = doSslRequest(true, null);
    Assert.assertNull(event.getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissingCannotOverrideSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-dn", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));

    Event event = doSslRequest(true, null, avroEvent);
    Assert.assertNull(event.getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissingCannotOverrideSslCertHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-cert", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));

    Event event = doSslRequest(true, null, avroEvent);
    Assert.assertNull(event.getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissingBatchDoesNotSetSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    List<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
    avroEvents.add(avroEvent);
    avroEvents.add(avroEvent);

    List<Event> event = doSslRequestList(true, null, avroEvents);
    Assert.assertNull(event.get(0).getHeaders().get("ssl-dn"));
    Assert.assertNull(event.get(1).getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissingBatchDoesNotSetSslCertHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    List<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
    avroEvents.add(avroEvent);
    avroEvents.add(avroEvent);

    List<Event> event = doSslRequestList(true, null, avroEvents);
    Assert.assertNull(event.get(0).getHeaders().get("ssl-cert"));
    Assert.assertNull(event.get(1).getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissingBatchCannotOverrideSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    List<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-dn", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
    avroEvents.add(avroEvent);
    avroEvents.add(avroEvent);

    List<Event> event = doSslRequestList(true, null, avroEvents);
    Assert.assertNull(event.get(0).getHeaders().get("ssl-dn"));
    Assert.assertNull(event.get(1).getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedMissingBatchCannotOverrideSslCertHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    List<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    HashMap<CharSequence, CharSequence> headers = new HashMap<CharSequence, CharSequence>();
    headers.put("ssl-cert", "my own value");
    avroEvent.setHeaders(headers);
    avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
    avroEvents.add(avroEvent);
    avroEvents.add(avroEvent);

    List<Event> event = doSslRequestList(true, null, avroEvents);
    Assert.assertNull(event.get(0).getHeaders().get("ssl-cert"));
    Assert.assertNull(event.get(1).getHeaders().get("ssl-cert"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedInvalid() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    /** incorrect client auth supplied - "want" should still accept the connection */
    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_SERVER_SELF_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    doSslRequest(true, client);
  }

  @Test
  public void testSslRequestWithClientAuthWantedInvalidDoesNotSetSslDnHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    /** incorrect client auth supplied - "want" should still accept the connection */
    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_SERVER_SELF_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    Event event = doSslRequest(true, client);
    Assert.assertNull(event.getHeaders().get("ssl-dn"));
  }

  @Test
  public void testSslRequestWithClientAuthWantedInvalidDoesNotSetSslCertHeader() throws InterruptedException, IOException {
    Context context = new Context();

    context.put("bind", "0.0.0.0");
    context.put("ssl", "true");
    context.put("client-auth", "want");
    context.put("keystore", RESOURCES_SERVER_SIGNED_ROOT_CA_KEY_P12);
    context.put("keystore-password", KEYSTORE_PASSWORD);
    context.put("keystore-type", "PKCS12");
    context.put("truststore", RESOURCES_ROOT_CA_CERT_KEYSTORE);
    context.put("truststore-password", KEYSTORE_PASSWORD);
    prepareSslServer(context);

    /** incorrect client auth supplied - "want" should still accept the connection */
    AvroSourceProtocol client = SpecificRequestor.getClient(
        AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
            selectedPort), new SSLChannelFactory(
                RESOURCES_SERVER_SELF_SIGNED_KEYSTORE, KEYSTORE_PASSWORD, "JKS",
                RESOURCES_ROOT_CA_CERT_KEYSTORE, KEYSTORE_PASSWORD, "JKS")));

    Event event = doSslRequest(true, client);
    Assert.assertNull(event.getHeaders().get("ssl-cert"));
  }

  private void prepareSslServer(Context context) throws InterruptedException {
    boolean bound = false;

    for (int i = 0; i < 10 && !bound; i++) {
      try {
        context.put("port", String.valueOf(selectedPort = 41414 + i));

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
        Thread.sleep(100);
      }
    }
  }

  private Event doSslRequest(boolean successExpected, AvroSourceProtocol client) throws InterruptedException, IOException {
    ArrayList<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    return doSslRequestList(successExpected, client, avroEvents).get(0);
  }

  private Event doSslRequest(boolean successExpected, AvroSourceProtocol client, AvroFlumeEvent avroEvent) throws InterruptedException, IOException {
    ArrayList<AvroFlumeEvent> avroEvents = new ArrayList<AvroFlumeEvent>();
    avroEvents.add(avroEvent);
    return doSslRequestList(successExpected, client, avroEvents).get(0);
  }

  private List<Event> doSslRequestList(boolean successExpected, AvroSourceProtocol client, List<AvroFlumeEvent> avroEvents) throws InterruptedException, IOException {
    Assert
    .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
        source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    if (client == null) {
      client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, new NettyTransceiver(new InetSocketAddress(
              selectedPort), new SSLChannelFactory()));
    }

    if (avroEvents == null || avroEvents.isEmpty()) {
      avroEvents = new ArrayList<AvroFlumeEvent>();
      AvroFlumeEvent avroEvent = new AvroFlumeEvent();
      avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
      avroEvent.setBody(ByteBuffer.wrap("Hello avro ssl".getBytes()));
      avroEvents.add(avroEvent);
    }

    boolean sent = false;
    Status status = Status.UNKNOWN;
    try {
      if (avroEvents.size() == 1) {
        status = client.append(avroEvents.get(0));
      } else if (avroEvents.size() > 1) {
        // send as a batch
        status = client.appendBatch(avroEvents);
      }
      sent = true;
    } catch (AvroRemoteException ex) {
      logger.info("Failed to send event", ex);
    }

    if (successExpected) {
      Assert.assertEquals(Status.OK, status);
    } else {
      Assert.assertNotEquals(Status.OK, status);
    }

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    List<Event> events = new ArrayList<Event>();
    Event event;
    int count = 0;
    do {
      count++;
      event = channel.take();
      if (successExpected) {
        Assert.assertNotNull(event);
        Assert.assertEquals("Channel contained our event", "Hello avro ssl",
            new String(event.getBody()));
        logger.debug("Round trip event:{}", event);
      } else {
        Assert.assertNull(event);
      }
      events.add(event);
    } while (event != null && count < avroEvents.size());

    transaction.commit();
    transaction.close();

    source.stop();
    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR, 5000));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());

    if (!successExpected && sent) {
      Assert.fail("SSL-enabled source successfully accepted from client with an untrusted certificate when it should have failed");
    }

    return events;
  }

  private String getClientCertBase64() throws FileNotFoundException, KeyStoreException, IOException,
      NoSuchAlgorithmException, CertificateException, CertificateEncodingException {
    InputStream truststoreStream = new FileInputStream(RESOURCES_CLIENT_SIGNED_KEYSTORE);
    KeyStore ts = KeyStore.getInstance("JKS");
    ts.load(truststoreStream, KEYSTORE_PASSWORD.toCharArray());

    return DatatypeConverter.printBase64Binary(ts.getCertificate(CLIENT_KEY_ALIAS).getEncoded());
  }

  /**
   * Factory of SSL-enabled client channels
   * Copied from Avro's org.apache.avro.ipc.TestNettyServerWithSSL test
   */
  private static class SSLChannelFactory extends NioClientSocketChannelFactory {
    private boolean testClientAuth;
    private String keystore;
    private String keystorePassword;
    private String keystoreType;
    private String truststore;
    private String truststorePassword;
    private String truststoreType;

    public SSLChannelFactory() {
      super(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    }

    public SSLChannelFactory(String keystore, String keystorePassword, String keystoreType,
        String truststore, String truststorePassword, String truststoreType) {
      super(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
      testClientAuth = true;
      this.keystore = keystore;
      this.keystorePassword = keystorePassword;
      this.keystoreType = keystoreType;
      this.truststore = truststore;
      this.truststorePassword = truststorePassword;
      this.truststoreType = truststoreType;
    }

    @Override
    public SocketChannel newChannel(ChannelPipeline pipeline) {
      try {
        KeyManager[] keyManagers = null;
        TrustManager[] trustManagers = new TrustManager[]{new PermissiveTrustManager()};
        if (testClientAuth) {
          if (truststore != null) {
            if (truststorePassword == null) {
              throw new RuntimeException("truststore password is null");
            }
            InputStream truststoreStream = new FileInputStream(truststore);
            KeyStore ts = KeyStore.getInstance(truststoreType);
            ts.load(truststoreStream, truststorePassword.toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(ts);
            trustManagers = tmf.getTrustManagers();
          }
          if (keystore != null) {
            if (keystorePassword == null) {
              throw new RuntimeException("keystore password is null");
            }
            InputStream keystoreStream = new FileInputStream(keystore);
            KeyStore ks = KeyStore.getInstance(keystoreType);
            ks.load(keystoreStream, keystorePassword.toCharArray());
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keystorePassword.toCharArray());
            keyManagers = kmf.getKeyManagers();
          }
        }
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, null);
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(true);
        // addFirst() will make SSL handling the first stage of decoding
        // and the last stage of encoding
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
        return super.newChannel(pipeline);
      } catch (Exception ex) {
        throw new RuntimeException("Cannot create SSL channel", ex);
      }
    }
  }

  /**
   * Bogus trust manager accepting any certificate
   */
  private static class PermissiveTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  @Test
  public void testValidIpFilterAllows()
      throws InterruptedException, IOException {
    doIpFilterTest(localhost, "allow:name:localhost,deny:ip:*", true, false);
    doIpFilterTest(localhost, "allow:ip:" + localhost.getHostAddress() +
        ",deny:ip:*", true, false);
    doIpFilterTest(localhost, "allow:ip:*", true, false);
    doIpFilterTest(localhost, "allow:ip:" +
        localhost.getHostAddress().substring(0, 3) +
        "*,deny:ip:*", true, false);
    doIpFilterTest(localhost, "allow:ip:127.0.0.2,allow:ip:" +
        localhost.getHostAddress().substring(0, 3) +
        "*,deny:ip:*", true, false);
    doIpFilterTest(localhost, "allow:name:localhost,deny:ip:*", true, true);
    doIpFilterTest(localhost, "allow:ip:*", true, true);
  }

  @Test
  public void testValidIpFilterDenys()
      throws InterruptedException, IOException {
    doIpFilterTest(localhost, "deny:ip:*", false, false);
    doIpFilterTest(localhost, "deny:name:localhost", false, false);
    doIpFilterTest(localhost, "deny:ip:" + localhost.getHostAddress() +
        ",allow:ip:*", false, false);
    doIpFilterTest(localhost, "deny:ip:*", false, false);
    doIpFilterTest(localhost, "allow:ip:45.2.2.2,deny:ip:*", false, false);
    doIpFilterTest(localhost, "deny:ip:" +
        localhost.getHostAddress().substring(0, 3) +
        "*,allow:ip:*", false, false);
    doIpFilterTest(localhost, "deny:ip:*", false, true);
  }

  @Test
  public void testInvalidIpFilter() throws InterruptedException, IOException {
    doIpFilterTest(localhost, "deny:ip:*", false, false);
    doIpFilterTest(localhost, "allow:name:localhost", true, false);
    doIpFilterTest(localhost, "deny:ip:127.0.0.2,allow:ip:*,deny:ip:" +
        localhost.getHostAddress(), true, false);
    doIpFilterTest(localhost, "deny:ip:" +
        localhost.getHostAddress().substring(0, 3) + "*,allow:ip:*",
        false, false);
    try {
      doIpFilterTest(localhost, null, false, false);
      Assert.fail(
        "The null ipFilterRules config should have thrown an exception.");
    } catch (FlumeException e) {
      //Do nothing
    }

    try {
      doIpFilterTest(localhost, "", true, false);
      Assert.fail("The empty string ipFilterRules config should have thrown "
          + "an exception");
    } catch (FlumeException e) {
      //Do nothing
    }

    try {
      doIpFilterTest(localhost, "homer:ip:45.4.23.1", true, false);
      Assert.fail("Bad ipFilterRules config should have thrown an exception.");
    } catch (FlumeException e) {
      //Do nothing
    }
    try {
      doIpFilterTest(localhost, "allow:sleeps:45.4.23.1", true, false);
      Assert.fail("Bad ipFilterRules config should have thrown an exception.");
    } catch (FlumeException e) {
      //Do nothing
    }
  }

  public void doIpFilterTest(InetAddress dest, String ruleDefinition,
      boolean eventShouldBeAllowed, boolean testWithSSL)
      throws InterruptedException, IOException {
    boolean bound = false;

    for (int i = 0; i < 100 && !bound; i++) {
      try {
        Context context = new Context();
        context.put("port", String.valueOf(selectedPort = 41414 + i));
        context.put("bind", "0.0.0.0");
        context.put("ipFilter", "true");
        if (ruleDefinition != null) {
          context.put("ipFilterRules", ruleDefinition);
        }
        if (testWithSSL) {
          logger.info("Client testWithSSL" + testWithSSL);
          context.put("ssl", "true");
          context.put("keystore", RESOURCES_SERVER_SELF_SIGNED_KEY_P12);
          context.put("keystore-password", KEYSTORE_PASSWORD);
          context.put("keystore-type", "PKCS12");
        }

        Configurables.configure(source, context);

        source.start();
        bound = true;
      } catch (ChannelException e) {
        /*
         * NB: This assume we're using the Netty server under the hood and the
         * failure is to bind. Yucky.
         */
        Thread.sleep(100);
      }
    }

    Assert
        .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
            source, LifecycleState.START_OR_ERROR));
    Assert.assertEquals("Server is started", LifecycleState.START,
        source.getLifecycleState());

    AvroSourceProtocol client;
    NettyTransceiver nettyTransceiver = null;
    try {
      if (testWithSSL) {
        nettyTransceiver = new NettyTransceiver(
          new InetSocketAddress (dest, selectedPort),
          new SSLChannelFactory());
        client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, nettyTransceiver);
      } else {
        nettyTransceiver = new NettyTransceiver(
          new InetSocketAddress (dest, selectedPort));
        client = SpecificRequestor.getClient(
          AvroSourceProtocol.class, nettyTransceiver);
      }

      AvroFlumeEvent avroEvent = new AvroFlumeEvent();
      avroEvent.setHeaders(new HashMap<CharSequence, CharSequence>());
      avroEvent.setBody(ByteBuffer.wrap("Hello avro ipFilter".getBytes()));

      logger.info("Client about to append");
      Status status = client.append(avroEvent);
      logger.info("Client appended");
      Assert.assertEquals(Status.OK, status);
    } catch (IOException e) {
      Assert.assertTrue("Should have been allowed: " + ruleDefinition,
        !eventShouldBeAllowed);
      return;
    } finally {
      if (nettyTransceiver != null) {
        nettyTransceiver.close();
      }
      source.stop();
    }
    Assert.assertTrue("Should have been denied: " + ruleDefinition,
        eventShouldBeAllowed);

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    Assert.assertNotNull(event);
    Assert.assertEquals("Channel contained our event", "Hello avro ipFilter",
        new String(event.getBody()));
    transaction.commit();
    transaction.close();
    logger.debug("Round trip event:{}", event);

    Assert.assertTrue("Reached stop or error",
        LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
    Assert.assertEquals("Server is stopped", LifecycleState.STOP,
        source.getLifecycleState());
  }
}
