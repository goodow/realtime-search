/*
 * Copyright 2014 Goodow.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.goodow.realtime.search.server;

import com.alienos.guice.VertxModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;

public class SearchModule extends AbstractModule implements VertxModule {
  private Vertx vertx;
  private Container container;

  @Override
  public void setContainer(Container container) {
    this.container = container;
  }

  @Override
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  protected void configure() {
  }

  @Provides
  @Singleton
  Client provideElasticSearchClient() {
    JsonObject config = container.config().getObject("elasticsearch", new JsonObject());
    Settings settings =
        ImmutableSettings.settingsBuilder().put("cluster.name",
            config.getString("cluster_name", "elasticsearch")).put("client.transport.sniff",
            config.getBoolean("client_transport_sniff", true)).build();
    TransportClient client = new TransportClient(settings);
    JsonArray transportAddresses =
        config.getArray("transportAddresses", new JsonArray().add(new JsonObject().putString(
            "host", "localhost").putNumber("port", 9300)));
    for (Object addr : transportAddresses) {
      JsonObject transportAddress = (JsonObject) addr;
      String hostname = transportAddress.getString("host");

      if (hostname != null && !hostname.isEmpty()) {
        int port = transportAddress.getInteger("port", 9300);
        client.addTransportAddress(new InetSocketTransportAddress(hostname, port));
      }
    }
    return client;
  }
}