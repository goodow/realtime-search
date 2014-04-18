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
package com.goodow.realtime.search.server.impl;

import com.goodow.realtime.json.JsonObject.MapIterator;
import com.goodow.realtime.json.impl.JreJsonObject;
import com.goodow.realtime.json.util.Yaml;
import com.goodow.realtime.search.server.SearchVerticle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.CountingCompletionHandler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import javax.inject.Inject;

public class AdminActioin implements Handler<Message<JsonObject>> {
  private static final long REPLY_TIMEOUT = 500;
  private final Logger logger;
  @Inject private Client client;
  private final Vertx vertx;
  private final String address;

  @Inject
  AdminActioin(Vertx vertx, Container container) {
    this.vertx = vertx;
    logger = container.logger();
    address = container.config().getString("address", SearchVerticle.DEFAULT_ADDRESS);
  }

  @Override
  public void handle(final Message<JsonObject> message) {
    JsonObject body = message.body();
    client.admin().indices().preparePutTemplate(body.getString("_name")).setCreate(
        body.getBoolean("create", false)).setSource(body.getObject("source").encode()).execute(
        new ActionListener<PutIndexTemplateResponse>() {
          @Override
          public void onFailure(Throwable e) {
            ElasticSearchHandler.replyFail(logger, message, "Put Index Template error: "
                + e.getMessage(), e);
          }

          @Override
          public void onResponse(PutIndexTemplateResponse response) {
            message.reply(new JsonObject().putBoolean("acknowledged", response.isAcknowledged()));
          }
        });
  }

  public void start(final CountingCompletionHandler<Void> countDownLatch) {
    countDownLatch.incRequired();
    vertx.fileSystem().readDir("elasticsearch/templates", ".*\\.yaml",
        new Handler<AsyncResult<String[]>>() {
          @Override
          public void handle(AsyncResult<String[]> ar) {
            if (ar.succeeded()) {
              for (final String file : ar.result()) {
                countDownLatch.incRequired();
                vertx.fileSystem().readFile(file, new Handler<AsyncResult<Buffer>>() {
                  @Override
                  public void handle(AsyncResult<Buffer> ar) {
                    if (ar.failed()) {
                      logger.debug("Can't read file " + file, ar.cause());
                    } else {
                      JreJsonObject templates = Yaml.parse(ar.result().toString());
                      templates.forEach(new MapIterator<JreJsonObject>() {
                        @Override
                        public void call(String name, JreJsonObject template) {
                          countDownLatch.incRequired();
                          JsonObject message =
                              new JsonObject().putString("action",
                                  ElasticSearchHandler.PUT_INDEX_TEMPLATE).putString("_name", name)
                                  .putObject("source", new JsonObject(template.toNative()));
                          vertx.eventBus().sendWithTimeout(address, message, REPLY_TIMEOUT,
                              new Handler<AsyncResult<Message<JsonObject>>>() {
                                @Override
                                public void handle(AsyncResult<Message<JsonObject>> ar) {
                                  if (ar.failed()) {
                                    countDownLatch.failed(ar.cause());
                                  } else {
                                    countDownLatch.complete();
                                  }
                                }
                              });
                        }
                      });
                    }
                    countDownLatch.complete();
                  }
                });
              }
            }
            countDownLatch.complete();
          }
        });
  }
}