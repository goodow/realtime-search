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
package com.goodow.realtime.search.server.integration;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

public class SearchIntegrationTest extends TestVerticle {
  private final String id = "test_id";
  private final String index = "test_index";
  private final String type = "test_type";
  private final String source_user = "larry";
  private final String source_message = "中华人民共和国";

  @Override
  public void start() {
    initialize();
    container.deployModule(System.getProperty("vertx.modulename"), new JsonObject().putBoolean(
        "client_transport_sniff", false), new AsyncResultHandler<String>() {
      @Override
      public void handle(AsyncResult<String> asyncResult) {
        VertxAssert.assertTrue(asyncResult.succeeded());
        VertxAssert.assertNotNull("deploymentID should not be null", asyncResult.result());
        startTests();
      }
    });
  }

  @Test
  public void testIndex() throws Exception {
    JsonObject message =
        new JsonObject().putString("action", "index").putString("_index", index).putString("_type",
            type).putString("_id", id).putObject("source",
            new JsonObject().putString("user", source_user).putString("message", source_message));

    vertx.eventBus().sendWithTimeout("realtime.search", message, 5000,
        new AsyncResultHandler<Message<JsonObject>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> ar) {
            VertxAssert.assertTrue(ar.succeeded());
            JsonObject body = ar.result().body();
            VertxAssert.assertEquals(index, body.getString("_index"));
            VertxAssert.assertEquals(type, body.getString("_type"));
            VertxAssert.assertEquals(id, body.getString("_id"));
            VertxAssert.assertTrue(body.getLong("_version") > 0);
            VertxAssert.testComplete();
          }
        });

  }
}
