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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import java.io.IOException;

import javax.inject.Inject;

public class ElasticSearchHandler implements Handler<Message<JsonObject>> {
  public static final String CONST_INDEX = "_index";
  public static final String CONST_TYPE = "_type";
  public static final String CONST_ID = "_id";
  public static final String CONST_VERSION = "_version";
  public static final String CONST_SOURCE = "_source";

  public static void sendError(Logger logger, Message<JsonObject> message, String error, Exception e) {
    logger.error(error, e);
    JsonObject json = new JsonObject().putString("status", "error").putString("message", error);
    message.reply(json);
  }

  public static void sendOK(Message<JsonObject> message, JsonObject json) {
    if (json == null) {
      json = new JsonObject();
    }
    json.putString("status", "ok");
    message.reply(json);
  }

  static void handleActionResponse(Logger logger, ToXContent toXContent, Message<JsonObject> message) {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      toXContent.toXContent(builder, SearchResponse.EMPTY_PARAMS);
      builder.endObject();

      JsonObject response = new JsonObject(builder.string());
      sendOK(message, response);
    } catch (IOException e) {
      sendError(logger, message, "Error reading search response: " + e.getMessage(), e);
    }
  }

  @Inject private Client client;
  @Inject private SearchActioin search;
  private final Logger logger;

  @Inject
  ElasticSearchHandler(Container container) {
    logger = container.logger();
  }

  @Override
  public void handle(Message<JsonObject> message) {
    try {
      String action = message.body().getString("action");
      if (action == null) {
        sendError(logger, message, "action must be specified", null);
        return;
      }
      switch (action) {
        case "index":
          doIndex(message);
          break;
        case "get":
          doGet(message);
          break;
        case "search":
          search.handle(message);
          break;
        case "scroll":
          doScroll(message);
          break;
        default:
          sendError(logger, message, "Unrecognized action " + action, null);
          break;
      }
    } catch (Exception e) {
      sendError(logger, message, "Unhandled exception!", e);
    }
  }

  String getRequiredIndex(JsonObject json, Message<JsonObject> message) {
    String index = json.getString(CONST_INDEX);
    if (index == null || index.isEmpty()) {
      sendError(logger, message, CONST_INDEX + " is required", null);
      return null;
    }
    return index;
  }

  String getRequiredType(JsonObject json, Message<JsonObject> message) {
    String type = json.getString(CONST_TYPE);
    if (type == null || type.isEmpty()) {
      sendError(logger, message, CONST_TYPE + " is required", null);
      return null;
    }
    return type;
  }

  private void doGet(final Message<JsonObject> message) {
    JsonObject body = message.body();
    final String index = getRequiredIndex(body, message);
    if (index == null) {
      return;
    }
    String type = getRequiredType(body, message);
    if (type == null) {
      return;
    }
    String id = body.getString(CONST_ID);
    if (id == null) {
      sendError(logger, message, CONST_ID + " is required", null);
      return;
    }
    client.prepareGet(index, type, id).execute(new ActionListener<GetResponse>() {
      @Override
      public void onFailure(Throwable e) {
        sendError(logger, message, "Get error: " + e.getMessage(), new RuntimeException(e));
      }

      @Override
      public void onResponse(GetResponse getFields) {
        JsonObject source =
            (getFields.isExists() ? new JsonObject(getFields.getSourceAsString()) : null);
        JsonObject reply =
            new JsonObject().putString(CONST_INDEX, getFields.getIndex()).putString(CONST_TYPE,
                getFields.getType()).putString(CONST_ID, getFields.getId()).putNumber(
                CONST_VERSION, getFields.getVersion()).putObject(CONST_SOURCE, source);
        sendOK(message, reply);
      }
    });
  }

  private void doIndex(final Message<JsonObject> message) {
    JsonObject body = message.body();
    final String index = getRequiredIndex(body, message);
    if (index == null) {
      return;
    }
    String type = getRequiredType(body, message);
    if (type == null) {
      return;
    }
    JsonObject source = body.getObject(CONST_SOURCE);
    if (source == null) {
      sendError(logger, message, CONST_SOURCE + " is required", null);
      return;
    }

    IndexRequestBuilder builder =
        client.prepareIndex(index, type, body.getString(CONST_ID)).setSource(source.encode());

    if (body.containsField("version")) {
      builder.setVersion(body.getLong("version"));
    }
    if (body.containsField("version_type")) {
      builder.setVersionType(VersionType.fromString(body.getString("version_type")));
    }

    builder.execute(new ActionListener<IndexResponse>() {
      @Override
      public void onFailure(Throwable e) {
        sendError(logger, message, "Index error: " + e.getMessage(), new RuntimeException(e));
      }

      @Override
      public void onResponse(IndexResponse indexResponse) {
        JsonObject reply =
            new JsonObject().putString(CONST_INDEX, indexResponse.getIndex()).putString(CONST_TYPE,
                indexResponse.getType()).putString(CONST_ID, indexResponse.getId()).putNumber(
                CONST_VERSION, indexResponse.getVersion());
        sendOK(message, reply);
      }
    });
  }

  private void doScroll(final Message<JsonObject> message) {
    JsonObject body = message.body();
    String scrollId = body.getString("_scroll_id");
    if (scrollId == null) {
      sendError(logger, message, "_scroll_id is required", null);
      return;
    }
    String scroll = body.getString("scroll");
    if (scroll == null) {
      sendError(logger, message, "scroll is required", null);
      return;
    }

    client.prepareSearchScroll(scrollId).setScroll(scroll).execute(
        new ActionListener<SearchResponse>() {
          @Override
          public void onFailure(Throwable e) {
            sendError(logger, message, "Scroll error: " + e.getMessage(), new RuntimeException(e));
          }

          @Override
          public void onResponse(SearchResponse searchResponse) {
            handleActionResponse(logger, searchResponse, message);
          }
        });
  }
}
