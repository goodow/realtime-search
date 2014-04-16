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
  public static final String INDEX = "_index";
  public static final String TYPE = "_type";
  public static final String ID = "_id";
  public static final String VERSION = "_version";
  public static final String SOURCE = "_source";

  public static void replyFail(Logger logger, Message<JsonObject> message, String error, Throwable e) {
    logger.error(error, e);
    message.fail(-1, error);
  }

  static void handleActionResponse(Logger logger, ToXContent toXContent, Message<JsonObject> message) {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      toXContent.toXContent(builder, SearchResponse.EMPTY_PARAMS);
      builder.endObject();

      JsonObject response = new JsonObject(builder.string());
      message.reply(response);
    } catch (IOException e) {
      replyFail(logger, message, "Error reading search response: " + e.getMessage(), e);
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
        replyFail(logger, message, "action must be specified", null);
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
          replyFail(logger, message, "Unrecognized action " + action, null);
          break;
      }
    } catch (Exception e) {
      replyFail(logger, message, "Unhandled exception!", e);
    }
  }

  String getRequiredIndex(JsonObject json, Message<JsonObject> message) {
    String index = json.getString(INDEX);
    if (index == null || index.isEmpty()) {
      replyFail(logger, message, INDEX + " is required", null);
      return null;
    }
    return index;
  }

  String getRequiredType(JsonObject json, Message<JsonObject> message) {
    String type = json.getString(TYPE);
    if (type == null || type.isEmpty()) {
      replyFail(logger, message, TYPE + " is required", null);
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
    String id = body.getString(ID);
    if (id == null) {
      replyFail(logger, message, ID + " is required", null);
      return;
    }
    client.prepareGet(index, type, id).execute(new ActionListener<GetResponse>() {
      @Override
      public void onFailure(Throwable e) {
        replyFail(logger, message, "Get error: " + e.getMessage(), e);
      }

      @Override
      public void onResponse(GetResponse getFields) {
        JsonObject source =
            (getFields.isExists() ? new JsonObject(getFields.getSourceAsString()) : null);
        JsonObject reply =
            new JsonObject().putString(INDEX, getFields.getIndex()).putString(TYPE,
                getFields.getType()).putString(ID, getFields.getId()).putNumber(VERSION,
                getFields.getVersion()).putObject(SOURCE, source);
        message.reply(reply);
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
    JsonObject source = body.getObject(SOURCE);
    if (source == null) {
      replyFail(logger, message, SOURCE + " is required", null);
      return;
    }

    IndexRequestBuilder builder =
        client.prepareIndex(index, type, body.getString(ID)).setSource(source.encode());

    if (body.containsField("version")) {
      builder.setVersion(body.getLong("version"));
    }
    if (body.containsField("version_type")) {
      builder.setVersionType(VersionType.fromString(body.getString("version_type")));
    }
    if (body.containsField("op_type")) {
      builder.setOpType(body.getString("op_type"));
    }

    builder.execute(new ActionListener<IndexResponse>() {
      @Override
      public void onFailure(Throwable e) {
        replyFail(logger, message, "Index error: " + e.getMessage(), e);
      }

      @Override
      public void onResponse(IndexResponse indexResponse) {
        JsonObject reply =
            new JsonObject().putString(INDEX, indexResponse.getIndex()).putString(TYPE,
                indexResponse.getType()).putString(ID, indexResponse.getId()).putNumber(VERSION,
                indexResponse.getVersion());
        message.reply(reply);
      }
    });
  }

  private void doScroll(final Message<JsonObject> message) {
    JsonObject body = message.body();
    String scrollId = body.getString("_scroll_id");
    if (scrollId == null) {
      replyFail(logger, message, "_scroll_id is required", null);
      return;
    }
    String scroll = body.getString("scroll");
    if (scroll == null) {
      replyFail(logger, message, "scroll is required", null);
      return;
    }

    client.prepareSearchScroll(scrollId).setScroll(scroll).execute(
        new ActionListener<SearchResponse>() {
          @Override
          public void onFailure(Throwable e) {
            replyFail(logger, message, "Scroll error: " + e.getMessage(), e);
          }

          @Override
          public void onResponse(SearchResponse searchResponse) {
            handleActionResponse(logger, searchResponse, message);
          }
        });
  }
}
