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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Container;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

public class SearchActioin implements Handler<Message<JsonObject>> {
  private final Logger logger;
  @Inject private Client client;

  @Inject
  SearchActioin(Container container) {
    logger = container.logger();
  }

  @Override
  public void handle(final Message<JsonObject> message) {
    JsonObject body = message.body();
    body.removeField("action");
    // Get indices to be searched
    String index = body.getString(ElasticSearchHandler.CONST_INDEX);
    JsonArray indices = body.getArray("_indices");
    List<String> list = new ArrayList<>();
    if (index != null) {
      list.add(index);
      body.removeField(ElasticSearchHandler.CONST_INDEX);
    }
    if (indices != null) {
      for (Object idx : indices) {
        list.add((String) idx);
      }
      body.removeField("_indices");
    }
    SearchRequestBuilder builder = client.prepareSearch(list.toArray(new String[list.size()]));

    // Get types to be searched
    String type = body.getString(ElasticSearchHandler.CONST_TYPE);
    JsonArray types = body.getArray("_types");
    list.clear();
    if (type != null) {
      list.add(type);
      body.removeField(ElasticSearchHandler.CONST_TYPE);
    }
    if (types != null) {
      for (Object tp : types) {
        list.add((String) tp);
      }
      body.removeField("_types");
    }
    if (!list.isEmpty()) {
      builder.setTypes(list.toArray(new String[list.size()]));
    }

    // Set search type
    String searchType = body.getString("search_type");
    if (searchType != null) {
      builder.setSearchType(searchType);
      body.removeField("search_type");
    }

    // Set scroll keep alive time
    String scroll = body.getString("scroll");
    if (scroll != null) {
      builder.setScroll(scroll);
      body.removeField("scroll");
    }

    builder.setExtraSource(body.encode());
    builder.execute(new ActionListener<SearchResponse>() {
      @Override
      public void onFailure(Throwable e) {
        ElasticSearchHandler.sendError(logger, message, "Search error: " + e.getMessage(),
            new RuntimeException(e));
      }

      @Override
      public void onResponse(SearchResponse searchResponse) {
        ElasticSearchHandler.handleActionResponse(logger, searchResponse, message);
      }
    });
  }
}
