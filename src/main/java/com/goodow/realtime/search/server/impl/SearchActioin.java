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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

public class SearchActioin implements Handler<Message<JsonObject>> {
  private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
  private final Logger logger;
  @Inject private Client client;

  @Inject
  SearchActioin(Container container) {
    logger = container.logger();
  }

  @Override
  public void handle(final Message<JsonObject> message) {
    JsonObject body = message.body();
    // Get indices to be searched
    String index = body.getString(ElasticSearchHandler.CONST_INDEX);
    JsonArray indices = body.getArray("_indices");
    List<String> list = new ArrayList<>();
    if (index != null) {
      list.add(index);
    }
    if (indices != null) {
      for (Object idx : indices) {
        list.add((String) idx);
      }
    }

    SearchRequestBuilder builder = client.prepareSearch(list.toArray(new String[list.size()]));
    // Get types to be searched
    String type = body.getString(ElasticSearchHandler.CONST_TYPE);
    JsonArray types = body.getArray("_types");
    list.clear();
    if (type != null) {
      list.add(type);
    }
    if (types != null) {
      for (Object tp : types) {
        list.add((String) tp);
      }
    }
    if (!list.isEmpty()) {
      builder.setTypes(list.toArray(new String[list.size()]));
    }

    // Set the query
    JsonObject query = body.getObject("query");
    if (query != null) {
      builder.setQuery(query.encode());
    }

    // Set the filter
    JsonObject filter = body.getObject("filter");
    if (filter != null) {
      builder.setPostFilter(filter.encode());
    }

    // Set facets
    JsonObject facets = body.getObject("facets");
    if (facets != null) {
      builder.setFacets(facets.encode().getBytes(CHARSET_UTF8));
    }

    // Set search type
    String searchType = body.getString("search_type");
    if (searchType != null) {
      builder.setSearchType(searchType);
    }

    // Set scroll keep alive time
    String scroll = body.getString("scroll");
    if (scroll != null) {
      builder.setScroll(scroll);
    }

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
