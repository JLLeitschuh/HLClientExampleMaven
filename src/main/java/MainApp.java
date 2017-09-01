import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class MainApp {

    public static void main(String[] args) throws IOException {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        try (RestHighLevelClient hlClient = new RestHighLevelClient(restClientBuilder)) {
            MainResponse info = hlClient.info();
            System.out.println(Strings.toString(info));

            try {
                hlClient.getLowLevelClient().performRequest("PUT", "/test");
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() == 400 &&
                        (e.getMessage().contains("resource_already_exists_exception"))) {
                    System.out.println("index already exists. Ignoring error...");
                } else {
                    throw e;
                }
            }

            IndexRequest indexRequest = new IndexRequest("test", "doc", "1");
            indexRequest.source("title", "Getting started with Elasticsearch", "author" , "John Doe");
            IndexResponse indexResponse = hlClient.index(indexRequest);
            if (indexResponse.getResult() != Result.CREATED && indexResponse.getResult() != Result.UPDATED) {
                System.out.println(Strings.toString(indexResponse));
            }

            indexRequest = new IndexRequest("test", "doc", "2");
            indexRequest.source("title", "How search makes the difference", "author" , "Jane Doe");
            indexResponse = hlClient.index(indexRequest);
            if (indexResponse.getResult() != Result.CREATED && indexResponse.getResult() != Result.UPDATED) {
                System.out.println(Strings.toString(indexResponse));
            }

            SearchRequest searchRequest = new SearchRequest("test");
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = hlClient.search(searchRequest);

            System.out.println(Strings.toString(searchResponse));
        }
    }
}
