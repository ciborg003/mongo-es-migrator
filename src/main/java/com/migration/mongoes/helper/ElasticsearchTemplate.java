package com.migration.mongoes.helper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.TimeoutRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URL;
import java.sql.Time;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.search.sort.SortOrder.DESC;

public class ElasticsearchTemplate {


    public static class NativeSearchQueryBuilder {
        private final SearchSourceBuilder searchSourceBuilder;
        private String indexName;
        private String type;
        private String routing;

        public NativeSearchQueryBuilder() {
            this.searchSourceBuilder = new SearchSourceBuilder();
        }

        public NativeSearchQueryBuilder withIndices(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public NativeSearchQueryBuilder withTypes(String type) {
            this.type = type;
            return this;
        }

        public NativeSearchQueryBuilder withRouting(String routing) {
            this.routing = routing;
            return this;
        }

        public NativeSearchQueryBuilder withQuery(QueryBuilder queryBuilder) {
            searchSourceBuilder.query(queryBuilder);
            return this;
        }

        public NativeSearchQueryBuilder withFilter(QueryBuilder queryBuilder) {
            searchSourceBuilder.postFilter(queryBuilder);
            return this;
        }

        public NativeSearchQueryBuilder highlighter(HighlightBuilder highlightBuilder) {
            searchSourceBuilder.highlighter(highlightBuilder);
            return this;
        }

        public NativeSearchQueryBuilder withFields(String... fields) {
            searchSourceBuilder.fetchSource(fields, null);
            return this;
        }

        public NativeSearchQueryBuilder withPageable(Pageable pageable) {
            searchSourceBuilder.from((int) pageable.getOffset());
            searchSourceBuilder.size(pageable.getPageSize());
            if (pageable.getSort() != null) {
                Sort.Order o = pageable.getSort().iterator().next();
                searchSourceBuilder.sort(o.getProperty(), o.isAscending() ? ASC : DESC);
            }
            return this;
        }

        public NativeSearchQueryBuilder addAggregation(AggregationBuilder aggregation) {
            searchSourceBuilder.aggregation(aggregation);
            return this;
        }


        public SearchRequest build() {
            return new SearchRequest().indices(indexName).types(type).routing(routing).source(searchSourceBuilder);
        }
    }

    public static class IndexQueryBuilder {
        private String indexName;
        private String type;
        private String routing;
        private String id;
        private Object doc;

        public IndexQueryBuilder withIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public IndexQueryBuilder withType(String type) {
            this.type = type;
            return this;
        }

        public IndexQueryBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public IndexQueryBuilder withObject(Object document) {
            this.doc = document;
            return this;
        }
        public IndexQueryBuilder withRouting(String routing) {
            this.routing = routing;
            return this;
        }
        public IndexQuery build() {
            return new IndexQuery(indexName, type, id, doc, routing);
        }


    }

    public static class IndexQuery {
        private final String indexName;
        private final String type;
        private final String id;
        private final Object doc;
        private final String routing;

        public IndexQuery(String indexName, String type, String id, Object doc, String routing) {
            this.indexName = indexName;
            this.type = type;
            this.id = id;
            this.doc = doc;
            this.routing = routing;
        }
    }

    public static class UpdateQueryBuilder {
        private String indexName;
        private String type;
        private String id;
        private UpdateRequest request;
        private String routing;

        public UpdateQueryBuilder withIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public UpdateQueryBuilder withType(String type) {
            this.type = type;
            return this;
        }

        public UpdateQueryBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public UpdateQueryBuilder withUpdateRequest(UpdateRequest req) {
            this.request = req;
            return this;
        }

        public UpdateQueryBuilder withRouting(String routing) {
            this.routing = routing;
            return this;
        }
        public UpdateQuery build() {
            return new UpdateQuery(indexName, type, id, request, routing);
        }
    }

    public static class UpdateQuery {
        private final UpdateRequest request;

        public UpdateQuery(String indexName, String type, String id, UpdateRequest request, String routing) {
            this.request = request.index(indexName).type(type).routing(routing).id(id);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchTemplate.class);

    private static int DEFAULT_NUMBER_OF_SHARDS = 5;
    private static int DEFAULT_NUMBER_OF_REPLICAS = 2;
    private static final String TEXT_GENERAL_ANALYZER = "text_general";
    private static ObjectMapper objectMapper = new ObjectMapper();
    private RetryTemplate retryTemplate = new RetryTemplate();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final RestHighLevelClient client;

    public ElasticsearchTemplate(RestHighLevelClient client) {
        this.client = client;
    }

    public ElasticsearchTemplate(String clusterNodes) {
        String[] addrs = clusterNodes.split("\\s+|;|,");
        this.client = new RestHighLevelClient(
                RestClient.builder(Stream.of(addrs).map(a -> {
                    if (!a.contains("://")) {
                        a = "http://" + a;
                    }
                    try {
                        URL url = new URL(a);
                        return new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).toArray(HttpHost[]::new)).setMaxRetryTimeoutMillis((int) TimeUnit.HOURS.toMillis(1)));
    }


    @PostConstruct
    public void init() {
        TimeoutRetryPolicy policy = new TimeoutRetryPolicy();
        policy.setTimeout(TimeUnit.HOURS.toMillis(1));
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(TimeUnit.SECONDS.toMillis(5));
        retryTemplate.setRetryPolicy(policy);
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
    }

    @PreDestroy
    public void destroy() {
        executorService.shutdown();
    }


    public RestHighLevelClient getClient() {
        return client;
    }

//    public void createIndexWithAliasesAndMapping(String indexName, List<String> aliases, String type, Class<?> tClass, Integer shards, Integer replicas) {
//        Assert.notNull(indexName, "No index defined for Query");
//        CreateIndexRequest ci = new CreateIndexRequest().index(indexName);
//        if (!CollectionUtils.isEmpty(aliases)) {
//            aliases.stream().map(Alias::new).forEach(ci::alias);
//        }
//        if (Arrays.stream(tClass.getDeclaredFields()).anyMatch(field -> field.isAnnotationPresent(Field.class))) {
//            try {
//                Map<String, Object> fields = new LinkedHashMap<>();
//                for (java.lang.reflect.Field rf : tClass.getDeclaredFields()) {
//                    if (rf.isAnnotationPresent(Field.class)) {
//                        writeField(fields, rf);
//                    }
//                }
//
//                Map<String, Object> properties = fields.entrySet().stream().filter(me -> {
//                    int nestedIx = me.getKey().indexOf('.');
//                    //skip explicit mapping of nested field, as it is not processed by elastic: the mapping will be created automatically, when document is created
//                    return (nestedIx == -1 || !fields.containsKey(me.getKey().substring(0, nestedIx)));
//                }).collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue()));
//
//                XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
//                        .startObject()
//                        .field("properties", properties)
//                        .endObject();
//                ci.mapping(type, xContentBuilder);
//
//                if (shards == null){
//                    shards = DEFAULT_NUMBER_OF_SHARDS;
//                }
//                if (replicas == null) {
//                    replicas = DEFAULT_NUMBER_OF_REPLICAS;
//                }
//                XContentBuilder b = XContentFactory.jsonBuilder().startObject()
//                        .field("number_of_shards", shards)
//                        .field("number_of_replicas", replicas);
//                addCustomAnalysis(b);
//                b.endObject();
//                ci.settings(b);
//
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        try {
//            client.indices().create(ci);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    private void addCustomAnalysis(XContentBuilder builder) throws IOException {
        builder.startObject("analysis")
                .startObject("analyzer")
                .startObject(TEXT_GENERAL_ANALYZER)
                .field("type", "custom")
                .field("tokenizer", "whitespace")
                .field("filter", Arrays.asList("lowercase", "custom_word_delimiter"))
                .endObject()
                .endObject()
                .startObject("filter")
                .startObject( "custom_word_delimiter")
                .field("type", "word_delimiter")
                .field("generate_word_parts", true)
                .field("generate_number_parts", true)
                .field("catenate_words",true)
                .field("catenate_numbers", true)
                .field("split_on_case_change", true)
                .field("preserve_original", true)
                .endObject()
                .endObject()
                .endObject();
    }


//    private String resolveFieldType(java.lang.reflect.Field rf) {
//        Field f = rf.getAnnotation(Field.class);
//        String type = f == null ? null : f.type();
//        if (StringUtils.isNotBlank(type)) {
//            return type;
//        }
//        if (rf.getType() == String.class) {
//            return "text";
//        }
//        if (rf.getType() == Date.class) {
//            return "date";
//        }
//        return "keyword";
//    }

//    private void writeField(Map<String, Object> fields, java.lang.reflect.Field rf) throws IOException {
//        Field f = rf.getAnnotation(Field.class);
//        String fieldName = StringUtils.isNotBlank(f.value()) ? f.value() : rf.getName();
//        String fieldType = resolveFieldType(rf);
//
//        Map<String, Object> props = new HashMap<>();
//        fields.put(fieldName, props);
//        props.put("type", fieldType);
//        props.put("store", true);
//        if (f.copyTo().length > 0) {
//            props.put("copy_to", f.copyTo());
//            for(String cp: f.copyTo()) {
//                if (!fields.containsKey(cp)) {
//                    Map<String, Object> cp_props = new HashMap<>();
//                    cp_props.put("type", "text");
//                    cp_props.put("analyzer", TEXT_GENERAL_ANALYZER);
//                    cp_props.put("store", true);
//                    fields.put(cp, cp_props);
//                }
//            }
//        }
//        if (f.eagerGlobalOrdinals()) {
//            props.put("eager_global_ordinals", true);
//        }
//    }

    public boolean deleteIndex(String...indexNames) {
        Assert.notNull(indexNames, "No index defined for Query");
        try {
            client.indices().delete(new DeleteIndexRequest().indices(indexNames));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Autowired
    private Environment environment;
    public String[] getExistingIndies(String indexName) {
        try {
            Map<String, Object> map = new ObjectMapper().readValue(client.getLowLevelClient()
                    .performRequest("GET", "/"+indexName).getEntity().getContent(), Map.class);
            return map.keySet().stream().toArray(String[]::new);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public <T> T query(SearchRequest query, java.util.function.Function<SearchResponse, T> resultsExtractor) {
        try {
            return resultsExtractor.apply(client.search(query));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    public <T> Page<T> queryForPage(SearchRequest query, Class<T> tClass) {
//        SearchResponse response = null;
//        try {
//            response = client.search(query);
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        List<T> docs = Arrays.stream(response.getHits().getHits())
//                .map(sh -> SearchUtils.toObject(sh.getSourceRef(), tClass)).collect(Collectors.toList());
//        int size = query.source().size() == -1 ? SearchUtils.DEFAULT_ROWS_SIZE : query.source().size();
//        int page = query.source().from() == -1 ? 0 : query.source().from() / size;
//        Sort s = null;
//        if (!CollectionUtils.isEmpty(query.source().sorts())) {
//            FieldSortBuilder sb = (FieldSortBuilder) query.source().sorts().get(0);
//            s = new Sort(sb.order() == ASC ? Sort.Direction.ASC : Sort.Direction.DESC, sb.getFieldName());
//        }
//        return new PageImpl(docs, new PageRequest(page, size, s), response.getHits().getTotalHits());
//    }

//    public <T> T getById(GetRequest req, Class<T> tClass) {
//        try {
//            return SearchUtils.toObject(client.get(req).getSourceAsBytesRef(), tClass);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    private IndexRequest prepareIndex(IndexQuery query) {
        IndexRequest req = new IndexRequest();
        try {
            req.index(query.indexName).routing(query.routing).type(query.type).id(query.id)
                    .source(objectMapper.writeValueAsString(query.doc), XContentType.JSON);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return req;
    }

    public void index(IndexQuery query) {
        try {
            client.index(prepareIndex(query));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void refresh(String indexName) {
        try {
            client.getLowLevelClient().performRequest("POST", "/" + indexName + "/_refresh");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void bulkIndex(List<IndexQuery> queries) {
        BulkRequest bulkRequest = new BulkRequest();
        for (IndexQuery query : queries) {
            bulkRequest.add(prepareIndex(query));
        }
        try {
            checkForBulkUpdateFailure(client.bulk(bulkRequest));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void checkForBulkUpdateFailure(BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            Map<String, String> failedDocuments = new HashMap<>();
            for (BulkItemResponse item : bulkResponse.getItems()) {
                if (item.isFailed())
                    failedDocuments.put(item.getId(), item.getFailureMessage());
            }
            throw new RuntimeException(
                    "Bulk indexing has failures. Use ElasticsearchException.getFailedDocuments() for detailed messages ["
                            + failedDocuments + "]");
        }
    }


    public UpdateResponse update(UpdateQuery query) {
        try {
            return client.update(query.request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void bulkUpdate(List<UpdateQuery> queries) {
        BulkRequest bulkRequest = new BulkRequest();
        for (UpdateQuery query : queries) {
            bulkRequest.add(query.request);
        }
        try {
            checkForBulkUpdateFailure(client.bulk(bulkRequest));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void delete(DeleteRequest request) {
        try {
            client.delete(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    public String reindex(String source, String dest, String versionType, ESRoutingResolver routingResolver, Integer batchSize, Runnable onSuccess) {
//        try {
//            XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
//                    .field("conflicts","proceed");
//
//            builder.startObject("source").field("index", source);
//            if (batchSize != null) {
//                builder.field("size", batchSize);
//            }
//            builder.endObject();
//            builder.startObject("dest").field("index", dest);
//
//            if (versionType != null) {
//                builder.field("version_type", versionType);
//            }
//            builder.endObject();
//            if (routingResolver.getWriteScript() != null) {
//                builder.startObject("script")
//                        .field("source", routingResolver.getWriteScript())
//                        .field("lang", "painless")
//                        .endObject();
//            }
//
//            builder.endObject();
//            HashMap<String, String> params = new HashMap<>(1);
//            params.put("wait_for_completion", "false");
//            Response resp = client.getLowLevelClient().performRequest("POST", "/_reindex",
//                    params,
//                    new StringEntity(builder.string(), APPLICATION_JSON));
//            if (resp.getStatusLine().getStatusCode() != HttpStatus.OK.value()) {
//                throw new RuntimeException("Error during reindex: " + resp.getStatusLine());
//            }
//            String taskId = (String) objectMapper.readValue(resp.getEntity().getContent(), Map.class).get("task");
//            Assert.notNull(taskId);
//            executorService.execute(() -> {
//                Map<String, Object> result = retryTemplate.execute(context -> {
//                    Response r = null;
//                    try {
//                        r = client.getLowLevelClient().performRequest("GET", "/_tasks/"+taskId);
//                        Map<String, Object> res = objectMapper.readValue(r.getEntity().getContent(), Map.class);
//                        if ((Boolean) res.get("completed")) {
//                            context.setExhaustedOnly();
//                            return res;
//                        }
//                        throw new RuntimeException("Repeat");
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
//                if (!CollectionUtils.isEmpty((List)result.get("failures"))) {
//                    logger.error("Reindex was finished with errors: " + result.get("failures"));
//                }
//                else {
//                    onSuccess.run();
//                }
//            });
//            return taskId;
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

//    public void switchAlias(String alias, String oldIndexName, String newIndexName) {
//        try {
//            XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
//                    .startArray("actions")
//                    .startObject()
//                    .startObject("remove")
//                    .field("index", oldIndexName)
//                    .field("alias", alias)
//                    .endObject()
//                    .endObject()
//                    .startObject()
//                    .startObject("add")
//                    .field("index", newIndexName)
//                    .field("alias", alias)
//                    .endObject()
//                    .endObject()
//                    .endArray()
//                    .endObject();
//            client.getLowLevelClient().performRequest("POST", "/_aliases", Collections.emptyMap(),
//                    new StringEntity(builder.string(), APPLICATION_JSON));
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

}