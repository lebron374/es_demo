package com.zhi;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by lebron374 on 2017/6/15.
 */



public class ESClient {

    private TransportClient client = null;

    public void init() {
        try {
            this.client = TransportClient.builder().build().
                    addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        this.client.close();
    }

    // 创建索引
    public void createIndex(String index) {
        CreateIndexResponse response = client.admin().indices().prepareCreate(index).get();
        System.out.println(JSON.toJSONString(response.isAcknowledged()));

    }

    // 删除索引
    public void delIndex(String... indices) {
        DeleteIndexResponse response = client.admin().indices().prepareDelete(indices).get();
        System.out.println(response.isAcknowledged());
    }

    // 添加mapping
    public void addMapping(String type, XContentBuilder source, String... indices) {
        PutMappingResponse response = client.admin().indices().
                preparePutMapping(indices).setType(type).
                setSource(source).get();

        System.out.println(JSON.toJSONString(response.isAcknowledged()));
    }

    // 更新mapping
    public void updateMapping(String type, XContentBuilder source, String... indices) {
        PutMappingResponse response = client.admin().indices().preparePutMapping(indices).
                setType(type).setSource(source).get();

        System.out.println(JSON.toJSONString(response));
    }

    // 创建别名
    public void addAlias(String indices, String alias) {
        IndicesAliasesResponse response = client.admin().indices().prepareAliases().
                addAlias(indices, alias).get();

        System.out.println(JSON.toJSONString(response));
    }

    // 删除别名
    public void delAlias(String indices, String alias) {
        IndicesAliasesResponse response = client.admin().indices().prepareAliases().
                removeAlias(indices, alias).get();

        System.out.println(JSON.toJSONString(response.isAcknowledged()));
    }

    // 添加文档
    public void addDocument(String indices, String type, String id, byte[] source) {
        IndexResponse indexResponse = client.prepareIndex().setIndex(indices).setSource(source).
                setId(id).setType(type).get();

        System.out.println(indexResponse.isCreated());
    }

    // 获取文档
    public void getDocument(String indices, String type, String id) {
        GetResponse response = client.prepareGet().setIndex(indices).setType(type).setId(id).get();
        System.out.println(JSON.toJSONString(response.getSource()));
    }

    // 删除文档
    public void delDocument(String indices, String type, String id) {
        DeleteResponse response = client.prepareDelete().setIndex(indices).setType(type).setId(id).get();
        System.out.println(response.isFound());
    }

    // 批量添加
    public void batchAddDocument(List<String> sourceList, String indices, String type) {
        List<IndexRequestBuilder> builderList = new ArrayList<IndexRequestBuilder>();

        for (String source : sourceList) {
            builderList.add(client.prepareIndex(indices, type).setSource(source));
        }

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (IndexRequestBuilder builder:builderList) {
            bulkRequestBuilder = bulkRequestBuilder.add(builder);
        }

        BulkResponse response = bulkRequestBuilder.get();

        for (Iterator<BulkItemResponse> iter = response.iterator(); iter.hasNext();) {
            System.out.println(iter.next().getResponse());
        }
    }

    // 批量查询
    public void batchGetDocument(String indices, String type, List<String> ids) {
        MultiGetRequestBuilder builder = client.prepareMultiGet().add(indices, type, ids);
        MultiGetResponse response = builder.get();
        for (Iterator<MultiGetItemResponse> iter = response.iterator(); iter.hasNext();) {
            System.out.println(JSON.toJSONString(iter.next().getResponse().getSource()));
        }
    }

    // 简单查询
    public void simpleMatchQuery() {
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("answer","answer_2 answer_1");
        SearchResponse response = client.prepareSearch("demo_alias").
                setTypes("person").setQuery(queryBuilder).get();
        SearchHits hits = response.getHits();
        for (Iterator<SearchHit> hit = hits.iterator(); hit.hasNext(); ) {
            SearchHit searchHit = hit.next();
            System.out.println(searchHit.getSource().toString());
        }
    }

    // 前缀查询
    public void simplePrefixQuery() {
        PrefixQueryBuilder queryBuilder = QueryBuilders.prefixQuery("category", "category");
        SearchResponse response = client.prepareSearch().setIndices("demo_alias").setTypes("person").
                setQuery(queryBuilder).get();
        SearchHits hits = response.getHits();
        for (Iterator<SearchHit> hit = hits.iterator(); hit.hasNext(); ) {
            SearchHit searchHit = hit.next();
            System.out.println(searchHit.getSource().toString());
        }
    }

    // 多匹配查询
    public void multiMatchQuery() {
        MultiMatchQueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("pTitle", "title");
        SearchResponse response = client.prepareSearch().setIndices("demo_alias").setTypes("person")
                .setQuery(queryBuilder).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource());
        }
    }

    // 模糊查询
    public void fuzzyQuery() {
        FuzzyQueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("title", "title_*");
        SearchResponse response = client.prepareSearch().setIndices("demo_alias").setTypes("person")
                .setQuery(queryBuilder).setSize(10).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource());
        }
    }

    // bool查询
    public void boolQuery() {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.matchQuery("answer", "answer_2"))
                .should(QueryBuilders.matchQuery("title", "title_1"));
        SearchResponse response = client.prepareSearch().setIndices("demo_alias").setTypes("person")
                .setQuery(queryBuilder).setSize(8).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource());
        }
    }

    // simpleQueryStringQuery
    public void simpleQueryStringQuery() {
        SimpleQueryStringBuilder queryStringBuilder = QueryBuilders.simpleQueryStringQuery("+title -title_*").field("title");
        SearchResponse response = client.prepareSearch().setIndices("demo_alias")
                .setTypes("person").setQuery(queryStringBuilder).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource());
        }
    }

    // terms query
    public void termsQuery() {
        TermsQueryBuilder queryBuilder = QueryBuilders.termsQuery("title", Arrays.asList(new String[] {"title_1", "title_2"}));
        SearchResponse response = client.prepareSearch().setIndices("demo_alias")
                .setTypes("person").setQuery(queryBuilder).get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource());
        }
    }

    // term query
    public void termQuery() {
        TermQueryBuilder queryBuilder = QueryBuilders.termQuery("title", "title_1");
        SearchResponse response = client.prepareSearch().setIndices("demo_alias").setTypes("person")
                .setQuery(queryBuilder).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource());
        }
    }

    // range query
    public void rangeQuery() {
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery("title").from("title_0").to("title_1");
        SearchResponse response = client.prepareSearch().setIndices("demo_alias").setTypes("person")
                .setQuery(queryBuilder).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource());
        }
    }

    // 翻页查询 dismax query 对子查询的结果做union
    public void pageQuery() {
        DisMaxQueryBuilder queryBuilder = QueryBuilders.disMaxQuery().add(QueryBuilders.rangeQuery("title").from("title_0").to("title_0"))
                .add(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("author", "author_1")));

        SearchResponse response = client.prepareSearch().setIndices("demo_alias").setTypes("person")
                .setQuery(queryBuilder).setFrom(0).setSize(5).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSource());
        }
    }

    public static List<String> buildPerson(int length) {
        List<String> personList = new ArrayList<String>();
        for (int i=0; i<length; i++) {
            Person person = new Person();
            person.setQuestion("person_" + i);
            person.setCategory("category_" + i);
            person.setAuthor("author_" + i);
            person.setAnswer("answer_" + i);
            person.setTitle("title_" + i);
            personList.add(JSON.toJSONString(person));
        }

        return personList;
    }
    public static void main(String[] args) throws Exception {

        String indices = "demo";
        String alias = "demo_alias";
        String type = "person";
        String id = "key1";
        String sid = "key2";
        XContentBuilder mapping;
        XContentBuilder updateMapping;
        mapping = XContentFactory.jsonBuilder().startObject().
                startObject("_all").field("enabled", "false").
                endObject().
                startObject("properties").
                startObject("title").field("type", "string").endObject().
                startObject("question").field("type", "string").endObject().
                startObject("answer").field("type", "string").endObject().
                startObject("category").field("type", "string").endObject().
                startObject("author").field("type", "string").endObject().
                endObject().endObject();

        updateMapping = XContentFactory.jsonBuilder().startObject().startObject("properties").
                startObject("description").field("type", "string").endObject().
                endObject().endObject();

        Person person = new Person();
        person.setTitle("pTitle");
        person.setAnswer("pAnswer");
        person.setAuthor("pAuthor");
        person.setCategory("pCategory");
        person.setQuestion("pQuestion");
        byte[] json = JSON.toJSONBytes(person);

        Person sperson = new Person();
        sperson.setTitle("pTitle second");
        sperson.setAnswer("pAnswer second");
        sperson.setAuthor("pAuthor second");
        sperson.setCategory("pCategory second");
        sperson.setQuestion("pQuestion second");
        byte[] sjson = JSON.toJSONBytes(sperson);

        List<String> personList = buildPerson(3);

        ESClient esClient = new ESClient();
        esClient.init();
//        esClient.delAlias(indices, alias);
//        esClient.delIndex(indices);
//        esClient.delDocument(indices, type, id);
//        esClient.addDocument(indices, type, id, json);
//        esClient.getDocument(indices, type, id);
//        esClient.createIndex(indices);
//        esClient.addMapping(type, mapping, indices);
//        esClient.addDocument(indices, type, sid, sjson);
//        esClient.batchAddDocument(personList, indices, type);
//        esClient.batchGetDocument(indices, type, Arrays.asList(new String[] {"AVzUMm9mEyWIYXCCQu5Z","AVzUM7TmEyWIYXCCQu5a"}));
//        esClient.addAlias(indices, alias);
//        esClient.simpleMatchQuery();
//        esClient.simplePrefixQuery();
//        esClient.multiMatchQuery();
//        esClient.fuzzyQuery();
//        esClient.boolQuery();
//        esClient.simpleQueryStringQuery();
//        esClient.termsQuery();
//        esClient.termQuery();
//        esClient.rangeQuery();
        esClient.pageQuery();
        esClient.close();
    }
}
