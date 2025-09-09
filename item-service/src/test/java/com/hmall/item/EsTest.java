package com.hmall.item;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmall.common.utils.CollUtils;
import com.hmall.item.domain.dto.ItemDoc;
import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest(properties = "spring.profiles.active=local")
public class EsTest {

    private RestHighLevelClient client;

    @Autowired
    private IItemService itemService;
    @Test
    void testConnection() {
        System.out.println("client = " + client);
    }

    @BeforeEach
    void setUp() {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://192.168.63.128:9200")));
    }

    @AfterEach
    void tearDown() throws IOException {
        if(client != null){
            client.close();
        }
    }

    @Test
    void testCreateIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("items");
        request.source("{\n" +
                "  \"mappings\":{\n" +
                "    \"properties\":{\n" +
                "      \"id\":{\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"name\":{\n" +
                "        \"type\":\"text\",\n" +
                "        \"analyzer\":\"ik_smart\"\n" +
                "      },\n" +
                "      \"price\":{\n" +
                "        \"type\":\"integer\"\n" +
                "      },\n" +
                "      \"image\":{\n" +
                "        \"type\": \"keyword\",\n" +
                "        \"index\": false\n" +
                "      },\n" +
                "      \"category\":{\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"brand\":{\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"sold\":{\n" +
                "        \"type\": \"integer\"\n" +
                "      },\n" +
                "      \"commentCount\":{\n" +
                "        \"type\": \"integer\",\n" +
                "        \"index\": false\n" +
                "      },\n" +
                "      \"isAD\":{\n" +
                "        \"type\": \"boolean\"\n" +
                "      },\n" +
                "      \"updateTime\":{\n" +
                "        \"type\": \"date\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", XContentType.JSON);
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testGetIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("items");

        // GetIndexResponse getIndexResponse = client.indices().get(request, RequestOptions.DEFAULT);
        boolean isExisted = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(isExisted);
    }

    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("items");
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testAddDocument() throws IOException {
        // 1.根据id查询商品数据
        Item item = itemService.getById(100002644680L);
        // 2.转换为文档类型
        ItemDoc itemDoc = BeanUtil.copyProperties(item, ItemDoc.class);

        // 3.将ItemDTO转json
        String doc = JSONUtil.toJsonStr(itemDoc);

        // 1.准备Request对象
        IndexRequest request = new IndexRequest("items").id(itemDoc.getId());
        // 2.准备Json文档
        request.source(doc, XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }
    @Test
    void testGetDocumentById() throws IOException {
        // 1.准备Request对象
        GetRequest request = new GetRequest("items").id("100002644680");
        // 2.发送请求
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3.获取响应结果中的source
        String json = response.getSourceAsString();

        ItemDoc itemDoc = JSONUtil.toBean(json, ItemDoc.class);
        System.out.println("itemDoc= " + itemDoc);
    }
    @Test
    void testDeleteDocument() throws IOException {
        // 1.准备Request，两个参数，第一个是索引库名，第二个是文档id
        DeleteRequest request = new DeleteRequest("items", "100002644680");
        // 2.发送请求
        client.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testUpdateDocument() throws IOException {
        // 1.准备Request
        UpdateRequest request = new UpdateRequest("items", "100002644680");
        // 2.准备请求参数
        request.doc(
                "price", 58800,
                "commentCount", 1
        );
        // 3.发送请求
        client.update(request, RequestOptions.DEFAULT);
    }


    /**
     * 批量插入
     * @throws IOException
     */
    @Test
    void testBulk() throws IOException {
        // 1.创建Request
        BulkRequest request = new BulkRequest();
        // 2.准备请求参数
        request.add(new IndexRequest("items").id("1").source("json doc1", XContentType.JSON));
        request.add(new IndexRequest("items").id("2").source("json doc2", XContentType.JSON));
        // 3.发送请求
        client.bulk(request, RequestOptions.DEFAULT);
    }

    @Test
    void testBulkInsert() throws IOException {
        // 分页查询商品数据
        int pageNo = 1;
        int size = 1000;
        while (true) {

            Page<Item> pages = itemService.lambdaQuery().eq(Item::getStatus, 1).page(Page.of(pageNo, size));
            List<Item> items = pages.getRecords();
            BulkRequest bulkRequest = new BulkRequest();

            for (Item item : items) {
                ItemDoc itemDoc = BeanUtil.copyProperties(item, ItemDoc.class);
                bulkRequest.add(new IndexRequest("items").id(itemDoc.getId()).source(JSONUtil.toJsonStr(itemDoc),XContentType.JSON));

            }
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
            pageNo++;
        }
    }

    @Test
    void testMatchAll() throws IOException {
        // 1.准备request
        SearchRequest request = new SearchRequest("items");
        // 2.组织DSL参数
        request.source()
                .query(QueryBuilders.matchAllQuery());
        // 3.发送请求，得到相应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }


    @Test
    void testMatch() throws IOException {
        // 1.创建Request
        SearchRequest request = new SearchRequest("items");
        // 2.组织请求参数
        request.source().query(QueryBuilders.matchQuery("name", "脱脂牛奶"));
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    @Test
    void testMultiMatch() throws IOException {
        // 1.创建Request
        SearchRequest request = new SearchRequest("items");
        // 2.组织请求参数
        request.source().query(QueryBuilders.multiMatchQuery("脱脂牛奶", "name", "category"));
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }
    @Test
    void testRange() throws IOException {
        // 1.创建Request
        SearchRequest request = new SearchRequest("items");
        // 2.组织请求参数
        request.source().query(QueryBuilders.rangeQuery("price").gte(10000).lte(30000));
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    @Test
    void testTerm() throws IOException {
        // 1.创建Request
        SearchRequest request = new SearchRequest("items");
        // 2.组织请求参数
        request.source().query(QueryBuilders.termQuery("brand", "华为"));
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    @Test
    void testBool() throws IOException {
        // 1.创建Request
        SearchRequest request = new SearchRequest("items");
        // 2.组织请求参数
        // 2.1.准备bool查询
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        // 2.2.关键字搜索
        bool.must(QueryBuilders.matchQuery("name", "脱脂牛奶"));
        // 2.3.品牌过滤
        bool.filter(QueryBuilders.termQuery("brand", "德亚"));
        // 2.4.价格过滤
        bool.filter(QueryBuilders.rangeQuery("price").lte(30000));
        request.source().query(bool);
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    @Test
    void testPageAndSort() throws IOException {
        int pageNo = 1, pageSize = 5;

        // 1.创建Request
        SearchRequest request = new SearchRequest("items");
        // 2.组织请求参数
        // 2.1.搜索条件参数
        request.source().query(QueryBuilders.matchQuery("name", "脱脂牛奶"));
        // 2.2.排序参数
        request.source().sort("price", SortOrder.ASC);
        // 2.3.分页参数
        request.source().from((pageNo - 1) * pageSize).size(pageSize);
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    @Test
    void testHighlight() throws IOException {
        // 1.创建Request
        SearchRequest request = new SearchRequest("items");
        // 2.组织请求参数
        // 2.1.query条件
        request.source().query(QueryBuilders.matchQuery("name", "脱脂牛奶"));
        // 2.2.高亮条件
        request.source().highlighter(
                SearchSourceBuilder.highlight()
                        .field("name")
                        .preTags("<em>")
                        .postTags("</em>")
        );
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析响应
        handleResponse(response);
    }

    private void handleResponse(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        // 1.获取总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到" + total + "条数据");
        // 2.遍历结果数组
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            // 3.得到_source，也就是原始json文档
            String source = hit.getSourceAsString();
            // 4.反序列化
            ItemDoc item = JSONUtil.toBean(source, ItemDoc.class);
            // 5.获取高亮结果
            Map<String, HighlightField> hfs = hit.getHighlightFields();
            if (CollUtils.isNotEmpty(hfs)) {
                // 5.1.有高亮结果，获取name的高亮结果
                HighlightField hf = hfs.get("name");
                if (hf != null) {
                    // 5.2.获取第一个高亮结果片段，就是商品名称的高亮值
                    String hfName = hf.getFragments()[0].string();
                    item.setName(hfName);
                }
            }
            System.out.println(item);
        }
    }

    // es聚合操作
    @Test
    void testAggs() throws IOException {
        SearchRequest request = new SearchRequest("items");
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        BoolQueryBuilder bool = boolQuery.filter(QueryBuilders.termQuery("category", "手机"))
                .filter(QueryBuilders.rangeQuery("price").gte(30000));
        request.source().query(bool).size(0);

        // 聚合参数
        request.source().aggregation(
                AggregationBuilders.terms("brand_aggs").field("brand").size(5));
        // 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 解析聚合结果
        Aggregations aggregations = response.getAggregations();
        Terms brandAggs = aggregations.get("brand_aggs");

        List<? extends Terms.Bucket> buckets = brandAggs.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            // 获取桶内key
            String brand = bucket.getKeyAsString();
            System.out.println("brand = " + brand);
            // 获取文档数量
            long docCount = bucket.getDocCount();
            System.out.println("docCount = " + docCount);
        }
    }
}
