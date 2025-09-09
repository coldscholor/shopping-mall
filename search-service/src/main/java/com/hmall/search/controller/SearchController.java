package com.hmall.search.controller;


import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.metadata.OrderItem;
import com.hmall.common.domain.PageDTO;
import com.hmall.item.domain.dto.ItemDoc;
import com.hmall.item.domain.query.ItemPageQuery;
import com.hmall.search.service.IItemService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Api(tags = "搜索相关接口")
@RestController
@RequestMapping("/search")
@RequiredArgsConstructor
public class SearchController {

    private final IItemService itemService;
    private final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(HttpHost.create("http://192.168.63.128:9200")));

    @ApiOperation("搜索商品")
    @GetMapping("/list")
    public PageDTO<ItemDoc> search(ItemPageQuery query) throws IOException {
        SearchRequest request = new SearchRequest("items");
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (StrUtil.isNotBlank(query.getKey())) {
            boolQuery.must(QueryBuilders.matchQuery("name", query.getKey()));
        }
        if (StrUtil.isNotBlank(query.getCategory())) {
            boolQuery.filter(QueryBuilders.termQuery("category", query.getCategory()));
        }
        if (StrUtil.isNotBlank(query.getBrand())) {
            boolQuery.filter(QueryBuilders.termQuery("brand", query.getBrand()));
        }
        if (query.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").lte(query.getMaxPrice()));
        }
        if (query.getMinPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(query.getMaxPrice()));
        }
        request.source().query(boolQuery).size(query.getPageSize()).from(query.from());

        // sort
        List<OrderItem> orders = query.toMpPage("updateTime", false).orders();
        for (OrderItem order : orders) {
            request.source().sort(order.getColumn(), order.isAsc() ? SortOrder.ASC : SortOrder.DESC);
        }

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        long total = hits.getTotalHits().value;
        long pages = query.getPageNo();
        SearchHit[] searchHits = hits.getHits();
        ArrayList<ItemDoc> itemDocs = new ArrayList<>();
        if (searchHits != null) {
            for (SearchHit hitsHit : searchHits) {
                itemDocs.add(JSONUtil.toBean(hitsHit.getSourceAsString(), ItemDoc.class));
            }
        }
        // 封装并返回
        return new PageDTO<>(total, pages, itemDocs);
        // 分页查询
        /*Page<Item> result = itemService.lambdaQuery()
                .like(StrUtil.isNotBlank(query.getKey()), Item::getName, query.getKey())
                .eq(StrUtil.isNotBlank(query.getBrand()), Item::getBrand, query.getBrand())
                .eq(StrUtil.isNotBlank(query.getCategory()), Item::getCategory, query.getCategory())
                .eq(Item::getStatus, 1)
                .between(query.getMaxPrice() != null, Item::getPrice, query.getMinPrice(), query.getMaxPrice())
                .page(query.toMpPage("update_time", false));*/
        // 封装并返回
        // return PageDTO.of(result, ItemDTO.class);
    }




    @ApiOperation("过滤商品")
    @PostMapping("/filters")
    public Map<String, List<String>> filterItems(@RequestBody ItemPageQuery query) throws IOException {
        SearchRequest searchRequest = new SearchRequest("items");
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if (StrUtil.isNotBlank(query.getKey())) {
            queryBuilder.must(QueryBuilders.matchQuery("name", query.getKey()));
        }
        if (StrUtil.isNotBlank(query.getBrand())) {
            queryBuilder.filter(QueryBuilders.termQuery("brand", query.getBrand()));
        }
        if (StrUtil.isNotBlank(query.getCategory())) {
            queryBuilder.filter(QueryBuilders.matchQuery("category", query.getCategory()));
        }
        if (query.getMaxPrice() != null) {
            queryBuilder.filter(QueryBuilders.rangeQuery("price").gte(query.getMinPrice()).lte(query.getMaxPrice()));
        }
        String categoryAgg = "category_agg";
        String brandAgg = "brand_agg";
        searchRequest.source().query(queryBuilder).aggregation(
                        AggregationBuilders.terms(categoryAgg).field("category"))
                .aggregation(AggregationBuilders.terms(brandAgg).field("brand"));
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        HashMap<String, List<String>> resultMap = new HashMap<>();
        Terms terms = response.getAggregations().get(categoryAgg);
        if (terms != null) {
            resultMap.put("category",terms.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList()));
        }
        terms = response.getAggregations().get(brandAgg);
        if (terms != null) {
            resultMap.put("brand",terms.getBuckets().stream().map(MultiBucketsAggregation.Bucket::getKeyAsString).collect(Collectors.toList()));
        }
        // 封装并返回
        return resultMap;
    }
}

