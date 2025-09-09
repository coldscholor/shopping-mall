package com.hmall.item.listen;

import cn.hutool.json.JSONUtil;
import com.hmall.api.client.ItemClient;
import com.hmall.api.dto.ItemDTO;
import com.hmall.common.utils.BeanUtils;
import com.hmall.item.domain.dto.ItemDoc;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
public class ItemListener {

    private final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(HttpHost.create("http://192.168.63.128:9200")));

    private final ItemClient itemClient;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "item.save.direct"),
            exchange = @Exchange(name = "save.direct"),
            key = "save"
    ))
    public void listenSaveItem(Long id) throws IOException {
        ItemDTO itemDTO = itemClient.queryItemById(id);
        // 转换为文档类型
        ItemDoc itemDoc = BeanUtils.copyProperties(itemDTO, ItemDoc.class);
        String json = JSONUtil.toJsonStr(itemDoc);
        // 获取新增文档对象
        IndexRequest request = new IndexRequest("items").id(itemDoc.getId());
        // 构建json数据
        request.source(json, XContentType.JSON);
        // 发送请求
        client.index(request, RequestOptions.DEFAULT);
    }


    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "item.delete.direct"),
            exchange = @Exchange(name = "delete.direct"),
            key = "delete"
    ))
    public void listenDeketeItem(Long id) throws IOException {
        DeleteRequest request = new DeleteRequest("items",id.toString());
        client.delete(request, RequestOptions.DEFAULT);
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "item.update.direct"),
            exchange = @Exchange(name = "update.direct"),
            key = "update"
    ))
    public void listenUpdateItem(Long id) throws IOException {
        ItemDTO itemDTO = itemClient.queryItemById(id);
        // 转换为文档类型
        ItemDoc itemDoc = BeanUtils.copyProperties(itemDTO, ItemDoc.class);
        IndexRequest request = new IndexRequest("items").id(itemDoc.getId());
        // 全量修改
        request.source(JSONUtil.toJsonStr(itemDoc),XContentType.JSON);
        // 更新索引库中的文档信息
        client.index(request, RequestOptions.DEFAULT);
    }

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "item.search.direct"),
            exchange = @Exchange(name = "search.direct"),
            key = "search"
    ))
    public void listenSearchItem(Long id) throws IOException {
        GetRequest request = new GetRequest("items").id(id.toString());
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        String source = response.getSourceAsString();
        ItemDoc itemDoc = JSONUtil.toBean(source, ItemDoc.class);
        System.out.println("itemDoc = " + itemDoc);
    }

}
