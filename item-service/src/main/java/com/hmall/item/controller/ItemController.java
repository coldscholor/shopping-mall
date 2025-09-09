package com.hmall.item.controller;


import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmall.common.domain.PageDTO;
import com.hmall.common.domain.PageQuery;
import com.hmall.common.helper.RabbitMqHelper;
import com.hmall.common.utils.BeanUtils;
import com.hmall.item.domain.dto.ItemDTO;
import com.hmall.item.domain.dto.ItemDoc;
import com.hmall.item.domain.dto.OrderDetailDTO;
import com.hmall.item.domain.po.Item;
import com.hmall.item.service.IItemService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@Api(tags = "商品管理相关接口")
@RestController
@RequestMapping("/items")
@RequiredArgsConstructor
public class ItemController {

    private final IItemService itemService;

    private final RabbitMqHelper rabbitMqHelper;

    private final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(HttpHost.create("http://192.168.63.128:9200")));


    @ApiOperation("分页查询商品")
    @GetMapping("/page")
    public PageDTO<ItemDTO> queryItemByPage(PageQuery query) {
        // 1.分页查询
        Page<Item> result = itemService.page(query.toMpPage("update_time", false));
        // 2.封装并返回
        return PageDTO.of(result, ItemDTO.class);
    }

    @ApiOperation("根据id批量查询商品")
    @GetMapping
    public List<ItemDTO> queryItemByIds(@RequestParam("ids") List<Long> ids){
        // 模拟业务延迟
        ThreadUtil.sleep(500);
        return itemService.queryItemByIds(ids);
    }

    /**
     * 用ElasticSearch技术实现
     * @param id
     * @return
     * @throws IOException
     */
    @ApiOperation("根据id查询商品")
    @GetMapping("{id}")
    public ItemDTO queryItemById(@PathVariable("id") Long id) throws IOException {
        GetRequest request = new GetRequest("items").id(id.toString());
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        String source = response.getSourceAsString();
        ItemDoc itemDoc = JSONUtil.toBean(source, ItemDoc.class);
        return BeanUtils.copyProperties(itemDoc, ItemDTO.class);

    }
    @ApiOperation("新增商品")
    @PostMapping
    public void saveItem(@RequestBody ItemDTO itemDTO) {
        Item item = BeanUtils.copyBean(itemDTO, Item.class);
        // 新增
        itemService.save(item);
        // 更新索引库中的数据，这里采用MQ异步通知
        rabbitMqHelper.sendMessage("save.direct","save",item.getId());
    }

    @ApiOperation("更新商品状态")     @PutMapping("/status/{id}/{status}")
    public void updateItemStatus(@PathVariable("id") Long id, @PathVariable("status") Integer status){
        Item item = new Item();
        item.setId(id);
        item.setStatus(status);
        itemService.updateById(item);
    }

    @ApiOperation("更新商品")
    @PutMapping
    public void updateItem(@RequestBody ItemDTO item) {
        // 不允许修改商品状态，所以强制设置为null，更新时，就会忽略该字段
        item.setStatus(null);
        // 更新
        itemService.updateById(BeanUtils.copyBean(item, Item.class));

        // 更新索引库中的数据，这里采用MQ异步通知
        rabbitMqHelper.sendMessage("update.direct","update",item.getId());
    }

    @ApiOperation("根据id删除商品")
    @DeleteMapping("{id}")
    public void deleteItemById(@PathVariable("id") Long id) {
        itemService.removeById(id);
        // 更新索引库中的数据，这里采用MQ异步通知
        rabbitMqHelper.sendMessage("delete.direct","delete",id);
    }

    @ApiOperation("批量扣减库存")
    @PutMapping("/stock/deduct")
    public void deductStock(@RequestBody List<OrderDetailDTO> items){
        itemService.deductStock(items);
    }

    @ApiOperation("恢复商品库存")
    @PutMapping("/stock/restore")
    void restoreStock(@RequestBody List<OrderDetailDTO> orderDetailDTOS){
        itemService.restoreStock(orderDetailDTOS);
    }
}
