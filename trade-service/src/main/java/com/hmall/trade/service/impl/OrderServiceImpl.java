package com.hmall.trade.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.hmall.api.client.ItemClient;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.exception.BadRequestException;
import com.hmall.common.helper.RabbitMqHelper;
import com.hmall.common.utils.BeanUtils;
import com.hmall.common.utils.UserContext;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.domain.po.OrderDetail;
import com.hmall.trade.mapper.OrderMapper;
import com.hmall.trade.service.IOrderDetailService;
import com.hmall.trade.service.IOrderService;
import io.seata.spring.annotation.GlobalTransactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl extends ServiceImpl<OrderMapper, Order> implements IOrderService {

    private final ItemClient itemClient;
    private final IOrderDetailService detailService;
    // private final CartClient cartClient;
    private final RabbitTemplate rabbitTemplate;

    private final RabbitMqHelper rabbitMqHelper;

    @Override
    @GlobalTransactional // 开启分布式事务
    public Long createOrder(OrderFormDTO orderFormDTO) throws JsonProcessingException {
        // 1.订单数据
        Order order = new Order();
        // 1.1.查询商品
        List<OrderDetailDTO> detailDTOS = orderFormDTO.getDetails();
        // 1.2.获取商品id和数量的Map
        Map<Long, Integer> itemNumMap = detailDTOS.stream()
                .collect(Collectors.toMap(OrderDetailDTO::getItemId, OrderDetailDTO::getNum));
        Set<Long> itemIds = itemNumMap.keySet();
        // 1.3.查询商品
        List<ItemDTO> items = itemClient.queryItemByIds(itemIds);
        if (items == null || items.size() < itemIds.size()) {
            throw new BadRequestException("商品不存在");
        }
        // 1.4.基于商品价格、购买数量计算商品总价：totalFee
        int total = 0;
        for (ItemDTO item : items) {
            total += item.getPrice() * itemNumMap.get(item.getId());
        }
        order.setTotalFee(total);
        // 1.5.其它属性
        order.setPaymentType(orderFormDTO.getPaymentType());
        order.setUserId(UserContext.getUser());
        order.setStatus(1);
        // 1.6.将Order写入数据库order表中
        save(order);

        // 2.保存订单详情
        List<OrderDetail> details = buildDetails(order.getId(), items, itemNumMap);
        detailService.saveBatch(details);

        // 3.清理购物车商品
        // cartClient.removeByItemIds(itemIds);

        // rabbitMqHelper.sendMessage("trade.topic","order.create", itemIds,);
        try {
            rabbitTemplate.convertAndSend("trade.topic", "order.create", itemIds, new MessagePostProcessor() {
                @Override
                public Message postProcessMessage(Message message) throws AmqpException {
                    message.getMessageProperties().setHeader("user-info", UserContext.getUser());
                    return message;
                }
            });
            log.info("清理购物车成功");
        } catch (AmqpException e) {
            log.error("清理购物车失败，商品：{}，用户：{}", itemIds, UserContext.getUser());
        }

        // 4.扣减库存
        try {
            itemClient.deductStock(detailDTOS);
        } catch (Exception e) {
            throw new RuntimeException("库存不足！");
        }

        // 5.发送延迟消息
        /*rabbitTemplate.convertAndSend("trade.delay.direct", "deley.order.query", order.getId(), new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                // 设置延迟15分钟再去检查支付状态，方法商品被一直占用
                message.getMessageProperties().setDelay(10000);
                return message;
            }
        });*/
        rabbitMqHelper.sendDelayMessage("trade.delay.direct", "deley.order.query", order.getId(), 10000);

        return order.getId();
    }

    @Override
    @Transactional
    public void markOrderPaySuccess(Long orderId) {
        // 1.查询订单
        Order old = getById(orderId);
        // 2.判断订单状态
        if (old == null || old.getStatus() != 1) {
            // 订单不存在或者订单状态不是1，放弃处理
            return;
        }
        // 3.尝试更新订单
        Order order = new Order();
        order.setId(orderId);
        order.setStatus(2);
        order.setPayTime(LocalDateTime.now());
        updateById(order);
        // 分布式事务：throw new BizIllegalException("测试");
    }

    private List<OrderDetail> buildDetails(Long orderId, List<ItemDTO> items, Map<Long, Integer> numMap) {
        List<OrderDetail> details = new ArrayList<>(items.size());
        for (ItemDTO item : items) {
            OrderDetail detail = new OrderDetail();
            detail.setName(item.getName());
            detail.setSpec(item.getSpec());
            detail.setPrice(item.getPrice());
            detail.setNum(numMap.get(item.getId()));
            detail.setItemId(item.getId());
            detail.setImage(item.getImage());
            detail.setOrderId(orderId);
            details.add(detail);
        }
        return details;
    }

    @Override
    @Transactional
    public void cancelOrder(Long orderId) {
        // 1. 进行幂等性校验
        /*Order order = getById(orderId);
        if (order != null) {
            order.setStatus(5);
            order.setUpdateTime(LocalDateTime.now());
            updateById(order);
        }*/
        // 等价于：
        // 1. 修改订单状态为 "已关闭"
        update(new LambdaUpdateWrapper<Order>()
                .set(Order::getStatus, 5)
                .set(Order::getUpdateTime, LocalDateTime.now())
                .eq(Order::getId, orderId)
        );

        // 2. 查询订单详情，获取订单商品列表
        List<OrderDetail> orderDetails = detailService.list(
                new LambdaQueryWrapper<OrderDetail>().eq(OrderDetail::getOrderId, orderId)
        );

        List<OrderDetailDTO> orderDetailDTOS = BeanUtils.copyList(orderDetails, OrderDetailDTO.class);
        // 4. 恢复库存
        itemClient.restoreStock(orderDetailDTOS);
    }
}
