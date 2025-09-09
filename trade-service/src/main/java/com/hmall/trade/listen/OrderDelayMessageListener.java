package com.hmall.trade.listen;

import com.hmall.api.client.PayClient;
import com.hmall.api.dto.PayOrderDTO;
import com.hmall.trade.domain.po.Order;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderDelayMessageListener {

    private final IOrderService orderService;

    private final PayClient payClient;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "trade.delay.order.queue"),
            exchange = @Exchange(name = "trade.delay.direct", delayed = "true"),
            key = "deley.order.query"
    ))
    public void listenOrderDelayMessage(Long orderId){
        Order order = orderService.getById(orderId);
        if(order == null || order.getStatus() != 1){
            return;
        }

        PayOrderDTO payOrderDTO = payClient.queryPayOrderByBizOrderNo(orderId);
        if(payOrderDTO != null && payOrderDTO.getStatus() == 3){
            // 订单已支付，将订单状态标记为已支付
            orderService.markOrderPaySuccess(orderId);
        }else{
            // 如果未支付，则取消订单并恢复商品库存
            orderService.cancelOrder(orderId);
        }

    }
}
