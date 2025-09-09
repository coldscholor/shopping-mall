package com.hmall.trade.listen;

import com.hmall.trade.domain.po.Order;
import com.hmall.trade.service.IOrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PayStatusListener {

    private final IOrderService orderService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "trade.pay.success.queue", durable = "true"),
            exchange = @Exchange(name = "pay.direct", type = ExchangeTypes.DIRECT),
            key = "pay.success"
    ))
    /* 此处进行业务幂等性判断：
    *  如：订单支付业务
    * 支付完成后发送支付成功消息到MQ，trade-service监听MQ
    * 但是此时网络出现故障生产者没有得到确认，隔了一段时间后重新投递给交易服务。
    * 这时用户申请退款，订单状态被修改为退款中
    * 退款成功，一段时间后网络恢复，MQ又会重新投递消息到监听MQ的服务，订单又被修改为已支付，
    * 但这时都已经退款成功了，就发生了业务异常
    * */
    public void listenPaySuccess(Long orderId){
        // 1.查询订单
        Order order = orderService.getById(orderId);
        // 2.判断订单状态是否为已支付
        if(order == null || order.getStatus() != 1){
            return;
        }
        // 3.标记订单为付
        orderService.markOrderPaySuccess(orderId);
    }

}
