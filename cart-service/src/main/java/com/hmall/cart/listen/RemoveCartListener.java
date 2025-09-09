package com.hmall.cart.listen;

import cn.hutool.core.util.ObjectUtil;
import com.hmall.cart.service.ICartService;
import com.hmall.common.utils.UserContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
@RequiredArgsConstructor
@Slf4j
public class RemoveCartListener {

    private final ICartService cartService;

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "cart.clear.queue", durable = "true"),
            exchange = @Exchange(name = "trade.topic", type = ExchangeTypes.TOPIC),
            key = "order.create"
    ))
    public void listenPaySuccess(Collection<Long> ids, Message message){
        // 从请求头中获取用户id
        Long userId = message.getMessageProperties().getHeader("user-info");
        if(ObjectUtil.isNotNull(userId)){
            UserContext.setUser(userId);
        }
        log.info("下单的商品id：{}，当前登录用户信息：{}", ids, userId);
        cartService.removeByItemIds(ids);
        // 处理完毕后清除线程本地存储的用户信息
        UserContext.removeUser();
    }

}
