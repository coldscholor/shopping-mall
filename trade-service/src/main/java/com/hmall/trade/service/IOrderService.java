package com.hmall.trade.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.hmall.trade.domain.dto.OrderFormDTO;
import com.hmall.trade.domain.po.Order;
import org.springframework.transaction.annotation.Transactional;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2023-05-05
 */
public interface IOrderService extends IService<Order> {


    @Transactional
    Long createOrder(OrderFormDTO orderFormDTO) throws JsonProcessingException;

    void markOrderPaySuccess(Long orderId);

    void cancelOrder(Long orderId);
}
