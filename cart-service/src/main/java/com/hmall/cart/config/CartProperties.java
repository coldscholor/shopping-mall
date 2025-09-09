package com.hmall.cart.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

@Data
@Component
// @ConfigurationProperties("hm.cart")
@RefreshScope
public class CartProperties {
    // 设置购物车的最大数量，热更新
    @Value("${hm.cart.maxItems}")
    private Integer maxItems;
}
