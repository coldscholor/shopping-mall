package com.hmall.api.config;

import com.hmall.api.fallback.ItemClientFallbackFactory;
import com.hmall.api.fallback.TradeClientFallbackFactory;
import com.hmall.common.utils.UserContext;
import feign.Logger;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import org.springframework.context.annotation.Bean;

public class DefaultFeignConfig {

    // 配置全局日志
    @Bean
    public Logger.Level level(){
        return Logger.Level.FULL;
    }


    /**
     * 当你调用下游微服务（比如通过 OpenFeign）时，微服务之间的调用默认是独立的，彼此之间是隔离的。
     * 如果你需要传递用户信息（如 user-info）到下游微服务，你需要在微服务调用之前将用户信息
     * （从 ThreadLocal 中获取的 userId）加到请求头中。
     * 为了实现这一点，通常做法是通过 Feign 的 RequestInterceptor 来修改请求头。
     * @return
     */
    @Bean
    public RequestInterceptor requestInterceptor(){
        return new RequestInterceptor() {
            @Override
            public void apply(RequestTemplate requestTemplate) {
                Long userId = UserContext.getUser();
                if(userId != null){
                    requestTemplate.header("user-info", userId.toString());
                }
            }
        };
    }

    @Bean
    public ItemClientFallbackFactory itemFallbackFactory(){
        return new ItemClientFallbackFactory();
    }

    @Bean
    public TradeClientFallbackFactory tradeFallbackFactory(){
        return new TradeClientFallbackFactory();
    }

}
