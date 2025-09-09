package com.hmall.api.fallback;

import com.hmall.api.client.ItemClient;
import com.hmall.api.dto.ItemDTO;
import com.hmall.api.dto.OrderDetailDTO;
import com.hmall.common.exception.BizIllegalException;
import com.hmall.common.utils.CollUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;

import java.util.Collection;
import java.util.List;

// 编写失败处理逻辑
@Slf4j
public class ItemClientFallbackFactory implements FallbackFactory<ItemClient> {
    @Override
    public ItemClient create(Throwable cause) {
        return new ItemClient() {
            @Override
            public List<ItemDTO> queryItemByIds(Collection<Long> ids) {
                log.error("查询商品失败：{}",cause);
                return CollUtils.emptyList();
            }

            @Override
            public ItemDTO queryItemById(Long id) {
                log.error("查询商品失败：{}",cause);
                return null;
            }

            @Override
            public void deductStock(List<OrderDetailDTO> items) {
                log.error("扣减商品库存失败：{}",cause);
                throw new BizIllegalException("扣减商品库存失败");
            }

            @Override
            public void restoreStock(List<OrderDetailDTO> orderDetailDTOS) {
                log.error("商品库存恢复失败：{}",cause);
                throw new BizIllegalException("商品库存恢复失败");
            }
        };
    }
}
