package com.hmall.common.helper;

import lombok.RequiredArgsConstructor;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class RabbitMqHelper {

    private final RabbitTemplate rabbitTemplate;

    public void sendMessage(String exchange, String routingKey, Object msg) {
        rabbitTemplate.convertAndSend(exchange, routingKey, msg);
    }

    public void sendDelayMessage(String exchange, String routingKey, Object msg, int delay) {
        rabbitTemplate.convertAndSend(exchange, routingKey, msg, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                message.getMessageProperties().setDelay(delay);
                return message;
            }
        });
    }


    public void sendMessageWithConfirm(String exchange, String routingKey, Object msg, int maxRetries) {
        rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
            @Override
            public void confirm(CorrelationData correlationData, boolean ack, String cause) {
                if (ack) {
                    System.out.println("消息发送成功！");
                } else {
                    System.out.println("消息发送失败，原因：" + cause);
                }
            }
        });
        // 发送消息并设置重试机制
        sendWithRetry(exchange, routingKey, msg, maxRetries);
    }

    // 发送带有重试机制的消息
    private void sendWithRetry(String exchange, String routingKey, Object msg, int maxRetries) {
        int retryCount = 0;
        while (retryCount < maxRetries) {
            try {
                sendMessage(exchange, routingKey, msg);
                break; // 如果发送成功，跳出重试循环
            } catch (AmqpException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    // 达到最大重试次数，处理失败
                    System.out.println("达到最大重试次数，消息发送失败：" + e.getMessage());
                } else {
                    // 暂停一段时间后重试
                    try {
                        Thread.sleep(1000 * retryCount); // 简单的重试延迟
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}