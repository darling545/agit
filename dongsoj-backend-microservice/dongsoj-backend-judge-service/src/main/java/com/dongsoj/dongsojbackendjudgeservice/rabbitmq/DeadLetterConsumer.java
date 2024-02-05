package com.dongsoj.dongsojbackendjudgeservice.rabbitmq;


import com.dongsoj.dongsojbackendcommon.common.ErrorCode;
import com.dongsoj.dongsojbackendcommon.exception.BusinessException;
import com.dongsoj.dongsojbackendmodel.model.entity.QuestionSubmit;
import com.dongsoj.dongsojbackendmodel.model.enums.QuestionSubmitStatusEnum;
import com.dongsoj.dongsojbackendserviceclient.service.QuestionFeignClient;
import com.rabbitmq.client.Channel;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 死信队列消费者
 *  @author dongs
 */
@Slf4j
//@Component
public class DeadLetterConsumer {

    @Resource
    private QuestionFeignClient questionFeignClient;


    /**
     * 监听死信队列
     * @param message
     * @param channel
     * @param deliveryTag
     */
    @SneakyThrows
    @RabbitListener(queues = {"code_dead_queue"}, ackMode = "MANUAL")
    public void receiveFailureMessage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag){
        log.info("收到死信队列消息：{}",message);
        if (StringUtils.isBlank(message)){
            channel.basicNack(deliveryTag, false, false);
            throw new BusinessException(ErrorCode.PARAMS_ERROR,"消息为空");
        }

        long questionSubmitId = Long.parseLong(message);
        QuestionSubmit questionSubmitById = questionFeignClient.getQuestionSubmitById(questionSubmitId);

        if (questionSubmitById == null){
            channel.basicNack(deliveryTag, false, false);
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR,"提交的题目信息不存在");
        }
        // 标记提交题目信息为失败
        questionSubmitById.setStatus(QuestionSubmitStatusEnum.FAILED.getValue());
        boolean update = questionFeignClient.updateQuestionSubmitById(questionSubmitById);

        if (!update){
            log.info("处理死信队列信息失败,对应的题目信息id为: {}", questionSubmitById.getId());
            throw new BusinessException(ErrorCode.SYSTEM_ERROR,"处理死信队列信息失败");
        }

        channel.basicAck(deliveryTag,false);
    }

}
