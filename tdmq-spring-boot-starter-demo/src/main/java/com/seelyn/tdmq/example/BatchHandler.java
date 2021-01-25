package com.seelyn.tdmq.example;

import com.seelyn.tdmq.TdmqBatchListener;
import com.seelyn.tdmq.annotation.TdmqHandler;
import com.seelyn.tdmq.annotation.TdmqTopic;
import com.seelyn.tdmq.exception.MessageRedeliverException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.springframework.stereotype.Component;

/**
 * @author linfeng
 */
@Component
@TdmqHandler(topics = {
        @TdmqTopic(topic = "persistent://pulsar-m93253wq27/eqx-scs/scs", tags = "cdc || mns")
})
public class BatchHandler implements TdmqBatchListener<String> {


    @Override
    public void received(Consumer<String> consumer, Messages<String> messages) throws MessageRedeliverException {

        System.out.printf("消息数量：%d", messages.size());
        for (Message<String> message : messages) {
            System.out.println(message.getValue());
        }

    }

}
