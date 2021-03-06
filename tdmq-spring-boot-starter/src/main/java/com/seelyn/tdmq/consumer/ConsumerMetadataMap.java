package com.seelyn.tdmq.consumer;

import java.util.Map;

/**
 * 订阅者方法集合接口
 *
 * @author linfeng
 */
public interface ConsumerMetadataMap {


    /**
     * 订阅者Map
     *
     * @return 订阅者Map
     */
    Map<String, ConsumerMetadata> getMap();
}
