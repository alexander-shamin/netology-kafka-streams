/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.dsw.kafka.streams.pageview;

import com.example.dsw.kafka.streams.JSONSerDe;
import com.example.dsw.kafka.streams.pageview.POJO.*;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings({"WeakerAccess", "unused"})
public class PageViewTypedDemo {

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsw-kafka-streams" + UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JSONSerDe.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, JSONSerDe.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerDe.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JSONSerDe.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //"earliest");

        PageViewTypedDemo app = new PageViewTypedDemo();

        Topology topology = app.buildTopology();

        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }


    Topology buildTopology() {
        final JSONSerDe<PageView> pageViewJSONSerDe = new JSONSerDe<>(new TypeReference<PageView>() {});
        final JSONSerDe<User> userJSONSerDe = new JSONSerDe<>(new TypeReference<User>() {});
        final JSONSerDe<PageViewByRegion> pageViewByRegionJSONSerDe = new JSONSerDe<>(new TypeReference<PageViewByRegion>() {});
        final JSONSerDe<WindowedPageViewByRegion> windowedPageViewByRegionJSONSerDe = new JSONSerDe<>(new TypeReference<WindowedPageViewByRegion>() {});
        final JSONSerDe<RegionCount> regionCountJSONSerDe = new JSONSerDe<>(new TypeReference<RegionCount>() {});

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PageView> views = builder.stream("pageviews", Consumed.with(Serdes.String(), pageViewJSONSerDe))
                .selectKey((k, v) -> v.userid);

        final KTable<String, User> users = builder.table("users", Consumed.with(Serdes.String(), userJSONSerDe));

        final Duration duration24Hours = Duration.ofHours(24);

        final KStream<String, RegionCount> regionCount = views
                .leftJoin(users, (view, profile) -> {
                    final PageViewByRegion viewByRegion = new PageViewByRegion();
                    viewByRegion.user = view.userid;
                    viewByRegion.page = view.pageid;

                    if (profile != null) {
                        viewByRegion.region = profile.regionid;
                    } else {
                        viewByRegion.region = "UNKNOWN";
                    }
                    return viewByRegion;
                })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.region, viewRegion))
                .groupByKey(Grouped.with(Serdes.String(), pageViewByRegionJSONSerDe))
                .count()
                .toStream()
                .mapValues((k, v) -> {
                    RegionCount count = new RegionCount();

                    count.region = k;
                    count.count = v;

                    return count;
                });

        regionCount.to("streams-pageviewstats-typed-output", Produced.with(Serdes.String(), regionCountJSONSerDe));

        return builder.build();
    }
}
