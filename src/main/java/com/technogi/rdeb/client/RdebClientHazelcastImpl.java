package com.technogi.rdeb.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.technogi.rdeb.client.exceptions.PublishingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RdebClientHazelcastImpl implements RdebClient {

  private final Logger log = LoggerFactory.getLogger(RdebClientHazelcastImpl.class);

  private Map<String, List<EventHandler>> eventHandlersMap = Collections.synchronizedMap(new HashMap<>());

  private HazelcastInstance instance;
  private ScheduledExecutorService executor = null;
  private Config config = new Config().setPollingTime(2);
  private ClientConfig hazelcastConfig = null;

  public RdebClientHazelcastImpl setExecutor(ScheduledExecutorService executor) {
    this.executor = executor;
    return this;
  }

  public RdebClientHazelcastImpl setHazelcastConfig(ClientConfig hazelcastConfig) {
    this.hazelcastConfig = hazelcastConfig;
    return this;
  }

  @Override
  public void connect(final Config config) {
    if (config != null) this.config = config;
    instance = HazelcastClient.newHazelcastClient(hazelcastConfig);
  }

  @Override
  public void start() {
    executor.scheduleAtFixedRate(() -> {
      eventHandlersMap.forEach((key, handlerList) -> {
        if (!handlerList.isEmpty()) {
          try {
            Event event = instance.<Event>getQueue(key).take();
            handlerList.forEach(handler -> handler.apply(event));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }, 5, config.getPollingTime(), TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    if (executor != null && !executor.isTerminated()) {
      executor.shutdown();
    }
  }

  @Override
  public void publish(Event event) {
    log.debug("Publishing event {}", event);
    if (event != null && event.getType() != null)
      try {
        instance.getQueue("rdeb:" + event.getType()).put(event);
      } catch (InterruptedException e) {
        log.error(e.getLocalizedMessage(), e);
        throw new PublishingException(e);
      }
  }

  @Override
  public void broadcast(Event event) {
    log.debug("Broadcasting event {}", event);
    if (event != null && event.getType() != null)
      instance.getTopic("rdeb:" + event.getType()).publish(event);
  }

  @Override
  public void subscribe(String s, EventHandler eventHandler) {
    final String key = "rdeb:" + s;

    if (!eventHandlersMap.containsKey(key)) {
      eventHandlersMap.put(key, new LinkedList<>());
    }
    eventHandlersMap.get(key).add(eventHandler);

    instance.<Event>getTopic(key).addMessageListener(message -> {
      if (executor != null && !executor.isTerminated()) {
        eventHandler.apply(message.getMessageObject());
      }
    });
  }

}
