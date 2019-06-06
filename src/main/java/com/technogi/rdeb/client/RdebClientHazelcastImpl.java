package com.technogi.rdeb.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.technogi.rdeb.client.exceptions.PublishingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.String.format;

public class RdebClientHazelcastImpl implements RdebClient {

  private final Logger log = LoggerFactory.getLogger(RdebClientHazelcastImpl.class);
  private static final String SERVICES_LIST_NAME = "rdeb:_ServicesIdx";
  private static final String BROADCAST_KEY = "rdeb:_Broadcasts";
  private Map<String, List<EventHandler>> eventHandlersMap = Collections.synchronizedMap(new HashMap<>());

  private HazelcastInstance instance;
  private ScheduledExecutorService executor = null;
  private Config config = new Config().setPollingTime(1);
  private ClientConfig hazelcastConfig = null;
  private int initialDelay = 2;

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
    if (this.config.getClientId() == null) {
      this.config.setClientId(UUID.randomUUID().toString());
    }
    instance = HazelcastClient.newHazelcastClient(hazelcastConfig);
    IList<String> services = instance.getList(SERVICES_LIST_NAME);
    if (!services.contains(this.config.getClientId())) {
      services.add(this.config.getClientId());
    }
  }

  @Override
  public void start() {
    if (executor == null) executor = Executors.newSingleThreadScheduledExecutor();
    executor.scheduleAtFixedRate(() -> {
      eventHandlersMap.forEach((key, handlerList) -> {
        if (!handlerList.isEmpty()) {
          while (!instance.<Event>getQueue(key).isEmpty()) {
            //try {
              //Event event = instance.<Event>getQueue(key).take();
            log.debug("Reading queue");
            Event event = instance.<Event>getQueue(key).remove();
            log.debug("There are {} messages available in {}",instance.<Event>getQueue(key).size(), key);
            handlerList.forEach(handler -> handler.apply(event));
            //} catch (InterruptedException e) {
            //  e.printStackTrace();
            //}
          }
        }
      });
    }, initialDelay, config.getPollingTime(), TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    if (executor != null && !executor.isTerminated()) {
      executor.shutdown();
    }
  }

  @Override
  public void publish(Event event) {
    if (event == null) {
      throw new PublishingException("Event can not be null");
    }
    if (event.getType() == null) {
      throw new PublishingException("Event type can not be null");
    }
    log.debug("Publishing event {}", event);
    serviceKeys(event).forEach(key -> instance.<Event>getQueue(key).offer(event));
  }

  @Override
  public void broadcast(Event event) {
    log.debug("Broadcasting event {}", event);
    if (event != null && event.getType() != null)
      instance.getTopic(format("%s:%s", BROADCAST_KEY, event.getType())).publish(event);
  }

  @Override
  public void subscribe(String eventType, EventHandler eventHandler) {
    final String key = key(eventType);

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

  private Stream<String> serviceKeys(Event event) {
    return instance.getList(SERVICES_LIST_NAME)
        .stream()
        .map(serviceName -> format("rdeb:%s:%s", serviceName, event.getType()));
  }

  public RdebClientHazelcastImpl setInitialDelay(int initialDelay) {
    this.initialDelay = initialDelay;
    return this;
  }

  private String key(String eventType) {
    return format("rdeb:%s:%s", this.config.getClientId(), eventType);
  }
}
