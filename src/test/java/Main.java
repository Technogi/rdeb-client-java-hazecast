package com.technogi.rdeb.client;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Date;
import java.util.Properties;

public class Main {

  public static void main(String[] args) throws Exception {
    System.out.println("=================== COMIENZA =====================");
    com.hazelcast.config.Config cfg = new Config();
    HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
    System.out.println("=================== INSTANCIA =====================");

    ClientConfig clientConfig = new ClientConfig();
    HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
    System.out.println("=================== CLIENTE =====================");

    RdebClientHazelcastImpl rdeb = new RdebClientHazelcastImpl();

    rdeb.connect(new com.technogi.rdeb.client.Config());
    rdeb.subscribe("evento1", event -> {
      System.out.println("Se recibió evento1 :" + event.getProps());
    });

    rdeb.start();

    rdeb.subscribe("evento1", event -> {
      System.out.println("Se recibió evento1 en otro:" + event.getProps());
    });


    rdeb.subscribe("evento2", event -> {
      System.out.println("Se recibió evento2 :" + event.getProps());
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });

    rdeb.publish(new Event().setType("evento1").setProps(new Properties() {{
      put("propiedad1", "prop1");
      put("p2", "prop2");
    }}));


    rdeb.publish(new Event().setType("evento1").setProps(new Properties() {{
      put("propiedad1", new Date());
      put("p2", "prop2");
    }}));


    rdeb.publish(new Event().setType("evento2").setProps(new Properties() {{
      put("propiedad1", "---");
      put("p2", "***");
    }}));

    rdeb.publish(new Event().setType("evento2").setProps(new Properties() {{
      put("propiedad1", "-e--");
      put("p2", "**e*");
    }}));

    rdeb.broadcast(new Event().setType("evento1").setProps(new Properties(){{put(
        "broad","cast"
    );}}));
    System.out.println("=================== Handlers =====================");


    Thread.sleep(10000);

    System.out.println("=================== LISTO =====================");
    System.exit(-1);
  }
}
