package com.indocms.notificationservice.controller;

import com.indocms.notificationservice.channel.NotificationChannel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Controller
@RestController
@EnableBinding(NotificationChannel.class)
public class NotificationController {

    @Autowired
    private NotificationChannel notificationChannel;

    @RequestMapping(value="/notification/export", method = RequestMethod.GET)
    public void sendMessage() {
        notificationChannel.exportNotification().send(MessageBuilder.withPayload("Sample Export Notification").build());
    }

    @MessageMapping("/hello")
    @SendTo("/topic/exportNotification")
    public String greeting() throws Exception {
        Thread.sleep(1000); // simulated delay
        return "Web Socket From Spring";
    }
}