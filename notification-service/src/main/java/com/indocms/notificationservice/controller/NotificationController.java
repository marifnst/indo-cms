package com.indocms.notificationservice.controller;

import com.indocms.notificationservice.channel.NotificationChannel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@EnableBinding(NotificationChannel.class)
public class NotificationController {

    @Autowired
    private NotificationChannel notificationChannel;

    @RequestMapping(value="/notification/export", method = RequestMethod.GET)
    public void sendMessage() {
        notificationChannel.exportNotification().send(MessageBuilder.withPayload("Sample Export Notification").build());
    }
}