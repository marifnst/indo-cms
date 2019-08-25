package com.indocms.notificationservice.channel;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface NotificationChannel {
    
	@Output("exportNotification")
	MessageChannel exportNotification();
}