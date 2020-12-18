
package eu.domibus.plugin.rabbitmq;

import eu.domibus.common.NotificationType;

import static eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants.MESSAGE_ID;
import static eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY;

import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;

/**
 * @author Christian Koch, Stefan Mueller
 * @author Cosmin Baciu, Generix Group
 */
public class SignalMessageCreator  {
    private NotificationType notificationType;
    private String messageId;

    public SignalMessageCreator(String messageId, NotificationType notificationType) {
        this.messageId = messageId;
        this.notificationType = notificationType;
    }


    public Message createMessage() {
        final MessageProperties properties = MessagePropertiesBuilder.newInstance().build();
        String messageType = null;
        if (this.notificationType == NotificationType.MESSAGE_SEND_SUCCESS) {
            messageType = RabbitmqMessageConstants.MESSAGE_TYPE_SEND_SUCCESS;
        }
        properties.setHeader(RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, messageType);
        properties.setMessageId(messageId);
        return MessageBuilder.withBody(StringUtils.EMPTY.getBytes()).copyProperties(properties).build();
    }
}
