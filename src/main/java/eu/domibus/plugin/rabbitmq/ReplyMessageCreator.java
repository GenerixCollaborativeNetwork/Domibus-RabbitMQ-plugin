
package eu.domibus.plugin.rabbitmq;

import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;

/**
 * @author Christian Koch, Stefan Mueller, Generix Group
 */
public class ReplyMessageCreator {
    private String messageId;
    private String errorMessage;
    private String correlationId;

    ReplyMessageCreator(final String messageId, final String errorMessage, final String correlationId) {
        this.messageId = messageId;
        this.errorMessage = errorMessage;
        this.correlationId = correlationId;
    }


    public Message createMessage() {
        final MessageProperties properties = MessagePropertiesBuilder.newInstance().build();
        properties.setHeader(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, RabbitmqMessageConstants.MESSAGE_TYPE_SUBMIT_RESPONSE);
        properties.setCorrelationId(correlationId);
        if (messageId != null) {
        	properties.setMessageId(messageId);
        }
        if (errorMessage != null) {
        	properties.setHeader("ErrorMessage", errorMessage);
        }
        return MessageBuilder.withBody(StringUtils.EMPTY.getBytes()).copyProperties(properties).build();
    }
}