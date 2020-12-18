
package eu.domibus.plugin.rabbitmq;

import eu.domibus.common.ErrorResult;
import eu.domibus.common.NotificationType;

import static eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants.*;

import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;

/**
 * @author Christian Koch, Stefan Mueller, Generix Group
 */
class ErrorMessageCreator {

    private final ErrorResult errorResult;
    private final String endpoint;
    private final NotificationType notificationType;

    public ErrorMessageCreator(ErrorResult errorResult, String endpoint, NotificationType notificationType) {
        this.errorResult = errorResult;
        this.endpoint = endpoint;
        this.notificationType = notificationType;
    }

    public Message createMessage() {
        final MessageProperties properties = MessagePropertiesBuilder.newInstance().build();
        String messageType;
        switch (this.notificationType) {
            case MESSAGE_SEND_FAILURE:
                messageType = RabbitmqMessageConstants.MESSAGE_TYPE_SEND_FAILURE;
                break;
            case MESSAGE_RECEIVED_FAILURE:
                messageType = RabbitmqMessageConstants.MESSAGE_TYPE_RECEIVE_FAILURE;
                break;
            default:
                throw new DefaultRabbitmqPluginException("unknown NotificationType: " + notificationType.name());
        }
        properties.setHeader(RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, messageType);

        if (this.endpoint != null) {
            properties.setHeader(PROPERTY_ENDPOINT, endpoint);
        }
        if (errorResult != null) {
            if (errorResult.getErrorCode() != null) {
                properties.setHeader(RabbitmqMessageConstants.ERROR_CODE, errorResult.getErrorCode().getErrorCodeName());
            }
            properties.setHeader(RabbitmqMessageConstants.ERROR_DETAIL, errorResult.getErrorDetail());
            properties.setMessageId(errorResult.getMessageInErrorId());
        }

        return MessageBuilder.withBody(StringUtils.EMPTY.getBytes()).copyProperties(properties).build();
    }
}