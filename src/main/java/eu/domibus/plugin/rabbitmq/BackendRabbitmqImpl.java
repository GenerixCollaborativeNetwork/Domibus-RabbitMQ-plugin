package eu.domibus.plugin.rabbitmq;

import eu.domibus.common.ErrorResult;
import eu.domibus.common.MessageReceiveFailureEvent;
import eu.domibus.common.NotificationType;
import eu.domibus.ext.domain.DomainDTO;
import eu.domibus.ext.exceptions.DomibusPropertyExtException;
import eu.domibus.ext.services.DomainContextExtService;
import eu.domibus.ext.services.DomibusPropertyExtService;
import eu.domibus.logging.DomibusLogger;
import eu.domibus.logging.DomibusLoggerFactory;
import eu.domibus.logging.DomibusMessageCode;
import eu.domibus.logging.MDCKey;
import eu.domibus.messaging.MessageConstants;
import eu.domibus.messaging.MessageNotFoundException;
import eu.domibus.messaging.MessagingProcessingException;
import eu.domibus.plugin.AbstractBackendConnector;
import eu.domibus.plugin.transformer.MessageRetrievalTransformer;
import eu.domibus.plugin.transformer.MessageSubmissionTransformer;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jms.core.JmsOperations;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants.MESSAGE_TYPE_SUBMIT;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

/**
 * @author Christian Koch, Stefan Mueller, Generix Group
 */
public class BackendRabbitmqImpl extends AbstractBackendConnector<Message, Message> {

	private static final DomibusLogger LOG = DomibusLoggerFactory.getLogger(BackendRabbitmqImpl.class);

	protected static final String RABBITMQ_PLUGIN_QUEUE_REPLY = "plugin.rabbitmq.queue.reply";
	protected static final String RABBITMQ_PLUGIN_QUEUE_CONSUMER_NOTIFICATION_ERROR = "plugin.rabbitmq.queue.consumer.notification.error";
	protected static final String RABBITMQ_PLUGIN_QUEUE_PRODUCER_NOTIFICATION_ERROR = "plugin.rabbitmq.queue.producer.notification.error";
	protected static final String RABBITMQ_PLUGIN_QUEUE_OUT = "plugin.rabbitmq.queue.out";

	@Autowired
	protected DomibusPropertyExtService domibusPropertyExtService;

	@Autowired
	protected DomainContextExtService domainContextExtService;

	@Autowired
	@Qualifier(value = "mshToBackendTemplate")
	private JmsOperations mshToBackendTemplate;

	private MessageRetrievalTransformer<Message> messageRetrievalTransformer;
	private MessageSubmissionTransformer<Message> messageSubmissionTransformer;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	public BackendRabbitmqImpl(String name) {
		super(name);
	}

	@Override
	public MessageSubmissionTransformer<Message> getMessageSubmissionTransformer() {
		return this.messageSubmissionTransformer;
	}

	public void setMessageSubmissionTransformer(MessageSubmissionTransformer<Message> messageSubmissionTransformer) {
		this.messageSubmissionTransformer = messageSubmissionTransformer;
	}

	@Override
	public MessageRetrievalTransformer<Message> getMessageRetrievalTransformer() {
		return this.messageRetrievalTransformer;
	}

	public void setMessageRetrievalTransformer(MessageRetrievalTransformer<Message> messageRetrievalTransformer) {
		this.messageRetrievalTransformer = messageRetrievalTransformer;
	}

	/**
	 * This method is called when a message was received at the incoming queue
	 *
	 * @param message
	 *            The incoming RabbitMQ Message
	 */
	@MDCKey(DomibusLogger.MDC_MESSAGE_ID)
	@Transactional
	public void receiveMessage(final Message message) {
		try {
			MessageProperties properties = message.getMessageProperties();
			Map<String, Object> headers = properties.getHeaders();
			String messageID = properties.getMessageId();
			if (StringUtils.isNotBlank(messageID)) {
				// trim the empty space
				messageID = messageExtService.cleanMessageIdentifier(messageID);
				LOG.putMDC(DomibusLogger.MDC_MESSAGE_ID, messageID);
			}
			final String correlationID = properties.getCorrelationId();
			final String messageType = headers.get(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY).toString();

			LOG.info("Received message with messageId [{}], correlationID [{}]", messageID, correlationID);

			if (!MESSAGE_TYPE_SUBMIT.equals(messageType)) {
				String wrongMessageTypeMessage = getWrongMessageTypeErrorMessage(messageID, correlationID, messageType);
				LOG.error(wrongMessageTypeMessage);
				sendReplyMessage(messageID, wrongMessageTypeMessage, correlationID);
				return;
			}

			String errorMessage = null;
			try {
				// in case the messageID is not sent by the user it will be
				// generated
				messageID = submit(message);
			} catch (final MessagingProcessingException e) {
				LOG.error("Exception occurred receiving message [{}}], correlationID [{}}]", messageID, correlationID, e);
				errorMessage = e.getMessage() + ": Error Code: " + (e.getEbms3ErrorCode() != null ? e.getEbms3ErrorCode().getErrorCodeName() : " not set");
			}

			sendReplyMessage(messageID, errorMessage, correlationID);

			LOG.info("Submitted message with messageId [{}], correlationID [{}}]", messageID, correlationID);
		} catch (Exception e) {
			LOG.error("Exception occurred while receiving message [" + message + "]", e);
			throw new DefaultRabbitmqPluginException("Exception occurred while receiving message [" + message + "]", e);
		}
	}

	protected String getWrongMessageTypeErrorMessage(String messageID, String correlationID, String messageType) {
		return MessageFormat.format(
				"Illegal messageType [{0}] on message with correlationId [{1}] and messageId [{2}]. Only [{3}] messages are accepted on this queue",
				messageType, correlationID, messageID, MESSAGE_TYPE_SUBMIT);
	}

	protected void sendReplyMessage(final String messageId, final String errorMessage, final String correlationId) {
		LOG.debug("Sending reply message");
		final Message message = new ReplyMessageCreator(messageId, errorMessage, correlationId).createMessage();
		sendRabbitMQMessage(message, RABBITMQ_PLUGIN_QUEUE_REPLY);
	}

	@Override
	public void deliverMessage(final String messageId) {
		LOG.debug("Delivering message");
		final DomainDTO currentDomain = domainContextExtService.getCurrentDomain();
		final String queueValue = domibusPropertyExtService.getDomainProperty(currentDomain, RABBITMQ_PLUGIN_QUEUE_OUT);
		if (StringUtils.isEmpty(queueValue)) {
			throw new DomibusPropertyExtException("Error getting the queue [" + RABBITMQ_PLUGIN_QUEUE_OUT + "]");
		}
		LOG.info("Sending message to queue [{}]", queueValue);
		rabbitTemplate.send(queueValue, createMessage(messageId));
	}

	@Override
	public void messageReceiveFailed(MessageReceiveFailureEvent messageReceiveFailureEvent) {
		LOG.debug("Handling messageReceiveFailed");
		final Message message = new ErrorMessageCreator(messageReceiveFailureEvent.getErrorResult(), 
				messageReceiveFailureEvent.getEndpoint(), NotificationType.MESSAGE_RECEIVED_FAILURE).createMessage();
		sendRabbitMQMessage(message, RABBITMQ_PLUGIN_QUEUE_CONSUMER_NOTIFICATION_ERROR);
	}

	@Override
	public void messageSendFailed(final String messageId) {
		List<ErrorResult> errors = super.getErrorsForMessage(messageId);
		final Message message = new ErrorMessageCreator(errors.get(errors.size() - 1), null, NotificationType.MESSAGE_SEND_FAILURE).createMessage();
		sendRabbitMQMessage(message, RABBITMQ_PLUGIN_QUEUE_PRODUCER_NOTIFICATION_ERROR);
	}

	@Override
	public void messageSendSuccess(String messageId) {
		LOG.debug("Handling messageSendSuccess");
		final Message rabbitmqMessageDTO = new SignalMessageCreator(messageId, NotificationType.MESSAGE_SEND_SUCCESS).createMessage();
		sendRabbitMQMessage(rabbitmqMessageDTO, RABBITMQ_PLUGIN_QUEUE_REPLY);
	}

	protected void sendRabbitMQMessage(Message message, String queueProperty) {
		final DomainDTO currentDomain = domainContextExtService.getCurrentDomain();
		final String queueValue = domibusPropertyExtService.getDomainProperty(currentDomain, queueProperty);
		if (StringUtils.isEmpty(queueValue)) {
			throw new DomibusPropertyExtException("Error getting the queue [" + queueProperty + "]");
		}
		LOG.info("Sending message to queue [{}]", queueValue);
		rabbitTemplate.send(queueValue, message);
	}

	@Override
	@MDCKey(DomibusLogger.MDC_MESSAGE_ID)
	public Message downloadMessage(String messageId, Message target) throws MessageNotFoundException {
		LOG.debug("Downloading message [{}]", messageId);
		try {
			Message result = this.getMessageRetrievalTransformer().transformFromSubmission(this.messageRetriever.downloadMessage(messageId), target);

			LOG.businessInfo(DomibusMessageCode.BUS_MESSAGE_RETRIEVED);
			return result;
		} catch (Exception ex) {
			LOG.businessError(DomibusMessageCode.BUS_MESSAGE_RETRIEVE_FAILED, ex);
			throw ex;
		}
	}

	public Message createMessage(String messageId) {
		final Message message = MessageBuilder.withBody(StringUtils.EMPTY.getBytes()).build();
		try {
			downloadMessage(messageId, message);
		} catch (final MessageNotFoundException e) {
			throw new DefaultRabbitmqPluginException("Unable to create push message", e);
		}
		final DomainDTO currentDomain = domainContextExtService.getCurrentDomain();
		message.getMessageProperties().setHeader(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, RabbitmqMessageConstants.MESSAGE_TYPE_INCOMING);
		message.getMessageProperties().setHeader(MessageConstants.DOMAIN, currentDomain.getCode());
		return message;
	}
}
