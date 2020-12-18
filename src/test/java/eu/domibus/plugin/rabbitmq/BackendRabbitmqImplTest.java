package eu.domibus.plugin.rabbitmq;

import eu.domibus.common.ErrorResult;
import eu.domibus.common.ErrorResultImpl;
import eu.domibus.common.MessageReceiveFailureEvent;
import eu.domibus.common.NotificationType;
import eu.domibus.ext.services.DomainContextExtService;
import eu.domibus.ext.services.DomibusPropertyExtService;
import eu.domibus.ext.services.JMSExtService;
import eu.domibus.ext.services.MessageExtService;
import eu.domibus.plugin.handler.MessagePuller;
import eu.domibus.plugin.handler.MessageRetriever;
import eu.domibus.plugin.handler.MessageSubmitter;
import mockit.*;
import mockit.integration.junit4.JMockit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jms.core.JmsOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Cosmin Baciu, Generix Group
 * @since 3.3
 */
@RunWith(JMockit.class)
public class BackendRabbitmqImplTest {

	@Injectable
	protected MessageRetriever messageRetriever;

	@Injectable
	protected MessageSubmitter messageSubmitter;

	@Injectable
	protected MessagePuller messagePuller;

	@Injectable
	private JmsOperations replyJmsTemplate;

	@Injectable
	private JmsOperations mshToBackendTemplate;

	@Injectable
	private JmsOperations errorNotifyConsumerTemplate;

	@Injectable
	private JmsOperations errorNotifyProducerTemplate;

	@Injectable
	protected JMSExtService jmsService;

	@Injectable
	protected DomibusPropertyExtService domibusPropertyService;

	@Injectable
	protected DomainContextExtService domainContextService;

	@Injectable
	private MessageExtService messageExtService;

	@Injectable
	private RabbitTemplate amqpTemplate;

	@Injectable
	String name = "myRabbitmqplugin";

	@Tested
	BackendRabbitmqImpl backendRabbitmq;

	@Test
	public void testReceiveMessage(@Injectable final Message message) throws Exception {
		final String messageId = "1";
		final String correlationId = "2";
		final String messageTypeSubmit = RabbitmqMessageConstants.MESSAGE_TYPE_SUBMIT;
		final MessageProperties messageProperties = MessagePropertiesBuilder.newInstance()
				.setHeader(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, messageTypeSubmit).setMessageId(messageId)
				.setCorrelationId(correlationId).build();

		new Expectations(backendRabbitmq) {
			{
				message.getMessageProperties();
				result = messageProperties;

				messageExtService.cleanMessageIdentifier(messageId);
				result = messageId;

				backendRabbitmq.submit(message);
				result = messageId;

				backendRabbitmq.sendReplyMessage(messageId, anyString, correlationId);
			}
		};

		backendRabbitmq.receiveMessage(message);

		new Verifications() {
			{
				backendRabbitmq.submit(message);

				String capturedMessageId = null;
				String capturedCorrelationId = null;
				String capturedErrorMessage = null;
				backendRabbitmq.sendReplyMessage(capturedMessageId = withCapture(), capturedCorrelationId = withCapture(),
						capturedCorrelationId = withCapture());

				assertEquals(capturedMessageId, messageId);
				assertEquals(capturedCorrelationId, correlationId);
			}
		};
	}

	@Test
	public void testReceiveMessage_MessageId_WithEmptySpaces(@Injectable final Message message) throws Exception {
		final String messageId = " test123 ";
		final String messageIdTrimmed = "test123";
		final String correlationId = "2";
		final String messageTypeSubmit = RabbitmqMessageConstants.MESSAGE_TYPE_SUBMIT;
		final MessageProperties messageProperties = MessagePropertiesBuilder.newInstance()
				.setHeader(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, messageTypeSubmit).setMessageId(messageId)
				.setCorrelationId(correlationId).build();

		new Expectations(backendRabbitmq) {
			{
				message.getMessageProperties();
				result = messageProperties;

				messageExtService.cleanMessageIdentifier(messageId);
				result = messageIdTrimmed;

				backendRabbitmq.submit(message);
				result = messageIdTrimmed;

				backendRabbitmq.sendReplyMessage(anyString, anyString, correlationId);
			}
		};

		backendRabbitmq.receiveMessage(message);

		new Verifications() {
			{
				backendRabbitmq.submit(message);

				String capturedMessageId;
				String capturedCorrelationId;
				String capturedErrorMessage;
				backendRabbitmq.sendReplyMessage(capturedMessageId = withCapture(), capturedErrorMessage = withCapture(),
						capturedCorrelationId = withCapture());

				assertEquals(capturedMessageId, messageIdTrimmed);
				assertEquals(capturedCorrelationId, correlationId);
				assertNull(capturedErrorMessage);
			}
		};
	}

	@Test
	public void testReceiveMessageWithUnacceptedMessageType(@Injectable final Message message) throws Exception {
		final String messageId = "1";
		final String correlationId = "2";
		final String unacceptedMessageType = "unacceptedMessageType";
		final MessageProperties messageProperties = MessagePropertiesBuilder.newInstance()
				.setHeader(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, unacceptedMessageType).setMessageId(messageId)
				.setCorrelationId(correlationId).build();

		new Expectations(backendRabbitmq) {
			{
				message.getMessageProperties();
				result = messageProperties;

				messageExtService.cleanMessageIdentifier(messageId);
				result = messageId;

				backendRabbitmq.sendReplyMessage(messageId, anyString, correlationId);
			}
		};

		backendRabbitmq.receiveMessage(message);

		new Verifications() {
			{
				String capturedMessageId = null;
				String capturedCorrelationId = null;
				String capturedErrorMessage = null;
				backendRabbitmq.sendReplyMessage(capturedMessageId = withCapture(), capturedErrorMessage = withCapture(),
						capturedCorrelationId = withCapture());

				assertEquals(capturedMessageId, messageId);
				assertEquals(capturedCorrelationId, correlationId);
				assertNotNull(capturedErrorMessage);
			}
		};
	}

	@Test
	public void testMessageReceiveFailed(@Mocked ErrorMessageCreator errorMessageCreator) throws Exception {
		MessageReceiveFailureEvent event = new MessageReceiveFailureEvent();
		final ErrorResult errorResult = new ErrorResultImpl();
		event.setErrorResult(errorResult);
		final String myEndpoint = "myEndpoint";
		event.setEndpoint(myEndpoint);
		final String messageId = "1";
		event.setMessageId(messageId);
		final SignalMessageCreator smc = new SignalMessageCreator(messageId, NotificationType.MESSAGE_SEND_SUCCESS);

		new Expectations(backendRabbitmq) {
			{
				backendRabbitmq.sendRabbitMQMessage(withAny(smc.createMessage()), anyString);
			}
		};

		backendRabbitmq.messageReceiveFailed(event);

		new Verifications() {
			{
				withCapture(new ErrorMessageCreator(errorResult, myEndpoint, NotificationType.MESSAGE_RECEIVED_FAILURE));

			}
		};
	}
}
