package eu.domibus.plugin.rabbitmq;

import eu.domibus.ext.domain.DomainDTO;
import eu.domibus.ext.services.DomainContextExtService;
import eu.domibus.ext.services.DomibusPropertyExtService;
import eu.domibus.ext.services.FileUtilExtService;
import eu.domibus.logging.DomibusLogger;
import eu.domibus.logging.DomibusLoggerFactory;
import eu.domibus.messaging.MessageConstants;
import eu.domibus.plugin.Submission;
import eu.domibus.plugin.transformer.MessageRetrievalTransformer;
import eu.domibus.plugin.transformer.MessageSubmissionTransformer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.activation.DataHandler;
import javax.activation.URLDataSource;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;

import javax.mail.util.ByteArrayDataSource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;

import static eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants.*;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.trim;

/**
 * This class is responsible for transformations from {@link org.springframework.amqp.core.Message}
 * to {@link eu.domibus.plugin.Submission} and vice versa
 *
 * @author Padraig McGourty, Christian Koch, Stefan Mueller
 * @author Cosmin Baciu, Generix Group
 */

@Service("rabbitmqMessageTransformer")
public class RabbitmqMessageTransformer implements MessageRetrievalTransformer<Message>, MessageSubmissionTransformer<Message> {

	private static final DomibusLogger LOG = DomibusLoggerFactory.getLogger(RabbitmqMessageTransformer.class);

	@Autowired
	protected DomibusPropertyExtService domibusPropertyExtService;

	@Autowired
	protected DomainContextExtService domainContextExtService;

	@Autowired
	protected FileUtilExtService fileUtilExtService;

	/**
	 * Transforms {@link eu.domibus.plugin.Submission} to
	 * {@link org.springframework.amqp.core.Message}
	 *
	 * @param submission
	 *            the message to be transformed *
	 * @return result of the transformation as {@link org.springframework.amqp.core.Message}
	 */
	@Override
	public Message transformFromSubmission(final Submission submission, final Message messageOut) {
		try {
			if (submission.getMpc() != null) {
				messageOut.getMessageProperties().setHeader(MPC, submission.getMpc());
			}
			messageOut.getMessageProperties().setHeader(ACTION, submission.getAction());
			messageOut.getMessageProperties().setHeader(SERVICE, submission.getService());
			messageOut.getMessageProperties().setHeader(SERVICE_TYPE, submission.getServiceType());
			messageOut.getMessageProperties().setHeader(CONVERSATION_ID, submission.getConversationId());
			messageOut.getMessageProperties().setHeader(MESSAGE_ID, submission.getMessageId());

			for (final Submission.Party fromParty : submission.getFromParties()) {
				messageOut.getMessageProperties().setHeader(FROM_PARTY_ID, fromParty.getPartyId());
				messageOut.getMessageProperties().setHeader(FROM_PARTY_TYPE, fromParty.getPartyIdType());
			}
			messageOut.getMessageProperties().setHeader(FROM_ROLE, submission.getFromRole());

			for (final Submission.Party toParty : submission.getToParties()) {
				messageOut.getMessageProperties().setHeader(TO_PARTY_ID, toParty.getPartyId());
				messageOut.getMessageProperties().setHeader(TO_PARTY_TYPE, toParty.getPartyIdType());
			}
			messageOut.getMessageProperties().setHeader(TO_ROLE, submission.getToRole());

			for (final Submission.TypedProperty p : submission.getMessageProperties()) {
				if (p.getKey().equals(PROPERTY_ORIGINAL_SENDER)) {
					messageOut.getMessageProperties().setHeader(PROPERTY_ORIGINAL_SENDER, p.getValue());
					continue;
				}
				if (p.getKey().equals(PROPERTY_ENDPOINT)) {
					messageOut.getMessageProperties().setHeader(PROPERTY_ENDPOINT, p.getValue());
					continue;
				}
				if (p.getKey().equals(PROPERTY_FINAL_RECIPIENT)) {
					messageOut.getMessageProperties().setHeader(PROPERTY_FINAL_RECIPIENT, p.getValue());
					continue;
				}
				// only reached if none of the predefined properties are set
				messageOut.getMessageProperties().setHeader(PROPERTY_PREFIX + p.getKey(), p.getValue());
				messageOut.getMessageProperties().setHeader(PROPERTY_TYPE_PREFIX + p.getKey(), p.getType());
			}
			messageOut.getMessageProperties().setHeader(PROTOCOL, "AS4");
			messageOut.getMessageProperties().setHeader(AGREEMENT_REF, submission.getAgreementRef());
			messageOut.getMessageProperties().setHeader(REF_TO_MESSAGE_ID, submission.getRefToMessageId());

			// save the first payload (payload_1) for the bodyload (if exists)
			int counter = 1;
			for (final Submission.Payload p : submission.getPayloads()) {
				if (p.isInBody()) {
					counter = 2;
					break;
				}
			}

			final boolean putAttachmentsInQueue = Boolean.parseBoolean(getProperty(PUT_ATTACHMENTS_IN_QUEUE, "true")); // Used
																														// to
																														// be
																														// set
																														// to
																														// true
			for (final Submission.Payload p : submission.getPayloads()) {
				// counter is increased for payloads (not for bodyload which is
				// always set to payload_1)
				counter = transformFromSubmissionHandlePayload(messageOut, putAttachmentsInQueue, counter, p);
			}
			messageOut.getMessageProperties().setHeader(TOTAL_NUMBER_OF_PAYLOADS, submission.getPayloads().size());
		} catch (final IOException ex) {
			LOG.error("Error while filling the Message", ex);
			throw new DefaultRabbitmqPluginException(ex);
		}

		return messageOut;
	}

	protected String getProperty(String propertyName) {
		return getProperty(propertyName, null);
	}

	protected String getProperty(String propertyName, String defaultValue) {
		final DomainDTO currentDomain = domainContextExtService.getCurrentDomain();
		return domibusPropertyExtService.getDomainProperty(currentDomain, RABBITMQ_PLUGIN_PROPERTY_PREFIX + "." + propertyName, defaultValue);
	}

	private int transformFromSubmissionHandlePayload(Message messageOut, boolean putAttachmentsInQueue, int counter, Submission.Payload p) throws IOException {
		MessageProperties properties = messageOut.getMessageProperties();

		if (p.isInBody()) {
			if (p.getPayloadDatahandler() != null) {
				messageOut = MessageBuilder.withBody(IOUtils.toByteArray(p.getPayloadDatahandler().getInputStream())).andProperties(properties).build();
				properties = messageOut.getMessageProperties();
				properties.setHeader(P1_IN_BODY, "true");
			}
			properties.setHeader(MessageFormat.format(PAYLOAD_MIME_TYPE_FORMAT, 1), findMime(p.getPayloadProperties()));
			properties.setHeader(MessageFormat.format(PAYLOAD_MIME_CONTENT_ID_FORMAT, 1), p.getContentId());
		} else {
			properties.setHeader(P1_IN_BODY, "false");
			final String payContID = String.valueOf(MessageFormat.format(PAYLOAD_MIME_CONTENT_ID_FORMAT, counter));
			final String propPayload = String.valueOf(MessageFormat.format(PAYLOAD_NAME_FORMAT, counter));
			final String payMimeTypeProp = String.valueOf(MessageFormat.format(PAYLOAD_MIME_TYPE_FORMAT, counter));
			final String payFileNameProp = String.valueOf(MessageFormat.format(PAYLOAD_FILE_NAME_FORMAT, counter));
			if (p.getPayloadDatahandler() != null) {
				if (putAttachmentsInQueue) {
					if (counter == 1)
						LOG.error("putAttachmentsInQueue is not supported yet");
					else {
						messageOut = MessageBuilder.fromClonedMessage(messageOut).withBody(IOUtils.toByteArray(p.getPayloadDatahandler().getInputStream()))
								.build();
					}
				} else {
					LOG.debug("putAttachmentsInQueue is false");
					messageOut.getMessageProperties().setHeader(payFileNameProp, findFilename(p.getPayloadProperties()));
				}
			}
			properties.setHeader(payMimeTypeProp, findMime(p.getPayloadProperties()));
			properties.setHeader(payContID, p.getContentId());
			counter++;
		}
		return counter;
	}

	private String findElement(String element, Collection<Submission.TypedProperty> props) {
		for (Submission.TypedProperty prop : props) {
			if (element.equals(prop.getKey()) && isEmpty(trim(prop.getType()))) {
				return prop.getValue();
			}
		}
		return null;
	}

	private String findMime(Collection<Submission.TypedProperty> props) {
		return findElement(MIME_TYPE, props);
	}

	private String findFilename(Collection<Submission.TypedProperty> props) {
		return findElement(PAYLOAD_FILENAME, props);
	}

	/**
	 * Transforms {@link org.springframework.amqp.core.Message} to
	 * {@link eu.domibus.plugin.Submission}
	 *
	 * @param messageIn
	 *            the message ({@link org.springframework.amqp.core.Message}) to be tranformed
	 * @return the result of the transformation as
	 *         {@link eu.domibus.plugin.Submission}
	 */
	@Override
	public Submission transformToSubmission(final Message messageIn) {
		final Submission target = new Submission();

		String mpc = trim(messageIn.getMessageProperties().getHeader(MPC));
		if (!isEmpty(mpc)) {
			target.setMpc(mpc);
		}
		target.setMessageId(trim(messageIn.getMessageProperties().getHeader(MESSAGE_ID)));

		setTargetFromPartyIdAndFromPartyType(messageIn, target);

		target.setFromRole(getPropertyWithFallback(messageIn, FROM_ROLE));

		setTargetToPartyIdAndToPartyType(messageIn, target);

		target.setToRole(getPropertyWithFallback(messageIn, TO_ROLE));

		target.setAction(getPropertyWithFallback(messageIn, ACTION));

		target.setService(getPropertyWithFallback(messageIn, SERVICE));

		target.setServiceType(getPropertyWithFallback(messageIn, SERVICE_TYPE));

		target.setAgreementRef(getPropertyWithFallback(messageIn, AGREEMENT_REF));

		target.setConversationId(trim(messageIn.getMessageProperties().getHeader(CONVERSATION_ID)));

		// not part of ebMS3, eCODEX legacy property
		String strOriginalSender = trim(messageIn.getMessageProperties().getHeader(PROPERTY_ORIGINAL_SENDER));
		if (!isEmpty(strOriginalSender)) {
			target.addMessageProperty(PROPERTY_ORIGINAL_SENDER, strOriginalSender);
		}

		String endpoint = trim(messageIn.getMessageProperties().getHeader(PROPERTY_ENDPOINT));
		if (!isEmpty(endpoint)) {
			target.addMessageProperty(PROPERTY_ENDPOINT, messageIn.getMessageProperties().getHeader(PROPERTY_ENDPOINT));
		}

		// not part of ebMS3, eCODEX legacy property
		String strFinalRecipient = trim(messageIn.getMessageProperties().getHeader(PROPERTY_FINAL_RECIPIENT));

		String strFinalRecipientType = trim(messageIn.getMessageProperties().getHeader(PROPERTY_FINAL_RECIPIENT_TYPE));

		LOG.debug("FinalRecipient [{}] and FinalRecipientType [{}] properties from Message", strFinalRecipient, strFinalRecipientType);

		if (!isEmpty(strFinalRecipient)) {
			target.addMessageProperty(PROPERTY_FINAL_RECIPIENT, strFinalRecipient, strFinalRecipientType);
		}

		target.setRefToMessageId(trim(messageIn.getMessageProperties().getHeader(REF_TO_MESSAGE_ID)));

		final int numPayloads = messageIn.getMessageProperties().getHeader(TOTAL_NUMBER_OF_PAYLOADS);

		Set<String> allProps = messageIn.getMessageProperties().getHeaders().keySet();
		for (String key : allProps) {
			if (key.startsWith(PROPERTY_PREFIX)) {
				target.addMessageProperty(key.substring(PROPERTY_PREFIX.length()), trim(messageIn.getMessageProperties().getHeader(key)),
						trim(messageIn.getMessageProperties().getHeader(PROPERTY_TYPE_PREFIX + key.substring(PROPERTY_PREFIX.length()))));
			}
		}

		String bodyloadEnabled = getPropertyWithFallback(messageIn, RabbitmqMessageConstants.P1_IN_BODY);
		for (int i = 1; i <= numPayloads; i++) {
			transformToSubmissionHandlePayload(messageIn, target, bodyloadEnabled, i);
		}

		return target;
	}

	private String getPropertyWithFallback(final Message messageIn, String propName) {
		String propValue = null;

		propValue = trim(messageIn.getMessageProperties().getHeader(propName));
		if (isEmpty(propValue)) {
			propValue = getProperty(propName);
		}

		return propValue;
	}

	private void setTargetToPartyIdAndToPartyType(Message messageIn, Submission target) {
		String toPartyID = getPropertyWithFallback(messageIn, TO_PARTY_ID);
		String toPartyType = getPropertyWithFallback(messageIn, TO_PARTY_TYPE);
		LOG.debug("To Party Id  [{}] and Type [{}]", toPartyID, toPartyType);
		if (toPartyID != null) {
			target.addToParty(toPartyID, toPartyType);
		}
	}

	private void setTargetFromPartyIdAndFromPartyType(Message messageIn, Submission target) {
		String fromPartyID = getPropertyWithFallback(messageIn, FROM_PARTY_ID);
		String fromPartyType = getPropertyWithFallback(messageIn, FROM_PARTY_TYPE);
		LOG.debug("From Party Id  [{}] and Type [{}]", fromPartyID, fromPartyType);
		target.addFromParty(fromPartyID, fromPartyType);
	}

	private void transformToSubmissionHandlePayload(Message messageIn, Submission target, String bodyloadEnabled, int i) {
		final String propPayload = String.valueOf(MessageFormat.format(PAYLOAD_NAME_FORMAT, i));

		final String contentId;
		final String mimeType;
		String fileName;
		String payloadName;

		final String payMimeTypeProp = MessageFormat.format(PAYLOAD_MIME_TYPE_FORMAT, i);
		mimeType = trim(messageIn.getMessageProperties().getHeader(payMimeTypeProp));
		final String payFileNameProp = MessageFormat.format(PAYLOAD_FILE_NAME_FORMAT, i);
		fileName = fileUtilExtService.sanitizeFileName(trim(messageIn.getMessageProperties().getHeader(payFileNameProp)));
		final String payloadNameProperty = MessageFormat.format(RABBITMQ_PAYLOAD_NAME_FORMAT, i);
		payloadName = fileUtilExtService.sanitizeFileName(trim(messageIn.getMessageProperties().getHeader(payloadNameProperty)));
		final String payContID = MessageFormat.format(PAYLOAD_MIME_CONTENT_ID_FORMAT, i);
		contentId = trim(messageIn.getMessageProperties().getHeader(payContID));
		final Collection<Submission.TypedProperty> partProperties = new ArrayList<>();
		if (mimeType != null && !mimeType.trim().equals("")) {
			partProperties.add(new Submission.TypedProperty(MIME_TYPE, mimeType));
		}
		if (fileName != null && !fileName.trim().equals("")) {
			partProperties.add(new Submission.TypedProperty(PAYLOAD_FILENAME, fileName));
		}
		if (StringUtils.isNotBlank(payloadName)) {
			partProperties.add(new Submission.TypedProperty(MessageConstants.PAYLOAD_PROPERTY_FILE_NAME, payloadName));
		}
		DataHandler payloadDataHandler;
		if (i == 1)
			payloadDataHandler = new DataHandler(new ByteArrayDataSource(messageIn.getBody(), mimeType));
		else {
			try {
				payloadDataHandler = new DataHandler(new URLDataSource(new URL(messageIn.getMessageProperties().getHeader(propPayload))));
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException(propPayload + " neither available as byte[] or URL, aborting transformation");
			}
		}

		boolean inBody = (i == 1 && "true".equalsIgnoreCase(bodyloadEnabled));

		target.addPayload(contentId, payloadDataHandler, partProperties, inBody, null, null);
	}

}
