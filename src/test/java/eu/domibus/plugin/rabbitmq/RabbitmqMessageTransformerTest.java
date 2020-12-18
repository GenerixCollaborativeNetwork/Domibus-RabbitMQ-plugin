package eu.domibus.plugin.rabbitmq;

import eu.domibus.ext.services.DomainContextExtService;
import eu.domibus.ext.services.DomibusPropertyExtService;
import eu.domibus.ext.services.FileUtilExtService;
import eu.domibus.plugin.Submission;
import eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants;
import eu.domibus.plugin.rabbitmq.RabbitmqMessageTransformer;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import mockit.Tested;
import mockit.integration.junit4.JMockit;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;

import javax.activation.DataHandler;
import javax.mail.util.ByteArrayDataSource;

import static eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants.*;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Generix Group
 * Created by Arun Raj on 18/10/2016.
 */
@RunWith(JMockit.class)
public class RabbitmqMessageTransformerTest {

    private static final String MIME_TYPE = "MimeType";
    private static final String DEFAULT_MT = "text/xml";
    private static final String DOMIBUS_BLUE = "domibus-blue";
    private static final String DOMIBUS_RED = "domibus-red";
    private static final String INITIATOR_ROLE = "http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/initiator";
    private static final String RESPONDER_ROLE = "http://docs.oasis-open.org/ebxml-msg/ebms/v3.0/ns/core/200704/responder";
    private static final String PAYLOAD_ID = "cid:message";
    private static final String UNREGISTERED_PARTY_TYPE = "urn:oasis:names:tc:ebcore:partyid-type:unregistered";
    private static final String ORIGINAL_SENDER = "urn:oasis:names:tc:ebcore:partyid-type:unregistered:C1";
    private static final String FINAL_RECIPIENT = "urn:oasis:names:tc:ebcore:partyid-type:unregistered:C4";
    private static final String FINAL_RECIPIENT_TYPE = "iso6523-actorid-upis";
    private static final String ACTION_TC1LEG1 = "TC1Leg1";
    private static final String PROTOCOL_AS4 = "AS4";
    private static final String SERVICE_NOPROCESS = "bdx:noprocess";
    private static final String SERVICE_TYPE_TC1 = "tc1";
    private static final String PAYLOAD_FILENAME = "FileName";
    private static final String PAYLOAD_1_FILENAME = "payload_1_fileName";
    private static final String PAYLOAD_2_FILENAME = "payload_2_fileName";
    private static final String FILENAME_TEST = "09878378732323.payload";
    private static final String CUSTOM_AGREEMENT_REF = "customAgreement";


    @Injectable
    protected DomibusPropertyExtService domibusPropertyExtService;

    @Injectable
    protected DomainContextExtService domainContextExtService;

    @Injectable
    protected FileUtilExtService fileUtilExtService;

    @Tested
    RabbitmqMessageTransformer testObj = new RabbitmqMessageTransformer();

    /**
     * Testing basic happy flow scenario of the transform from submission of Rabbitmq transformer
     */
    @Test
    public void transformFromSubmission_HappyFlow() throws Exception {
        Submission submissionObj = new Submission();
        submissionObj.setAction(ACTION_TC1LEG1);
        submissionObj.setService(SERVICE_NOPROCESS);
        submissionObj.setServiceType(SERVICE_TYPE_TC1);
        submissionObj.setConversationId("123");
        submissionObj.setMessageId("1234");
        submissionObj.addFromParty(DOMIBUS_BLUE, UNREGISTERED_PARTY_TYPE);
        submissionObj.setFromRole(INITIATOR_ROLE);
        submissionObj.addToParty(DOMIBUS_RED, UNREGISTERED_PARTY_TYPE);
        submissionObj.setToRole(RESPONDER_ROLE);
        submissionObj.addMessageProperty(PROPERTY_ORIGINAL_SENDER, ORIGINAL_SENDER);
        submissionObj.addMessageProperty(PROPERTY_ENDPOINT, "http://localhost:8080/domibus/domibus-blue");
        submissionObj.addMessageProperty(PROPERTY_FINAL_RECIPIENT, FINAL_RECIPIENT, FINAL_RECIPIENT_TYPE);
        submissionObj.setAgreementRef("12345");
        submissionObj.setRefToMessageId("123456");

        String strPayLoad1 = "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPGhlbGxvPndvcmxkPC9oZWxsbz4=";
        DataHandler payLoadDataHandler1 = new DataHandler(new ByteArrayDataSource(strPayLoad1.getBytes(), DEFAULT_MT));

        String strPayLoad2 = "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPGhlbGxvPndvcmxkPC9oZWxsbz4=";
        DataHandler payLoadDataHandler2 = new DataHandler(new ByteArrayDataSource(strPayLoad2.getBytes(), DEFAULT_MT));


        Submission.TypedProperty objTypedProperty1 = new Submission.TypedProperty(MIME_TYPE, DEFAULT_MT);
        Submission.TypedProperty objTypedProperty2 = new Submission.TypedProperty(PAYLOAD_FILENAME, FILENAME_TEST);
        Collection<Submission.TypedProperty> listTypedProperty = new ArrayList<>();
        listTypedProperty.add(objTypedProperty1);
        listTypedProperty.add(objTypedProperty2);
        Submission.Payload objPayload1 = new Submission.Payload(PAYLOAD_ID, payLoadDataHandler1, listTypedProperty, false, null, null);
        submissionObj.addPayload(objPayload1);
        Submission.Payload objBodyload = new Submission.Payload("", payLoadDataHandler2, listTypedProperty, false, null, null);
        submissionObj.addPayload(objBodyload);


        Message message = MessageBuilder.withBody(StringUtils.EMPTY.getBytes()).build();
        message = testObj.transformFromSubmission(submissionObj, message);


        Assert.assertEquals(ACTION_TC1LEG1, message.getMessageProperties().getHeader(ACTION));
        Assert.assertEquals(SERVICE_NOPROCESS, message.getMessageProperties().getHeader(SERVICE));
        Assert.assertEquals(SERVICE_TYPE_TC1, message.getMessageProperties().getHeader(SERVICE_TYPE));
        Assert.assertEquals("123", message.getMessageProperties().getHeader(CONVERSATION_ID));
        Assert.assertEquals("1234", message.getMessageProperties().getHeader(MESSAGE_ID));
        Assert.assertEquals(DOMIBUS_BLUE, message.getMessageProperties().getHeader(FROM_PARTY_ID));
        Assert.assertEquals(UNREGISTERED_PARTY_TYPE, message.getMessageProperties().getHeader(FROM_PARTY_TYPE));
        Assert.assertEquals(INITIATOR_ROLE, message.getMessageProperties().getHeader(FROM_ROLE));
        Assert.assertEquals(DOMIBUS_RED, message.getMessageProperties().getHeader(TO_PARTY_ID));
        Assert.assertEquals(UNREGISTERED_PARTY_TYPE, message.getMessageProperties().getHeader(TO_PARTY_TYPE));
        Assert.assertEquals(RESPONDER_ROLE, message.getMessageProperties().getHeader(TO_ROLE));
        Assert.assertEquals(ORIGINAL_SENDER, message.getMessageProperties().getHeader(PROPERTY_ORIGINAL_SENDER));
        Assert.assertEquals(FINAL_RECIPIENT, message.getMessageProperties().getHeader(PROPERTY_FINAL_RECIPIENT));
        Assert.assertEquals("12345", message.getMessageProperties().getHeader(AGREEMENT_REF));
        Assert.assertEquals("123456", message.getMessageProperties().getHeader(REF_TO_MESSAGE_ID));
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.AGREEMENT_REF, "customAgreement");
        Assert.assertEquals("false", message.getMessageProperties().getHeader(P1_IN_BODY));

        File file = new File(FILENAME_TEST);
        Assert.assertEquals(file.getName(), message.getMessageProperties().getHeader(PAYLOAD_2_FILENAME));

    }

    /*
     * Testing basic happy flow scenario of the transform from messaging to submission of Rabbitmq transformer
     */
    @Test
    public void transformToSubmission_HappyFlow() throws Exception {
        Message message = MessageBuilder.withBody(StringUtils.EMPTY.getBytes()).build();
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, "submitMessage");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.SERVICE, SERVICE_NOPROCESS);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.SERVICE_TYPE, SERVICE_TYPE_TC1);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.ACTION, ACTION_TC1LEG1);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.FROM_PARTY_ID, DOMIBUS_BLUE);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.FROM_PARTY_TYPE, UNREGISTERED_PARTY_TYPE);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TO_PARTY_ID, DOMIBUS_RED);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TO_PARTY_TYPE, UNREGISTERED_PARTY_TYPE);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.FROM_ROLE, INITIATOR_ROLE);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TO_ROLE, RESPONDER_ROLE);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROPERTY_ORIGINAL_SENDER, ORIGINAL_SENDER);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROPERTY_FINAL_RECIPIENT, FINAL_RECIPIENT);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROPERTY_FINAL_RECIPIENT_TYPE, FINAL_RECIPIENT_TYPE);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROTOCOL, PROTOCOL_AS4);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.AGREEMENT_REF, "customAgreement");
        message.getMessageProperties().setHeader(PAYLOAD_1_FILENAME, FILENAME_TEST);

        message.getMessageProperties().setCorrelationId("12345");

        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TOTAL_NUMBER_OF_PAYLOADS, 1);
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_MIME_CONTENT_ID_FORMAT, 1), PAYLOAD_ID);
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_MIME_TYPE_FORMAT, 1), DEFAULT_MT);
        String pay1 = "http://test";
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_NAME_FORMAT, 1), pay1);

        Submission objSubmission = testObj.transformToSubmission(message);
        Assert.assertNotNull(objSubmission);
        Assert.assertEquals(SERVICE_NOPROCESS, objSubmission.getService());
        Assert.assertEquals(SERVICE_TYPE_TC1, objSubmission.getServiceType());
        Assert.assertEquals(ACTION_TC1LEG1, objSubmission.getAction());
        for (Submission.Party objFromParty : objSubmission.getFromParties()) {
            Assert.assertEquals(DOMIBUS_BLUE, objFromParty.getPartyId());
            Assert.assertEquals(UNREGISTERED_PARTY_TYPE, objFromParty.getPartyIdType());
        }
        Assert.assertEquals(INITIATOR_ROLE, objSubmission.getFromRole());

        for (Submission.Party objToParty : objSubmission.getToParties()) {
            Assert.assertEquals(DOMIBUS_RED, objToParty.getPartyId());
            Assert.assertEquals(UNREGISTERED_PARTY_TYPE, objToParty.getPartyIdType());
        }
        Assert.assertEquals(RESPONDER_ROLE, objSubmission.getToRole());

        for (Submission.TypedProperty objTypedProperty : objSubmission.getMessageProperties()) {
            if (PROPERTY_ORIGINAL_SENDER.equalsIgnoreCase(objTypedProperty.getKey())) {
                Assert.assertEquals(ORIGINAL_SENDER, objTypedProperty.getValue());
            }
            if (PROPERTY_FINAL_RECIPIENT.equalsIgnoreCase(objTypedProperty.getKey())) {
                Assert.assertEquals(FINAL_RECIPIENT, objTypedProperty.getValue());
                Assert.assertEquals(FINAL_RECIPIENT_TYPE, objTypedProperty.getType());
            }
        }

        for (Submission.Payload objPayLoad : objSubmission.getPayloads()) {
            for (Submission.TypedProperty objTypedProperty : objPayLoad.getPayloadProperties()) {
                if (MIME_TYPE.equalsIgnoreCase(objTypedProperty.getKey())) {
                    Assert.assertEquals(DEFAULT_MT, objTypedProperty.getValue());
                }
                if (PAYLOAD_FILENAME.equalsIgnoreCase(objTypedProperty.getKey())) {
                    Assert.assertEquals(FILENAME_TEST, objTypedProperty.getValue());
                }
            }
        }
    }

    /*
     * Testing for bug EDELIVERY-1371, trimming whitespaces in the transform from UserMessage to Submission of Rabbitmq transformer
     */
    @Test
    public void transformToSubmission_TrimWhiteSpaces() throws Exception {
        Message message = MessageBuilder.withBody(StringUtils.EMPTY.getBytes()).build();
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, "submitMessage");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.SERVICE, "\t" + SERVICE_NOPROCESS + "   ");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.SERVICE_TYPE, "\t" + SERVICE_TYPE_TC1 + "    ");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.ACTION, "    " + ACTION_TC1LEG1 + "\t");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.FROM_PARTY_ID, '\t' + DOMIBUS_BLUE + "\t");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.FROM_PARTY_TYPE, "   " + UNREGISTERED_PARTY_TYPE + '\t');
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TO_PARTY_ID, "\t" + DOMIBUS_RED + "\t");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TO_PARTY_TYPE, "   " + UNREGISTERED_PARTY_TYPE + "\t");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.FROM_ROLE, "    " + INITIATOR_ROLE + "\t");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TO_ROLE, '\t' + RESPONDER_ROLE + "   ");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROPERTY_ORIGINAL_SENDER, "\t" + ORIGINAL_SENDER + "    ");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROPERTY_FINAL_RECIPIENT, "\t" + FINAL_RECIPIENT + "\t");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROTOCOL, "\t" + PROTOCOL_AS4 + "\t\t");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.AGREEMENT_REF, CUSTOM_AGREEMENT_REF);

        message.getMessageProperties().setCorrelationId("12345");

        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TOTAL_NUMBER_OF_PAYLOADS, 1);
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_MIME_CONTENT_ID_FORMAT, 1), "\t" + PAYLOAD_ID + "   ");
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_MIME_TYPE_FORMAT, 1), "   " + DEFAULT_MT + "\t\t");
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_NAME_FORMAT, 1), "http://test");

        Submission objSubmission = testObj.transformToSubmission(message);
        Assert.assertNotNull("Submission object in the response should not be null:", objSubmission);
        for (Iterator<Submission.Party> itr = objSubmission.getFromParties().iterator(); itr.hasNext(); ) {
            Submission.Party fromPartyObj = itr.next();
            Assert.assertEquals(DOMIBUS_BLUE, fromPartyObj.getPartyId());
            Assert.assertEquals(UNREGISTERED_PARTY_TYPE, fromPartyObj.getPartyIdType());
        }
        Assert.assertEquals(INITIATOR_ROLE, objSubmission.getFromRole());

        for (Iterator<Submission.Party> itr = objSubmission.getToParties().iterator(); itr.hasNext(); ) {
            Submission.Party toPartyObj = itr.next();
            Assert.assertEquals(DOMIBUS_RED, toPartyObj.getPartyId());
            Assert.assertEquals(UNREGISTERED_PARTY_TYPE, toPartyObj.getPartyIdType());
        }
        Assert.assertEquals(RESPONDER_ROLE, objSubmission.getToRole());

        Assert.assertEquals(SERVICE_NOPROCESS, objSubmission.getService());
        Assert.assertEquals(SERVICE_TYPE_TC1, objSubmission.getServiceType());
        Assert.assertEquals(ACTION_TC1LEG1, objSubmission.getAction());
    }

    /*
     * Testing for bug EDELIVERY-1386, fallback to defaults for missing properties
     */
    @Test
    public void transformToSubmission_FallbackToDefaults() throws Exception {

        new NonStrictExpectations(testObj) {{
            testObj.getProperty(RabbitmqMessageConstants.SERVICE);
            result = SERVICE_NOPROCESS;

            testObj.getProperty(RabbitmqMessageConstants.SERVICE_TYPE);
            result = SERVICE_TYPE_TC1;

            testObj.getProperty(RabbitmqMessageConstants.ACTION);
            result = ACTION_TC1LEG1;

            testObj.getProperty(RabbitmqMessageConstants.FROM_ROLE);
            result = INITIATOR_ROLE;

            testObj.getProperty(RabbitmqMessageConstants.TO_ROLE);
            result = RESPONDER_ROLE;

            testObj.getProperty(RabbitmqMessageConstants.FROM_PARTY_ID);
            result = DOMIBUS_BLUE;

            testObj.getProperty(RabbitmqMessageConstants.FROM_PARTY_TYPE);
            result = UNREGISTERED_PARTY_TYPE;

            testObj.getProperty(RabbitmqMessageConstants.TO_PARTY_ID);
            result = DOMIBUS_RED;

            testObj.getProperty(RabbitmqMessageConstants.TO_PARTY_TYPE);
            result = UNREGISTERED_PARTY_TYPE;

            testObj.getProperty(RabbitmqMessageConstants.AGREEMENT_REF);
            result = CUSTOM_AGREEMENT_REF;
        }};


        Message message = MessageBuilder.withBody(StringUtils.EMPTY.getBytes()).build();
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.RABBITMQ_BACKEND_MESSAGE_TYPE_PROPERTY_KEY, "submitMessage");
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROPERTY_ORIGINAL_SENDER, ORIGINAL_SENDER);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROPERTY_FINAL_RECIPIENT, FINAL_RECIPIENT);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROPERTY_FINAL_RECIPIENT_TYPE, FINAL_RECIPIENT_TYPE);
        message.getMessageProperties().setHeader(RabbitmqMessageConstants.PROTOCOL, PROTOCOL_AS4);

        message.getMessageProperties().setCorrelationId("12345");

        message.getMessageProperties().setHeader(RabbitmqMessageConstants.TOTAL_NUMBER_OF_PAYLOADS, 1);
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_MIME_CONTENT_ID_FORMAT, 1), PAYLOAD_ID);
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_MIME_TYPE_FORMAT, 1), DEFAULT_MT);
        String pay1 = "https://www.baeldung.com/jmockit-101";
        // byte[] payload = pay1.getBytes();
        // message.setBytes(MessageFormat.format(PAYLOAD_NAME_FORMAT, 1), payload);
        message.getMessageProperties().setHeader(MessageFormat.format(PAYLOAD_NAME_FORMAT, 1), pay1);

        Submission objSubmission = testObj.transformToSubmission(message);
        Assert.assertNotNull("Submission object in the response should not be null:", objSubmission);
        for (Iterator<Submission.Party> itr = objSubmission.getFromParties().iterator(); itr.hasNext(); ) {
            Submission.Party fromPartyObj = itr.next();
            Assert.assertEquals(DOMIBUS_BLUE, fromPartyObj.getPartyId());
            Assert.assertEquals(UNREGISTERED_PARTY_TYPE, fromPartyObj.getPartyIdType());
        }
        Assert.assertEquals(INITIATOR_ROLE, objSubmission.getFromRole());

        for (Iterator<Submission.Party> itr = objSubmission.getToParties().iterator(); itr.hasNext(); ) {
            Submission.Party toPartyObj = itr.next();
            Assert.assertEquals(DOMIBUS_RED, toPartyObj.getPartyId());
            Assert.assertEquals(UNREGISTERED_PARTY_TYPE, toPartyObj.getPartyIdType());
        }
        Assert.assertEquals(RESPONDER_ROLE, objSubmission.getToRole());

        Assert.assertEquals(SERVICE_NOPROCESS, objSubmission.getService());
        Assert.assertEquals(SERVICE_TYPE_TC1, objSubmission.getServiceType());
        Assert.assertEquals(ACTION_TC1LEG1, objSubmission.getAction());
        Assert.assertEquals(CUSTOM_AGREEMENT_REF, objSubmission.getAgreementRef());
    }

}