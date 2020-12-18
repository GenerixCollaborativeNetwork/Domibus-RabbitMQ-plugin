package eu.domibus.plugin.rabbitmq;

import eu.domibus.ext.services.AuthenticationExtService;
import eu.domibus.ext.services.DomainContextExtService;
import eu.domibus.ext.services.DomibusPropertyExtService;
import eu.domibus.logging.DomibusLogger;
import eu.domibus.plugin.rabbitmq.BackendRabbitmqImpl;
import eu.domibus.plugin.rabbitmq.BackendRabbitmqReceivingListener;
import eu.domibus.plugin.rabbitmq.DefaultRabbitmqPluginException;
import eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants;
import mockit.*;
import mockit.integration.junit4.JMockit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessagePropertiesBuilder;

/**
 * @author Cosmin Baciu, Generix Group
 * @since 4.1
 */
@RunWith(JMockit.class)
public class BackendRabbitmqReceivingListenerTest {

    @Injectable
    protected BackendRabbitmqImpl backendRabbitmq;

    @Injectable
    protected DomibusPropertyExtService domibusPropertyExtService;

    @Injectable
    protected DomainContextExtService domainContextExtService;

    @Injectable
    protected AuthenticationExtService authenticationExtService;

    @Tested
    BackendRabbitmqReceivingListener backendRabbitmqReceivingListener;

    @Test
    public void receiveMessage(@Injectable Message message, @Mocked DomibusLogger LOG) {
        new Expectations(backendRabbitmqReceivingListener) {{
            authenticationExtService.isUnsecureLoginAllowed();
            result = false;

            backendRabbitmqReceivingListener.authenticate(message);
        }};
        backendRabbitmqReceivingListener.onMessage(message);

        new FullVerificationsInOrder() {{
            LOG.debug("Performing authentication");
            LOG.clearCustomKeys();
            backendRabbitmqReceivingListener.authenticate(message);
            backendRabbitmq.receiveMessage(message);
        }};
    }

    @Test
    public void authenticate(@Injectable Message message, @Mocked DomibusLogger LOG) {
        String username = "cosmin";
        String password = "mypass";        
        final MessageProperties messageProperties = MessagePropertiesBuilder.newInstance()
        		.setHeader(RabbitmqMessageConstants.USERNAME, username)
        		.setHeader(RabbitmqMessageConstants.PASSWORD, password)
        		.build();
        
        new Expectations() {{
            message.getMessageProperties();
            result = messageProperties;
        }};

        backendRabbitmqReceivingListener.authenticate(message);

        new FullVerifications() {{
            authenticationExtService.basicAuthenticate(username, password);
        }};
    }

    @Test(expected = DefaultRabbitmqPluginException.class)
    public void authenticateWithMissingUsername(@Injectable Message message, @Mocked DomibusLogger LOG) {
        final MessageProperties messageProperties = MessagePropertiesBuilder.newInstance().build();
    	
    	new Expectations() {{
            message.getMessageProperties();
            result = messageProperties;
        }};

        backendRabbitmqReceivingListener.authenticate(message);

        new FullVerifications() {{
            authenticationExtService.basicAuthenticate(anyString, anyString);
            times = 0;
        }};
    }

    @Test(expected = DefaultRabbitmqPluginException.class)
    public void authenticateWithMissingPassword(@Injectable Message message, @Mocked DomibusLogger LOG) {
        String username = "cosmin";
        final MessageProperties messageProperties = MessagePropertiesBuilder.newInstance()
        		.setHeader(RabbitmqMessageConstants.USERNAME, username)
        		.setHeader(RabbitmqMessageConstants.PASSWORD, null)
        		.build();

        new Expectations() {{
            message.getMessageProperties();
            result = messageProperties;
        }};

        backendRabbitmqReceivingListener.authenticate(message);

        new FullVerifications() {{
            authenticationExtService.basicAuthenticate(anyString, anyString);
            times = 0;
        }};
    }
}