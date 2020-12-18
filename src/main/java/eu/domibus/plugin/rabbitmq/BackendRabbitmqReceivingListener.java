package eu.domibus.plugin.rabbitmq;

import eu.domibus.ext.services.AuthenticationExtService;
import eu.domibus.ext.services.DomainContextExtService;
import eu.domibus.ext.services.DomibusPropertyExtService;
import eu.domibus.logging.DomibusLogger;
import eu.domibus.logging.DomibusLoggerFactory;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessageProperties;
/**
 * @author Cosmin Baciu, Generix Group
 * 
 */
@Service
public class BackendRabbitmqReceivingListener implements MessageListener{

    private static final DomibusLogger LOG = DomibusLoggerFactory.getLogger(BackendRabbitmqReceivingListener.class);

    @Autowired
    protected BackendRabbitmqImpl backendRabbitmq;

    @Autowired
    protected AuthenticationExtService authenticationExtService;


    /**
     * This method is called when a message was received at the incoming queue
     *
     * @param map The incoming RabbitMQ Message
     */
	@Transactional(propagation = Propagation.REQUIRES_NEW, timeout = 1200)
    public void onMessage(final Message map) {
        if (!authenticationExtService.isUnsecureLoginAllowed()) {
            LOG.debug("Performing authentication");
            LOG.clearCustomKeys();
            authenticate(map);
        }
        backendRabbitmq.receiveMessage(map);
    }

    protected void authenticate(final Message map) {
        String username = null;
        String password = null;
        MessageProperties properties = map.getMessageProperties();
        try {
        	Map<String, Object> header = properties.getHeaders();
            username = header.get(RabbitmqMessageConstants.USERNAME).toString();
            password = header.get(RabbitmqMessageConstants.PASSWORD).toString();
        } catch (Exception e) {
            LOG.error("Exception occurred while retrieving the username or password", e);
            throw new DefaultRabbitmqPluginException("Exception occurred while retrieving the username or password", e);
        }
        if (StringUtils.isBlank(username)) {
            LOG.error("Username is empty");
            throw new DefaultRabbitmqPluginException("Username is empty");
        }
        if (StringUtils.isBlank(password)) {
            LOG.error("Password is empty");
            throw new DefaultRabbitmqPluginException("Password is empty");
        }

        authenticationExtService.basicAuthenticate(username, password);
    }

}
