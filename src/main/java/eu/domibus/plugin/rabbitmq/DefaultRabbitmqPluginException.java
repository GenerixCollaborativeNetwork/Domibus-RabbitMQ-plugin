package eu.domibus.plugin.rabbitmq;

/**
 * @author Tiago Miguel, Generix Group
 * @since 3.3
 */
public class DefaultRabbitmqPluginException extends RuntimeException {

    public DefaultRabbitmqPluginException(Exception e) {
        super(e);
    }

    public DefaultRabbitmqPluginException(String message, Exception e) {
        super(message, e);
    }

    public DefaultRabbitmqPluginException(String message) {
        super(message);
    }
}
