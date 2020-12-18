package eu.domibus.plugin.rabbitmq.property;

import eu.domibus.ext.domain.DomainDTO;
import eu.domibus.ext.domain.DomibusPropertyMetadataDTO;
import eu.domibus.ext.services.DomainExtService;
import eu.domibus.ext.services.DomibusPropertyExtService;
import eu.domibus.plugin.rabbitmq.property.RabbitmqPluginPropertyManager;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import mockit.integration.junit4.JMockit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;

/**
 * @author Ion Perpegel, Generix Group
 * @since 4.1.1
 */
@RunWith(JMockit.class)
public class RabbitmqPluginPropertyManagerTest {

    @Tested
    RabbitmqPluginPropertyManager rabbitmqPluginPropertyManager;

    @Injectable
    protected DomibusPropertyExtService domibusPropertyExtService;

    @Injectable
    protected DomainExtService domainExtService;


    private final String rabbitmqProperty = "plugin.rabbitmq.fromPartyId";
    private final String testValue = "new-value";
    private final DomainDTO testDomain = new DomainDTO("default", "default");

    @Test
    public void setKnownPropertyValue() {
        new Expectations() {{
            domainExtService.getDomain("default");
            result = testDomain;

            domibusPropertyExtService.getDomainProperty(testDomain, rabbitmqProperty);
            returns("old-value", testValue);
        }};

        final String oldValue = rabbitmqPluginPropertyManager.getKnownPropertyValue("default", rabbitmqProperty);
        rabbitmqPluginPropertyManager.setKnownPropertyValue("default", rabbitmqProperty, testValue);
        final String newValue = rabbitmqPluginPropertyManager.getKnownPropertyValue("default", rabbitmqProperty);

        Assert.assertTrue(oldValue != newValue);
        Assert.assertEquals(testValue, newValue);
    }

    @Test
    public void getKnownProperties() {
        Map<String, DomibusPropertyMetadataDTO> properties = rabbitmqPluginPropertyManager.getKnownProperties();
        Assert.assertTrue(properties.containsKey(rabbitmqProperty));
    }

    @Test
    public void hasKnownProperty() {
        boolean hasProperty = rabbitmqPluginPropertyManager.hasKnownProperty(rabbitmqProperty);
        Assert.assertTrue(hasProperty);
    }

    @Test
    public void testUnknownProperty() {
        String unknownPropertyName = "plugin.rabbitmq.unknown.property";

        try {
            rabbitmqPluginPropertyManager.getKnownPropertyValue("default", unknownPropertyName);
            Assert.fail("Expected exception not thrown");
        } catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().contains(unknownPropertyName));
        }

        try {
            rabbitmqPluginPropertyManager.setKnownPropertyValue("default", unknownPropertyName, testValue);
            Assert.fail("Expected exception not thrown");
        } catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().contains(unknownPropertyName));
        }
    }

}