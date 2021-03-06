package eu.domibus.plugin.rabbitmq.property;

import eu.domibus.ext.domain.DomainDTO;
import eu.domibus.ext.domain.DomibusPropertyMetadataDTO;
import eu.domibus.ext.services.DomainExtService;
import eu.domibus.ext.services.DomibusPropertyExtService;
import eu.domibus.ext.services.DomibusPropertyManagerExt;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static eu.domibus.plugin.rabbitmq.RabbitmqMessageConstants.*;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Ion Perpegel, Generix Group
 * @since 4.1.1
 * <p>
 * Property manager for the Default RabbitMQ plugin properties.
 */
@Service
public class RabbitmqPluginPropertyManager implements DomibusPropertyManagerExt {

    @Autowired
    protected DomibusPropertyExtService domibusPropertyExtService;

    @Autowired
    protected DomainExtService domainExtService;
    
    protected static final String RABBITMQ_PLUGIN_MODULE = "RABBIT_MQ";

    private String[] knownPropertyNames = new String[]{
            FROM_PARTY_ID, FROM_PARTY_TYPE, FROM_ROLE,
            TO_PARTY_ID, TO_PARTY_TYPE, TO_ROLE,
            AGREEMENT_REF, SERVICE, SERVICE_TYPE, ACTION,
            PUT_ATTACHMENTS_IN_QUEUE,
    };

    private Map<String, DomibusPropertyMetadataDTO> knownProperties = Arrays.stream(knownPropertyNames)
            .map(name -> new DomibusPropertyMetadataDTO(RABBITMQ_PLUGIN_PROPERTY_PREFIX + "." + name, RABBITMQ_PLUGIN_MODULE, true, true))
            .collect(Collectors.toMap(x -> x.getName(), x -> x));

    @Override
    public String getKnownPropertyValue(String domainCode, String propertyName) {
        if (!hasKnownProperty(propertyName)) {
            throw new IllegalArgumentException("Unknown property: " + propertyName);
        }

        final DomainDTO domain = domainExtService.getDomain(domainCode);

        if (StringUtils.equalsIgnoreCase(propertyName, PUT_ATTACHMENTS_IN_QUEUE)) {
            return domibusPropertyExtService.getDomainProperty(domain, propertyName, "true");
        } else {
            return domibusPropertyExtService.getDomainProperty(domain, propertyName);
        }
    }

    @Override
    public void setKnownPropertyValue(String domainCode, String propertyName, String propertyValue, boolean broadcast) {
        if (!hasKnownProperty(propertyName)) {
            throw new IllegalArgumentException("Unknown property: " + propertyName);
        }

        final DomainDTO domain = domainExtService.getDomain(domainCode);
        domibusPropertyExtService.setDomainProperty(domain, propertyName, propertyValue);
    }

    @Override
    public void setKnownPropertyValue(String domainCode, String propertyName, String propertyValue) {
        setKnownPropertyValue(domainCode, propertyName, propertyValue, true);
    }

    @Override
    public Map<String, DomibusPropertyMetadataDTO> getKnownProperties() {
        return knownProperties;
    }

    @Override
    public boolean hasKnownProperty(String name) {
        return getKnownProperties().containsKey(name);
    }
}
