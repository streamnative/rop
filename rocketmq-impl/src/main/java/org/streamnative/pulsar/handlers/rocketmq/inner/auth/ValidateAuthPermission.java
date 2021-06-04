package org.streamnative.pulsar.handlers.rocketmq.inner.auth;

import lombok.Data;
import org.apache.logging.log4j.util.Strings;

/**
 * Validate Auth Permission.
 */
@Data
public class ValidateAuthPermission {
    private String stringToken = Strings.EMPTY;
}
