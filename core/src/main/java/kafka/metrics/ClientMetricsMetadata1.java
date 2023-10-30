package kafka.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import kafka.network.RequestChannel;

import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.Map;

public class ClientMetricsMetadata1 {

  private static final String CLIENT_INSTANCE_ID = "CLIENT_INSTANCE_ID";
  private static final String CLIENT_ID = "CLIENT_ID";
  private static final String CLIENT_SOFTWARE_NAME = "CLIENT_SOFTWARE_NAME";
  private static final String CLIENT_SOFTWARE_VERSION = "CLIENT_SOFTWARE_VERSION";
  private static final String CLIENT_SOURCE_ADDRESS = "CLIENT_SOURCE_ADDRESS";
  private static final String CLIENT_SOURCE_PORT = "CLIENT_SOURCE_PORT";

  private Map<String, String> attributesMap;

  public ClientMetricsMetadata1() {
    attributesMap = new HashMap<>();
  }

  private void init(String clientInstanceId,
      String clientId,
      String clientSoftwareName,
      String clientSoftwareVersion,
      String clientSourceAddress,
      String clientSourcePort) {
    attributesMap.put(CLIENT_INSTANCE_ID, clientInstanceId);
    attributesMap.put(CLIENT_ID, clientId);
    attributesMap.put(CLIENT_SOFTWARE_NAME, clientSoftwareName);
    attributesMap.put(CLIENT_SOFTWARE_VERSION, clientSoftwareVersion);
    attributesMap.put(CLIENT_SOURCE_ADDRESS, clientSourceAddress);
    attributesMap.put(CLIENT_SOURCE_PORT, clientSourcePort);
  }

  public String getClientId() {
    return attributesMap.get(CLIENT_ID);
  }

  boolean isMatched(Optional<Map<String, String>> patterns) {
    // Empty pattern or missing pattern still considered as a match
    if (patterns.isPresent() && ! patterns.get().isEmpty()) {
      return matchPatterns(patterns.get());
    } else {
      return true;
    }
  }

  private boolean matchPatterns(Map<String, String> matchingPatterns) {
    return matchingPatterns.entrySet().stream()
        .allMatch(entry -> {
          String attribute = attributesMap.get(entry.getKey());
          return attribute != null && entry.getValue().r.anchored().findAll().nonEmpty();
        });
  }

  public Map<String, String> parseMatchingPatterns(List<String> patterns) {
    if (patterns == null) {
      throw new InvalidConfigurationException("Patterns cannot be null");
    }
    Map<String, String> patternsMap = new HashMap<>();
    for (String pattern : patterns) {
      String[] nameValuePair = pattern.split("=", 2);
      if (nameValuePair.length == 2 && isValidParam(nameValuePair[0]) && isValidRegex(nameValuePair[1])) {
        patternsMap.put(nameValuePair[0], nameValuePair[1]);
      } else {
        throw new InvalidConfigurationException("Illegal client matching pattern: " + pattern);
      }
    }
    return patternsMap;
  }

  private boolean isValidRegex(String inputPattern) {
    try {
      java.util.regex.Pattern.compile(inputPattern);
      return true;
    } catch (java.util.regex.PatternSyntaxException e) {
      return false;
    }
  }

  private boolean isValidParam(String param) {
    // Add your validation logic here
    return true;
  }
}
