package io.onetable.utilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class TokenProvider implements SASTokenProvider {
    private String SasKey;

    @Override
        public String getSASToken(String accountName, String fileSystem, String path, String operation) {
        return this.SasKey;
    }

  // Override the initialize method to read the fixed SAS token from the configuration
    @Override
        public void initialize(Configuration configuration, String accountName) {
        this.SasKey = configuration.get("sasToken");
    }
}