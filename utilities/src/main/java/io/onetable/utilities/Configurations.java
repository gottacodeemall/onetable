/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.utilities;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import lombok.Data;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.annotations.VisibleForTesting;

import io.onetable.iceberg.IcebergCatalogConfig;
import io.onetable.model.storage.TableFormat;

public class Configurations {
  public static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  @Data
  public static class DatasetConfig {

    /**
     * Table format of the source table. This is a {@link TableFormat} value. Although the format of
     * the source can be auto-detected, it is recommended to specify it explicitly for cases where
     * the directory contains metadata of multiple formats.
     */
    String sourceFormat;

    /** The target formats to sync to. This is a list of {@link TableFormat} values. */
    List<String> targetFormats;

    /** Configuration of the dataset to sync, path, table name, etc. */
    List<Table> datasets;

    @Data
    public static class Table {
      /**
       * The base path of the table to sync. Any authentication configuration needed by HDFS client
       * can be provided using hadoop config file
       */
      String tableBasePath;

      String tableDataPath;

      String tableName;
      String partitionSpec;
      String namespace;
    }
  }

  @Data
  public static class TableFormatClients {
    /** Map of table format name to the client configs. */
    @JsonProperty("tableFormatsClients")
    @JsonMerge
    Map<String, ClientConfig> tableFormatsClients;

    @Data
    public static class ClientConfig {
      /** The class name of the source client which reads the table metadata. */
      String sourceClientProviderClass;

      /** The class name of the target client which writes the table metadata. */
      String targetClientProviderClass;

      /** the configuration specific to the table format. */
      @JsonMerge Map<String, String> configuration;
    }
  }

  static byte[] getCustomConfigurations(CommandLine cmd, String option) throws IOException {
    byte[] customConfig = null;
    if (cmd.hasOption(option)) {
      customConfig = Files.readAllBytes(Paths.get(cmd.getOptionValue(option)));
    }
    return customConfig;
  }

  @VisibleForTesting
  static Configuration loadHadoopConf(byte[] customConfig) {
    Configuration conf = new Configuration();
    conf.addResource("onetable-hadoop-defaults.xml");
    if (customConfig != null) {
      conf.addResource(new ByteArrayInputStream(customConfig), "customConfigStream");
    }
    return conf;
  }

  /**
   * Loads the client configs. The method first loads the default configs and then merges any custom
   * configs provided by the user.
   *
   * @param customConfigs the custom configs provided by the user
   * @return available tableFormatsClients and their configs
   */
  @VisibleForTesting
  static TableFormatClients loadTableFormatClientConfigs(byte[] customConfigs) throws IOException {
    // get resource stream from default client config yaml file
    try (InputStream inputStream =
        RunSync.class.getClassLoader().getResourceAsStream("onetable-client-defaults.yaml")) {
      TableFormatClients clients = YAML_MAPPER.readValue(inputStream, TableFormatClients.class);
      if (customConfigs != null) {
        YAML_MAPPER.readerForUpdating(clients).readValue(customConfigs);
      }
      return clients;
    }
  }

  @VisibleForTesting
  static IcebergCatalogConfig loadIcebergCatalogConfig(byte[] customConfigs) throws IOException {
    return customConfigs == null
        ? null
        : YAML_MAPPER.readValue(customConfigs, IcebergCatalogConfig.class);
  }
}
