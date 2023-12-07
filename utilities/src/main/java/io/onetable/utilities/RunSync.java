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

import static io.onetable.utilities.Configurations.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.onetable.iceberg.IcebergCatalogConfig;

/**
 * Provides a standalone runner for the sync process. See README.md for more details on how to run
 * this.
 */
@Log4j2
public class RunSync {

  public static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final String DATASET_CONFIG_OPTION = "d";
  private static final String HADOOP_CONFIG_PATH = "p";
  private static final String CLIENTS_CONFIG_PATH = "c";
  private static final String ICEBERG_CATALOG_CONFIG_PATH = "i";
  private static final String HELP_OPTION = "h";

  private static final Options OPTIONS =
      new Options()
          .addRequiredOption(
              DATASET_CONFIG_OPTION,
              "datasetConfig",
              true,
              "The path to a yaml file containing dataset configuration")
          .addOption(
              HADOOP_CONFIG_PATH,
              "hadoopConfig",
              true,
              "Hadoop config xml file path containing configs necessary to access the "
                  + "file system. These configs will override the default configs.")
          .addOption(
              CLIENTS_CONFIG_PATH,
              "clientsConfig",
              true,
              "The path to a yaml file containing OneTable client configurations. "
                  + "These configs will override the default")
          .addOption(
              ICEBERG_CATALOG_CONFIG_PATH,
              "icebergCatalogConfig",
              true,
              "The path to a yaml file containing Iceberg catalog configuration. The configuration will be "
                  + "used for any Iceberg source or target.")
          .addOption(HELP_OPTION, "help", false, "Displays help information to run this utility");

  public static void main(String[] args) throws IOException {
    CommandLineParser parser = new DefaultParser();

    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      new HelpFormatter().printHelp("onetable.jar", OPTIONS, true);
      return;
    }

    if (cmd.hasOption(HELP_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("RunSync", OPTIONS);
      return;
    }

    DatasetConfig datasetConfig = new DatasetConfig();
    try (InputStream inputStream =
        Files.newInputStream(Paths.get(cmd.getOptionValue(DATASET_CONFIG_OPTION)))) {
      ObjectReader objectReader = YAML_MAPPER.readerForUpdating(datasetConfig);
      objectReader.readValue(inputStream);
    }

    Configuration hadoopConf = loadHadoopConf(getCustomConfigurations(cmd, HADOOP_CONFIG_PATH));
    IcebergCatalogConfig icebergCatalogConfig =
        loadIcebergCatalogConfig(getCustomConfigurations(cmd, ICEBERG_CATALOG_CONFIG_PATH));
    TableFormatClients tableFormatClients =
        loadTableFormatClientConfigs(getCustomConfigurations(cmd, CLIENTS_CONFIG_PATH));
    Orchestrator orchestrator =
        new Orchestrator(datasetConfig, hadoopConf, icebergCatalogConfig, tableFormatClients);
    orchestrator.Sync();
  }
}
