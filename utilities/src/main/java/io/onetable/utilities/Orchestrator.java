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

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import io.onetable.client.OneTableClient;
import io.onetable.client.PerTableConfig;
import io.onetable.client.SourceClientProvider;
import io.onetable.hudi.ConfigurationBasedPartitionSpecExtractor;
import io.onetable.hudi.HudiSourceConfig;
import io.onetable.iceberg.IcebergCatalogConfig;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.reflection.ReflectionUtils;
import io.onetable.utilities.Configurations.DatasetConfig;
import io.onetable.utilities.Configurations.TableFormatClients;

@Log4j2
public class Orchestrator {
  private DatasetConfig datasetConfig;
  private Configuration hadoopConf;
  private IcebergCatalogConfig icebergCatalogConfig;
  private TableFormatClients tableFormatClients;
  private OneTableClient client;

  public Orchestrator(
      DatasetConfig datasetConfig,
      Configuration hadoopConf,
      IcebergCatalogConfig icebergCatalogConfig,
      TableFormatClients tableFormatClients) {
    this.datasetConfig = datasetConfig;
    this.hadoopConf = hadoopConf;
    this.icebergCatalogConfig = icebergCatalogConfig;
    this.tableFormatClients = tableFormatClients;
    this.client = new OneTableClient(this.hadoopConf);
  }

  public String Sync() {
    String sourceFormat = datasetConfig.sourceFormat;
    TableFormatClients.ClientConfig sourceClientConfig =
        tableFormatClients.getTableFormatsClients().get(sourceFormat);
    if (sourceClientConfig == null) {
      throw new IllegalArgumentException(
          String.format(
              "Source format %s is not supported. Known source and target formats are %s",
              sourceFormat, tableFormatClients.getTableFormatsClients().keySet()));
    }
    String sourceProviderClass = sourceClientConfig.sourceClientProviderClass;
    SourceClientProvider<?> sourceClientProvider =
        ReflectionUtils.createInstanceOfClass(sourceProviderClass);
    sourceClientProvider.init(hadoopConf, sourceClientConfig.configuration);

    List<TableFormat> tableFormatList =
        datasetConfig.getTargetFormats().stream()
            .map(TableFormat::valueOf)
            .collect(Collectors.toList());
    
    StringBuilder errors = new StringBuilder();
    for (DatasetConfig.Table table : datasetConfig.getDatasets()) {
      log.info(
          "Running sync for basePath {} for following table formats {}",
          table.getTableBasePath(),
          tableFormatList);

      PerTableConfig config =
          PerTableConfig.builder()
              .tableBasePath(table.getTableBasePath())
              .tableName(table.getTableName())
              .namespace(table.getNamespace() == null ? null : table.getNamespace().split("\\."))
              .tableDataPath(table.getTableDataPath())
              .icebergCatalogConfig(icebergCatalogConfig)
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionSpecExtractorClass(
                          ConfigurationBasedPartitionSpecExtractor.class.getName())
                      .partitionFieldSpecConfig(table.getPartitionSpec())
                      .build())
              .targetTableFormats(tableFormatList)
              .syncMode(SyncMode.INCREMENTAL)
              .build();
      try {
        client.sync(config, sourceClientProvider);
      } catch (Exception e) {
        log.error(String.format("Error running sync for %s", table.getTableBasePath()), e);
        errors.append(String.format("Error running sync for %s : %s \n", table.getTableBasePath(), e.getMessage()));
      }
    }

    return errors.toString();
  }
}
