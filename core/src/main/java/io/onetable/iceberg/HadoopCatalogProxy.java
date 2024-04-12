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
 
package io.onetable.iceberg;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTableOperationsProxy;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;

public class HadoopCatalogProxy extends HadoopCatalog {

  private FileIO fileIO;
  private CloseableGroup closeableGroup;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);

    Configuration conf = getConf();
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO =
        fileIOImpl == null
            ? new HadoopFileIO(conf)
            : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

    this.closeableGroup = new CloseableGroup();
    closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    return new HadoopTableOperationsProxy(
        new Path(defaultWarehouseLocation(identifier)), fileIO, getConf());
  }

  @Override
  public void close() throws IOException {
    closeableGroup.close();
    super.close();
  }
}
