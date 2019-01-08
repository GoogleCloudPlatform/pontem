/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pontem.config;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;

/** Class that reads a YAML config file and imports said config into the corresponding objects. */
public final class Configuration {

  /** The instance of the Configuration that this class is storing. */
  private static Configuration instance = null;

  private Integer concurrencyLevel = 0;
  private String outputFileFolder = "";
  private Boolean isRatioBasedBenchmark = false;
  private List<WorkloadSettings> workloads = new ArrayList<>();

  /**
   * Get the Instance of this class There should only ever be one instance of this class and other
   * classes can use this static method to retrieve the instance
   *
   * @return Configuration the stored Instance of this class
   */
  public static Configuration getInstance() {
    if (Configuration.instance == null) {
      throw new IllegalStateException(
          "The configuration must be loaded with .load() prior to access!");
    }

    return Configuration.instance;
  }

  /**
   * Load the Configuration specified at fileName
   *
   * @return boolean did this load succeed?
   */
  public static boolean loadConfig(String filename) {
    try {
      String configYaml =
          Resources.toString(Resources.getResource(filename), Charset.defaultCharset());

      Yaml yaml = new Yaml();
      yaml.setBeanAccess(BeanAccess.FIELD);
      instance = yaml.loadAs(configYaml, Configuration.class);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<WorkloadSettings> filteredWorkloads = new ArrayList<>();
    for (WorkloadSettings workload : instance.workloads) {
      if (workload != null) {
        // Eagerly read queries to ensure the loading does not affect the benchmark reads.
        workload.getQueries();
        filteredWorkloads.add(workload);
      } else {
        logger.warning("This configuration has empty workloads");
      }
    }

    instance.workloads = filteredWorkloads;

    return true;
  }

  public Integer getConcurrencyLevel() {
    return this.concurrencyLevel;
  }

  public void setConcurrencyLevel(Integer concurrencyLevel) {
    this.concurrencyLevel = concurrencyLevel;
  }

  public String getOutputFileFolder() {
    return this.outputFileFolder;
  }

  public void setOutputFileFolder(String outputFileFolder) {
    this.outputFileFolder = outputFileFolder;
  }

  public Boolean isRatioBasedBenchmark() {
    return this.isRatioBasedBenchmark;
  }

  public void setIsRatioBasedBenchmark(Boolean isRatioBasedBenchmark) {
    this.isRatioBasedBenchmark = isRatioBasedBenchmark;
  }

  public List<WorkloadSettings> getWorkloads() {
    return this.workloads;
  }

  public void setWorkloads(List<WorkloadSettings> workloads) {
    this.workloads = workloads;
  }
}
