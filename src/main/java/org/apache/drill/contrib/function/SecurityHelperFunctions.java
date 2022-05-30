/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.contrib.function;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import org.apache.drill.common.exceptions.UserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SecurityHelperFunctions {
  private static final Logger logger = LoggerFactory.getLogger(SecurityHelperFunctions.class);

  public static DatabaseReader getCountryDatabaseReader() throws UserException {
    InputStream db = SecurityHelperFunctions.class.getClassLoader()
      .getResourceAsStream("GeoLite2-Country.mmdb");

    try {
      return new DatabaseReader.Builder(db)
        .withCache(new CHMCache())
        .build();
    } catch (IOException e) {
      throw UserException.validationError(e)
        .message("Could not locate MaxMind Country Database.  Please ensure that it is in your classpath.")
        .build(logger);
    }
  }

  public static DatabaseReader getCityDatabaseReader() throws UserException {
    java.io.InputStream db = SecurityHelperFunctions.class.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
    try {
      return new DatabaseReader.Builder(db)
        .withCache(new CHMCache()).build();
    } catch (IOException e) {
      throw UserException.validationError(e)
        .message("Could not locate MaxMind City Database.  Please ensure that it is in your classpath.")
        .build(logger);
    }
  }

  public static DatabaseReader getASNDatabaseReader() throws UserException {
    java.io.InputStream db = SecurityHelperFunctions.class.getClassLoader().getResourceAsStream("GeoLite2-ASN.mmdb");
    try {
      return new DatabaseReader.Builder(db).withCache(new CHMCache()).build();
    } catch (IOException e) {
      throw UserException.validationError(e)
        .message("Could not locate MaxMind City Database.  Please ensure that it is in your classpath.")
        .build(logger);
    }
  }

  public static HashMap getPortHashMap() throws UserException {
    InputStream serviceFile = SecurityHelperFunctions.class.getClassLoader().getResourceAsStream("service-names-port-numbers.csv");

    if (serviceFile == null) {
      throw UserException.dataReadError()
        .message("Could not read port service names file. ")
        .build(logger);
    }

    HashMap serviceInfo = new HashMap<String, String>();

    String line = "";
    String key = "";
    String linePattern = "^[a-zA-Z0-9_-]*,\\d+,";
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(serviceFile));
      while ((line = br.readLine()) != null) {

        // Create a Pattern object
        Pattern r = Pattern.compile(linePattern);

        // Now create matcher object.
        Matcher m = r.matcher(line);
        int pos;
        String description;
        if (m.find()) {
          String[] values = line.split(",");
          pos = Integer.parseInt(values[1]);
          if (values.length == 3) {
            description = "";
          } else {
            description = values[3];
          }
          key = values[1] + ":" + values[2];
          serviceInfo.put(key, description);
        }
      }
      return serviceInfo;

    } catch (Exception e) {
      throw UserException.validationError(e)
        .message("Could not read ISO port lookup table.")
        .build(logger);
    }
  }
}
