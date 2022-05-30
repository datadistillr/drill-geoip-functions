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

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;

import javax.inject.Inject;

@SuppressWarnings("unused")
public class GeoIPFunctions {

  private GeoIPFunctions() {
  }

  @FunctionTemplate(names = {"getCountryName", "get_country_name"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCountryNameFunction implements DrillSimpleFunc {
    @Param
    VarCharHolder inputTextA;
    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buffer;
    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCountryDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String countryName;

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));

        countryName = country.getCountry().getName();
        if (countryName == null) {
          countryName = "Unknown";
        }
      } catch (Exception e) {
        countryName = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = countryName.getBytes().length;
      buffer.setBytes(0, countryName.getBytes());
    }
  }


  @FunctionTemplate(name = "getCountryISOCode",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCountryISOFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCountryDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String countryName;

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
        countryName = country.getCountry().getIsoCode();
        if (countryName == null) {
          countryName = "UNK";
        }

      } catch (Exception e) {
        countryName = "UNK";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = countryName.getBytes().length;
      buffer.setBytes(0, countryName.getBytes());
    }
  }


  @FunctionTemplate(names = {"getCountryConfidence", "get_country_confidence"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCountryConfidenceFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCountryDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int confidence;

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
        confidence = country.getCountry().getConfidence();

      } catch (Exception e) {
        confidence = 0;
      }
      out.value = confidence;
    }
  }


  @FunctionTemplate(names = {"getCityName", "get_city_name"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class getCityNameFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String cityName;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        cityName = city.getCity().getName();
        if (cityName == null) {
          cityName = "Unknown";
        }
      } catch (Exception e) {
        cityName = "Unknown";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = cityName.getBytes().length;
      buffer.setBytes(0, cityName.getBytes());
    }
  }

  @FunctionTemplate(names = {"getCityConfidence", "get_city_confidence"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class getCityConfidenceFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int cityConfidence;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        cityConfidence = city.getCity().getConfidence();
      } catch (Exception e) {
        cityConfidence = 0;
      }
      out.value = cityConfidence;
    }
  }

  @FunctionTemplate(names = {"getLatitudeFromIP", "get_latitude_from_ip"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getLatitudeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    Float8Holder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double latitude;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        latitude = location.getLatitude();

      } catch (Exception e) {
        latitude = 0.0;
      }

      out.value = latitude;

    }
  }

  @FunctionTemplate(names = {"getLongitudeFromIP", "get_longitude_from_ip"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getLongitudeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    Float8Holder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double longitude;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        longitude = location.getLongitude();

      } catch (Exception e) {
        longitude = 0.0;
      }

      out.value = longitude;

    }
  }

  @FunctionTemplate(names = {"getTimezoneFromIP", "get_timezone_from_ip"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getTimezoneFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String timezone;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        timezone = location.getTimeZone();

      } catch (Exception e) {
        timezone = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = timezone.getBytes().length;
      buffer.setBytes(0, timezone.getBytes());

    }
  }

  @FunctionTemplate(names = {"getAccuracyRadius", "get_accuracy_radius"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getAccuracyRadiusFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int accuracyRadius;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        accuracyRadius = location.getAccuracyRadius();

      } catch (Exception e) {
        accuracyRadius = 0;
      }
      out.value = accuracyRadius;
    }
  }

  @FunctionTemplate(names = {"getAverageIncome","get_average_income"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getAverageIncomeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int averageIncome;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        averageIncome = location.getAverageIncome();

      } catch (Exception e) {
        averageIncome = 0;
      }
      out.value = averageIncome;
    }
  }

  @FunctionTemplate(names = {"getMetroCode", "get_metro_code"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getMetroCodeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    @Workspace
    java.io.File database;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int metroCode;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        metroCode = location.getMetroCode();

      } catch (Exception e) {
        metroCode = 0;
      }
      out.value = metroCode;
    }
  }

  @FunctionTemplate(names = {"getPopulationDensity", "get_population_density"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getPopulationDensityFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    IntHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      int populationDensity;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        populationDensity = location.getPopulationDensity();

      } catch (Exception e) {
        populationDensity = 0;
      }
      out.value = populationDensity;
    }
  }

  @FunctionTemplate(names = {"isEU", "isEuropeanUnion", "is_eu", "is_european_union"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isEUFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCountryDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isEU;

      try {
        com.maxmind.geoip2.model.CountryResponse country = reader.country(java.net.InetAddress.getByName(ip));
        isEU = country.getCountry().isInEuropeanUnion();
      } catch (Exception e) {
        isEU = false;
      }
      if (isEU) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(names = {"getPostalCode", "get_postal_code"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getPostalCodeFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;


    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String postalCode;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Postal postal = city.getPostal();
        postalCode = postal.getCode();
        if (postalCode == null) {
          postalCode = "Unknown";
        }
      } catch (Exception e) {
        postalCode = "Unknown";
      }

      out.buffer = buffer;
      out.start = 0;
      out.end = postalCode.getBytes().length;
      buffer.setBytes(0, postalCode.getBytes());

    }
  }

  @FunctionTemplate(names = {"getCoordPoint", "get_coord_point"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getCoordPointFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarBinaryHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;


    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }


    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      double longitude;
      double latitude;

      try {
        com.maxmind.geoip2.model.CityResponse city = reader.city(java.net.InetAddress.getByName(ip));
        com.maxmind.geoip2.record.Location location = city.getLocation();
        longitude = location.getLongitude();
        latitude = location.getLatitude();

      } catch (Exception e) {
        latitude = 0.0;
        longitude = 0.0;
      }
      com.esri.core.geometry.ogc.OGCPoint point = new com.esri.core.geometry.ogc.OGCPoint(new com.esri.core.geometry.Point(longitude, latitude), com.esri.core.geometry.SpatialReference.create(4326));

      java.nio.ByteBuffer pointBytes = point.asBinary();
      out.buffer = buffer;
      out.start = 0;
      out.end = pointBytes.remaining();
      buffer.setBytes(0, pointBytes);

    }
  }

  @FunctionTemplate(names = {"getASN", "get_asn"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getASNFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BigIntHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getASNDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      Long ASN;

      try {
        ASN = reader.asn(java.net.InetAddress.getByName(ip)).getAutonomousSystemNumber();
      } catch (Exception e) {
        ASN = 0L;
      }
      out.value = ASN;
    }
  }

  @FunctionTemplate(names = {"getASNOrganization", "get_asn_organization"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class getASNOrgFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getASNDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      String ASNorg;

      try {
        ASNorg = reader.asn(java.net.InetAddress.getByName(ip)).getAutonomousSystemOrganization();
      } catch (Exception e) {
        ASNorg = "Unknown";
      }
      out.buffer = buffer;
      out.start = 0;
      out.end = ASNorg.getBytes().length;
      buffer.setBytes(0, ASNorg.getBytes());
    }
  }

  @FunctionTemplate(names = {"isAnonymous", "is_anonymous"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isAnonymousFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isAnonymous;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isAnonymous = response.isAnonymous();
      } catch (Exception e) {
        isAnonymous = false;
      }
      if (isAnonymous) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(names = {"isAnonymousVPN", "is_anonymous_vpn"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isAnonymousVPNFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isAnonymousVPN;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isAnonymousVPN = response.isAnonymousVpn();
      } catch (Exception e) {
        isAnonymousVPN = false;
      }
      if (isAnonymousVPN) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(names = {"isHostingProvider", "is_hosting_provider"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isHostingProviderFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isHostingProvider;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isHostingProvider = response.isHostingProvider();
      } catch (Exception e) {
        isHostingProvider = false;
      }
      if (isHostingProvider) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(names = {"isPublicProxy", "is_public_proxy"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isPublicProxyFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isPublicProxy;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isPublicProxy = response.isPublicProxy();
      } catch (Exception e) {
        isPublicProxy = false;
      }
      if (isPublicProxy) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(names = {"isTORExitNode", "is_tor_exit_node"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class isTORFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder inputTextA;

    @Output
    BitHolder out;

    @Workspace
    com.maxmind.geoip2.DatabaseReader reader;

    public void setup() {
      reader = org.apache.drill.contrib.function.SecurityHelperFunctions.getCityDatabaseReader();
    }

    public void eval() {
      String ip = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(inputTextA.start, inputTextA.end, inputTextA.buffer);
      boolean isTOR;

      try {
        com.maxmind.geoip2.model.AnonymousIpResponse response = reader.anonymousIp(java.net.InetAddress.getByName(ip));
        isTOR = response.isTorExitNode();
      } catch (Exception e) {
        isTOR = false;
      }
      if (isTOR) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }
}
