package de.kp.works.aerospike
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.aerospike.client.policy.AuthMode
import de.kp.works.aerospikegraph.AeroConfiguration.Keys
import org.apache.commons.configuration2.PropertiesConfiguration

class AeroOptions(config:PropertiesConfiguration) {

  def getAuthMode:AuthMode = {
    val value = if (config.containsKey(Keys.AEROSPIKE_AUTH_MODE)) {
      config.getString(Keys.AEROSPIKE_AUTH_MODE)
    }
    else "INTERNAL"

    AuthMode.valueOf(value.toUpperCase)

  }

  def getExpiration:Int = {
    if (config.containsKey(Keys.AEROSPIKE_EXPIRATION))
      config.getInt(Keys.AEROSPIKE_EXPIRATION)

    else 0
  }

  def getHost:String =
    if (config.containsKey(Keys.AEROSPIKE_HOST))
      config.getString(Keys.AEROSPIKE_HOST)

    else
     throw new Exception("No Aerospike database host specified.")

  def getNamespace:String =
    if (config.containsKey(Keys.AEROSPIKE_NAMESPACE))
      config.getString(Keys.AEROSPIKE_NAMESPACE)

    else
       throw new Exception("No Aerospike namespace specified.")

  def getPort:Int =
    if (config.containsKey(Keys.AEROSPIKE_PORT))
      config.getInt(Keys.AEROSPIKE_PORT)

    else
      throw new Exception("No Aerospike database port specified.")

  def getSetname:String =
    if (config.containsKey(Keys.AEROSPIKE_SET))
      config.getString(Keys.AEROSPIKE_SET)

    else
      throw new Exception("No Aerospike set name specified.")

  /**
   * The timeout of an Aerospike database connection
   * in milliseconds. Default is 1000.
   */
  def getTimeout:Int =
    if (config.containsKey(Keys.AEROSPIKE_TIMEOUT))
      config.getInt(Keys.AEROSPIKE_TIMEOUT)

    else 1000

  def getTlsMode:String =
    if (config.containsKey(Keys.AEROSPIKE_TLS_MODE))
      config.getString(Keys.AEROSPIKE_TLS_MODE)

    else "false"

  def getTlsName:String =
    if (config.containsKey(Keys.AEROSPIKE_TLS_NAME))
      config.getString(Keys.AEROSPIKE_TLS_NAME)

    else null

  /* User authentication */

  def getUserAndPass:(String, String) = {

    val username =
      if (config.containsKey(Keys.AEROSPIKE_USER))
        config.getString(Keys.AEROSPIKE_USER)

      else
        throw new Exception("No Aerospike user name specified.")

    val password =
      if (config.containsKey(Keys.AEROSPIKE_PASSWORD))
        config.getString(Keys.AEROSPIKE_PASSWORD)

      else
        throw new Exception("No Aerospike user password specified.")

    (username, password)

  }

  def getWriteMode:String =
    if (config.containsKey(Keys.AEROSPIKE_WRITE))
      config.getString(Keys.AEROSPIKE_WRITE)

    else "Append"

}
