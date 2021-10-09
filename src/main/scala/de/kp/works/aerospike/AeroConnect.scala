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

import com.aerospike.client.{AerospikeClient, Bin, Host, Key}
import com.aerospike.client.policy.{ClientPolicy, RecordExistsAction, TlsPolicy, WritePolicy}
import de.kp.works.aerospike.util.NamedThreadFactory
import de.kp.works.aerospikegraph.{Constants, ElementType}
import org.apache.commons.configuration2.PropertiesConfiguration

import java.util
import scala.collection.JavaConversions._

object AeroConnect {

  private var instance:Option[AeroConnect] = None
  private var options:Option[AeroOptions] = None

  def getInstance(config: PropertiesConfiguration): AeroConnect = {

    options = Some(new AeroOptions(config))

    if (instance.isEmpty)
      instance = Some(new AeroConnect(options.get))

    instance.get

  }

}

class AeroConnect(options:AeroOptions) {

  private val THREAD_GROUP_NAME:String  = "aero-connect"
  private val THREAD_SCAN_PREFIX:String = "scan"
  /**
   * Build named [ThreadFactory] for scan operations
   */
  private val scanThreadFactory =
    new NamedThreadFactory(THREAD_GROUP_NAME, THREAD_SCAN_PREFIX)

  /**
   * This is the reference to the configured
   * Aerospike client
   */
  private var client:AerospikeClient = _

  private var clientPolicy:ClientPolicy = _
  private var writePolicy:WritePolicy = _

  private val namespace = options.getNamespace
  val setname: String = options.getSetname

  private val timeout = options.getTimeout

  buildAerospikeClient()

  def getClient:AerospikeClient = client

  def getOrCreateCache(name:String) = ???

  /** AEROSPIKE ACCESS METHODS **/

  // TODO TRANSACTION

  /**
   * A helper method to build an Aerospike compliant
   * key from a unique identifier
   */
  def getKey(userKey:String, elementType:ElementType):Key = {

    if (elementType == ElementType.VERTEX)
      new Key(namespace, s"${setname}_${Constants.VERTICES}", userKey)

    else
      new Key(namespace, s"${setname}_${Constants.EDGES}", userKey)

  }
  /**
   * A helper method to retrieve Aerospike records
   * by a set of user keys
   */
  def getByKeys(userKeys:Array[String], elementType:ElementType):util.Iterator[KeyRecord] = {

    val readPolicy = clientPolicy.readPolicyDefault
    val postfix =
      if (elementType == ElementType.VERTEX)
        Constants.VERTICES

      else
        Constants.EDGES

    userKeys.map(userkey => {

      val key = new Key(namespace, s"${setname}_${postfix}", userkey)
      val record = client.get(readPolicy, key)

      KeyRecord(key, record)
    }).iterator

  }
  /**
   * A helper method to write to an Aerospike namespace
   * and setname (_vertex or _edge); note, transaction
   * control is provided by the requester
   */
  def put(key:Key, values:util.List[Bin]):Unit =
    client.put(writePolicy, key, values.toList: _*)

  /**
   * A helper method to remove all Aerospike records
   * that refer to the provided keys
   */
  def removeAll(keys:util.List[Key]):Unit = {
    keys.foreach(key => client.delete(writePolicy, key))
  }

  def scan(scanset:String):util.Iterator[KeyRecord] = {
    val aeroScan = new AeroScan(client, namespace, scanset, timeout, timeout)
    aeroScan.run(scanThreadFactory)
  }

  def close(): Unit = {
    client.close()
  }

  /** AEROSPIKE HELPER METHODS **/

  private def buildAerospikeClient():Unit = {

    /* Define Client Policy */

    clientPolicy = new ClientPolicy()
    clientPolicy.timeout = timeout
    clientPolicy.failIfNotConnected = true

    /* User authentication */

    val (user, pass) = options.getUserAndPass
    clientPolicy.user = user
    clientPolicy.password = pass

    val authMode = options.getAuthMode
    clientPolicy.authMode = authMode

    val tlsMode = options.getTlsMode.toLowerCase
    val tlsName = options.getTlsName

    if (tlsMode == "true" && tlsName == null)
      throw new Exception(s"No Aerospike TLS name specified.")

    if (tlsMode == "true") {
      /*
       * The current implementation leverages the
       * default values
       */
      clientPolicy.tlsPolicy = new TlsPolicy()
    }

    val host = options.getHost
    val port = options.getPort

    val aerospikeHost = new Host(host, options.getTlsName, port)
    client = new AerospikeClient(clientPolicy, aerospikeHost)

    /* Define write policy */

    writePolicy = new WritePolicy(client.writePolicyDefault)
    writePolicy.expiration = options.getExpiration

    val writeMode = options.getWriteMode
    writeMode match {
      case "ErrorIfExists" =>
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY
      case "Ignore" =>
        writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY
      case "Overwrite" =>
        writePolicy.recordExistsAction = RecordExistsAction.REPLACE
      case "Append" =>
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY
      case _ =>
        /* Append */
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY

    }

  }
}
