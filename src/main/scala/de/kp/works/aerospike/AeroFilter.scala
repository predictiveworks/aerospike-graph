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

import java.util
import scala.collection.JavaConversions._

case class AeroFilters(condition:String, filters:util.List[AeroFilter]) {

  def head:AeroFilter = filters.head

  def nonEmpty:Boolean = filters.nonEmpty

  def size:Int = filters.size

  def tail:Seq[AeroFilter] = filters.tail

}

case class AeroFilter(condition:String, name:String, value:String)
