/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import kafka.admin.AdminUtils.assignReplicasToBrokers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

final class AdminUtilsTest {
  @Test
  def testAssignReplicasToBrokersRackUnaware(): Unit = {
    // Define the broker metadata
    val brokerMetadatas = List(
      BrokerMetadata(0, None),
      BrokerMetadata(1, None),
      BrokerMetadata(2, None)
    )

    // Assign replicas to brokers without considering racks
    val assignment = assignReplicasToBrokers(brokerMetadatas, 6, 3)

    // Expected assignment
    val expectedAssignment = Map(
      0 -> List(0, 1, 2),
      1 -> List(1, 2, 0),
      2 -> List(2, 0, 1),
      3 -> List(0, 2, 1),
      4 -> List(1, 0, 2),
      5 -> List(2, 1, 0)
    )

    assertEquals(expectedAssignment, assignment)
  }

  @Test
  def testAssignReplicasToBrokersRackAware(): Unit = {
    // Define the broker metadata with rack information
    val brokerMetadatas = List(
      BrokerMetadata(0, Some("rack1")),
      BrokerMetadata(1, Some("rack3")),
      BrokerMetadata(2, Some("rack3")),
      BrokerMetadata(3, Some("rack2")),
      BrokerMetadata(4, Some("rack2")),
      BrokerMetadata(5, Some("rack1"))
    )

    // Assign replicas to brokers considering racks
    val assignment = assignReplicasToBrokers(brokerMetadatas, 6, 3)

    // Expected assignment
    val expectedAssignment = Map(
      0 -> List(0, 3, 1),
      1 -> List(3, 1, 5),
      2 -> List(1, 5, 4),
      3 -> List(5, 4, 2),
      4 -> List(4, 2, 0),
      5 -> List(2, 0, 3)
    )

    assertEquals(expectedAssignment, assignment)
  }

  @Test()
  def testAssignReplicasToBrokersInvalidPartitions(): Unit = {
    // Define the broker metadata
    val brokerMetadatas = List(
      BrokerMetadata(0, None),
      BrokerMetadata(1, None)
    )

    // Assign replicas with an invalid number of partitions
    assignReplicasToBrokers(brokerMetadatas, 0, 2)
  }

  @Test()
  def testAssignReplicasToBrokersInvalidReplicationFactor(): Unit = {
    // Define the broker metadata
    val brokerMetadatas = List(
      BrokerMetadata(0, None),
      BrokerMetadata(1, None)
    )

    // Assign replicas with an invalid replication factor
    assignReplicasToBrokers(brokerMetadatas, 3, 0)
  }

  @Test()
  def testAssignReplicasToBrokersInsufficientBrokers(): Unit = {
    // Define the broker metadata
    val brokerMetadatas = List(
      BrokerMetadata(0, None)
    )

    // Assign replicas with an insufficient number of brokers
    assignReplicasToBrokers(brokerMetadatas, 3, 2)
  }

  @Test()
  def testAssignReplicasToBrokersIncompleteRackInformation(): Unit = {
    // Define the broker metadata with incomplete rack information
    val brokerMetadatas = List(
      BrokerMetadata(0, Some("rack1")),
      BrokerMetadata(1, None),
      BrokerMetadata(2, Some("rack3"))
    )

    // Assign replicas with incomplete rack information
    assignReplicasToBrokers(brokerMetadatas, 3, 2)
  }

}
