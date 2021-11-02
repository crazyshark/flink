/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.utils.WindowJoinUtil.{satisfyWindowJoin, excludeWindowStartEqualityAndEndEqualityFromWindowJoinCondition, getChildWindowProperties}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode

/**
 * Rule to convert a [[FlinkLogicalJoin]] into a [[StreamPhysicalWindowJoin]].
 * 将FlinkLogicalJoin转变为StreamPhysicalWindowJoin的规则
 */
class StreamPhysicalWindowJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamPhysicalWindowJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel[FlinkLogicalJoin](0)
    //判断FlinkLogicalJoin能否转变为StreamPhysicalWindowJoin
    //如果连接条件包含窗口开始输入表等式和窗口结束输入表等式为真，否则为假。
    satisfyWindowJoin(join)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {

    //根据连接的列设置distribute trait
    def toHashTraitByColumns(
        columns: Array[Int],
        inputTraitSet: RelTraitSet): RelTraitSet = {
      //如果连接的列为空则为SINGLETON就是不分区
      val distribution = if (columns.isEmpty) {
        FlinkRelDistribution.SINGLETON
      } else {
        //否则就是hash
        FlinkRelDistribution.hash(columns, true)
      }
      //修改trait
      inputTraitSet
        .replace(FlinkConventions.STREAM_PHYSICAL)
        .replace(distribution)
    }
    //转换输入这里主要是设置输入流的数据分布的trait例如是根据连接key hash还是啥
    def convertInput(input: RelNode, columns: Array[Int]): RelNode = {
      //根据连接的列获取distribute trait
      val requiredTraitSet = toHashTraitByColumns(columns, input.getTraitSet)
      //转换trait
      RelOptRule.convert(input, requiredTraitSet)
    }
    //获取join节点
    val join = call.rel[FlinkLogicalJoin](0)
    //从join节点获取连接信息
    val (
    //窗口开始相等的左连接键
      windowStartEqualityLeftKeys,
    //窗口结束相等的左连接键
      windowEndEqualityLeftKeys,
    //窗口开始相等的右连接键
      windowStartEqualityRightKeys,
    //窗口结束相等的右连接键
      windowEndEqualityRightKeys,
    //保留左连接键
      remainLeftKeys,
    //保留右连接键
      remainRightKeys,
    //保留连接条件，包括保留相等条件和不等条件
      remainCondition) = excludeWindowStartEqualityAndEndEqualityFromWindowJoinCondition(join)
    //设置目标convention trait
    val providedTraitSet: RelTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    //获取左侧输入节点
    val left = call.rel[FlinkLogicalRel](1)
    //获取右侧输入节点
    val right = call.rel[FlinkLogicalRel](2)
    //根据连接的key设置左侧输入的数据分布属性就是加不加exchange节点
    val newLeft = convertInput(left, remainLeftKeys)
    //根据连接的key设置右侧输入的数据分布属性就是加不加exchange节点
    val newRight = convertInput(right, remainRightKeys)
    //获取左右侧输入的窗口属性
    val (leftWindowProperties, rightWindowProperties) = getChildWindowProperties(join)
    // It's safe to directly get first element from windowStartEqualityLeftKeys because window
    // start equality is required in join condition for window join.
    //直接从 windowStartEqualityLeftKeys 获取第一个元素是安全的，因为窗口连接的连接条件需要窗口开始相等。
    //左侧的窗口策略，
    val leftWindowing = new WindowAttachedWindowingStrategy(
      //窗口的类型，定义
      leftWindowProperties.getWindowSpec,
      //窗口的时间属性
      leftWindowProperties.getTimeAttributeType,
      //窗口开始相等的左键
      windowStartEqualityLeftKeys(0),
      //窗口结束相等的左键
      windowEndEqualityLeftKeys(0))
    //右侧的窗口策略
    val rightWindowing = new WindowAttachedWindowingStrategy(
      //窗口的类型，定义
      rightWindowProperties.getWindowSpec,
      //窗口的时间属性
      rightWindowProperties.getTimeAttributeType,
      //窗口开始相等的右键
      windowStartEqualityRightKeys(0),
      //窗口结束相等的右键
      windowEndEqualityRightKeys(0))
    //获取window join的物理节点
    val newWindowJoin = new StreamPhysicalWindowJoin(
      join.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getJoinType,
      remainCondition,
      leftWindowing,
      rightWindowing)
    //注册到优化器上
    call.transformTo(newWindowJoin)
  }

}

object StreamPhysicalWindowJoinRule {
  val INSTANCE: RelOptRule = new StreamPhysicalWindowJoinRule
}
