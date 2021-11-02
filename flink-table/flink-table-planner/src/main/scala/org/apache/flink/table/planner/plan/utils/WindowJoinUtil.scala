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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.ExpressionFormat
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.`trait`.RelWindowProperties
import org.apache.flink.table.planner.utils.Logging

import org.apache.calcite.rex.{RexInputRef, RexNode, RexUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Util for window join.
 */
object WindowJoinUtil extends Logging {

  /**
   * Get window properties of left and right child.
   * 从左侧和右侧的子节点获取window属性
   *
   * @param join input join
   * @return window properties of left and right child.
   */
  def getChildWindowProperties(
      join: FlinkLogicalJoin): (RelWindowProperties, RelWindowProperties) = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(join.getCluster.getMetadataQuery)
    (fmq.getRelWindowProperties(join.getLeft), fmq.getRelWindowProperties(join.getRight))
  }

  /**
   * Checks whether join could be translated to windowJoin. it needs satisfy all the following
   * 检查 join 是否可以转换为 windowJoin。 它需要满足以下所有条件
   * 4 conditions:
   * 4个条件
   * 1) both two input nodes has window properties
   * 1) 两个输入节点都有窗口属性
   * 2) time attribute type of left input node is equals to the one of right input node
   * 2) 左输入节点的时间属性类型等于右输入节点之一
   * 3) window specification of left input node is equals to the one of right input node
   * 3) 左输入节点的窗口规格等于右输入节点之一
   * 4) join condition contains window starts equality of input tables and window
   * ends equality of input tables
   * 4) 连接条件包含输入表的窗口开始等式和输入表的窗口结束等式
   *
   * @param join input join
   * @return True if join condition contains window starts equality of input tables and window
   *         ends equality of input tables, else false.
   *         如果连接条件包含窗口开始输入表等式和窗口结束输入表等式为真，否则为假。
   */
  def satisfyWindowJoin(join: FlinkLogicalJoin): Boolean = {
    excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(join) match {
      case Some((windowStartEqualityLeftKeys, windowEndEqualityLeftKeys, _, _)) =>
        windowStartEqualityLeftKeys.nonEmpty && windowEndEqualityLeftKeys.nonEmpty
      case _ => false
    }
  }

  /**
   * Excludes window starts equality and window ends equality from join info.
   * 从连接信息中排除窗口开始相等和窗口结束相等。
   *
   * @param join a join which could be translated to window join, please see
   *             [[WindowJoinUtil.satisfyWindowJoin()]].
   * @return Remain join information after excludes window starts equality and window ends
   *         equality from join.
   *         The first element is left join keys of window starts equality,
   *         the second element is left join keys of window ends equality,
   *         the third element is right join keys of window starts equality,
   *         the forth element is right join keys of window ends equality,
   *         the fifth element is remain left join keys,
   *         the sixth element is remain right join keys,
   *         the last element is remain join condition which includes remain equal condition and
   *         non-equal condition.
   *         排除窗口开始等式和窗口结束等式后保留连接信息。
   *         * 第一个元素是窗口开始相等的左连接键，
   *         * 第二个元素是窗口结束相等的左连接键，
   *         * 第三个元素是窗口开始相等的右连接键，
   *         * 第四个元素是窗口结束相等的右连接键，
   *         * 第五个元素是保留左连接键，
   *         * 第六个元素是保留右连接键，
   *         * 最后一个元素是保持连接条件，包括保持相等条件和不等条件。
   */
  def excludeWindowStartEqualityAndEndEqualityFromWindowJoinCondition(
      join: FlinkLogicalJoin): (
    Array[Int], Array[Int], Array[Int], Array[Int], Array[Int], Array[Int], RexNode) = {
    val analyzeResult =
      excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(join)
    if (analyzeResult.isEmpty) {
      throw new IllegalArgumentException(
        "Pleas give a Join which could be translated to window join!")
    }
    val (
      windowStartEqualityLeftKeys,
      windowEndEqualityLeftKeys,
      windowStartEqualityRightKeys,
      windowEndEqualityRightKeys) = analyzeResult.get
    val joinSpec = JoinUtil.createJoinSpec(join)
    val (remainLeftKeys, remainRightKeys, remainCondition) = if (
      windowStartEqualityLeftKeys.nonEmpty || windowEndEqualityLeftKeys.nonEmpty) {
      val leftChildFieldsType = join.getLeft.getRowType.getFieldList
      val rightChildFieldsType = join.getRight.getRowType.getFieldList
      val leftFieldCnt = join.getLeft.getRowType.getFieldCount
      val rexBuilder = join.getCluster.getRexBuilder
      val remainingConditions = mutable.ArrayBuffer[RexNode]()
      val remainLeftKeysArray = mutable.ArrayBuffer[Int]()
      val remainRightKeysArray = mutable.ArrayBuffer[Int]()
      // convert remain pairs to RexInputRef tuple for building SqlStdOperatorTable.EQUALS calls
      // or SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
      joinSpec.getLeftKeys.zip(joinSpec.getRightKeys).
        zip(joinSpec.getFilterNulls).foreach { case ((source, target), filterNull) =>
        if (!windowStartEqualityLeftKeys.contains(source) &&
          !windowEndEqualityLeftKeys.contains(source)) {
          val leftFieldType = leftChildFieldsType.get(source).getType
          val leftInputRef = new RexInputRef(source, leftFieldType)
          val rightFieldType = rightChildFieldsType.get(target).getType
          val rightIndex = leftFieldCnt + target
          val rightInputRef = new RexInputRef(rightIndex, rightFieldType)
          val op = if (filterNull) {
            SqlStdOperatorTable.EQUALS
          } else {
            SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
          }
          val remainEqual = rexBuilder.makeCall(op, leftInputRef, rightInputRef)
          remainingConditions += remainEqual
          remainLeftKeysArray += source
          remainRightKeysArray += target
        }
      }
      val notEquiCondition = joinSpec.getNonEquiCondition
      if (notEquiCondition.isPresent) {
        remainingConditions += notEquiCondition.get()
      }
      (
        remainLeftKeysArray.toArray,
        remainRightKeysArray.toArray,
        // build a new condition
        RexUtil.composeConjunction(rexBuilder, remainingConditions.toList))
    } else {
      (joinSpec.getLeftKeys, joinSpec.getRightKeys, join.getCondition)
    }

    (
      windowStartEqualityLeftKeys,
      windowEndEqualityLeftKeys,
      windowStartEqualityRightKeys,
      windowEndEqualityRightKeys,
      remainLeftKeys,
      remainRightKeys,
      remainCondition)
  }

  /**
   * Analyzes input join node. If the join could be translated to windowJoin, excludes window
   * start equality and window end equality from JoinInfo pairs.
   * 分析输入连接节点。 如果连接可以转换为 windowJoin，则从 JoinInfo 对中排除窗口开始相等和窗口结束相等。
   *
   * @param join  the join to split
   * @return If the join could not translated to window join, the result
   *         is [[Option.empty]]; else the result contains a tuple with 4 arrays.
   *         1) The first array contains left join keys of window starts equality,
   *         2) the second array contains left join keys of window ends equality,
   *         3) the third array contains right join keys of window starts equality,
   *         4) the forth array contains right join keys of window ends equality.
   *         如果连接无法转换为窗口连接，则结果为 [[Option.empty]]; 否则结果包含一个有 4 个数组的元组。
   *         * 1) 第一个数组包含窗口开始相等的左连接键，
   *         * 2) 第二个数组包含窗口结束相等的左连接键，
   *         * 3) 第三个数组包含窗口开始相等的右连接键，
   *         * 4) 第四个数组包含窗口结束相等的右连接键。
   */
  private def excludeWindowStartEqualityAndEndEqualityFromJoinInfoPairs(
      join: FlinkLogicalJoin): Option[(Array[Int], Array[Int], Array[Int], Array[Int])] = {
    val joinInfo = join.analyzeCondition()
    val (leftWindowProperties, rightWindowProperties) = getChildWindowProperties(join)

    if (leftWindowProperties == null || rightWindowProperties == null) {
      return Option.empty
    }
    val windowStartEqualityLeftKeys = mutable.ArrayBuffer[Int]()
    val windowEndEqualityLeftKeys = mutable.ArrayBuffer[Int]()
    val windowStartEqualityRightKeys = mutable.ArrayBuffer[Int]()
    val windowEndEqualityRightKeys = mutable.ArrayBuffer[Int]()

    val leftWindowStartColumns = leftWindowProperties.getWindowStartColumns
    val rightWindowStartColumns = rightWindowProperties.getWindowStartColumns
    val leftWindowEndColumns = leftWindowProperties.getWindowEndColumns
    val rightWindowEndColumns = rightWindowProperties.getWindowEndColumns

    joinInfo.pairs().foreach { pair =>
      val leftKey = pair.source
      val rightKey = pair.target
      if (leftWindowStartColumns.get(leftKey) && rightWindowStartColumns.get(rightKey)) {
        windowStartEqualityLeftKeys.add(leftKey)
        windowStartEqualityRightKeys.add(rightKey)
      } else if (leftWindowEndColumns.get(leftKey) && rightWindowEndColumns.get(rightKey)) {
        windowEndEqualityLeftKeys.add(leftKey)
        windowEndEqualityRightKeys.add(rightKey)
      }
    }

    // Validate join
    if (windowStartEqualityLeftKeys.nonEmpty && windowEndEqualityLeftKeys.nonEmpty) {
      if (
        leftWindowProperties.getTimeAttributeType != rightWindowProperties.getTimeAttributeType) {
        LOG.warn(
          "Currently, window join doesn't support different time attribute type of left and " +
            "right inputs.\n" +
            s"The left time attribute type is " +
            s"${leftWindowProperties.getTimeAttributeType}.\n" +
            s"The right time attribute type is " +
            s"${rightWindowProperties.getTimeAttributeType}.")
        Option.empty
      } else if (leftWindowProperties.getWindowSpec != rightWindowProperties.getWindowSpec) {
        LOG.warn(
          "Currently, window join doesn't support different window table function of left and " +
            "right inputs.\n" +
            s"The left window table function is ${leftWindowProperties.getWindowSpec}.\n" +
            s"The right window table function is ${rightWindowProperties.getWindowSpec}.")
        Option.empty
      } else {
        Option(
          windowStartEqualityLeftKeys.toArray,
          windowEndEqualityLeftKeys.toArray,
          windowStartEqualityRightKeys.toArray,
          windowEndEqualityRightKeys.toArray
        )
      }
    } else if (windowStartEqualityLeftKeys.nonEmpty || windowEndEqualityLeftKeys.nonEmpty) {
      val leftFieldNames = join.getLeft.getRowType.getFieldNames.toList
      val rightFieldNames = join.getRight.getRowType.getFieldNames.toList
      val inputFieldNames = leftFieldNames ++ rightFieldNames
      val condition = join.getExpressionString(
        join.getCondition,
        inputFieldNames,
        None,
        ExpressionFormat.Infix)
      LOG.warn(
        "Currently, window join requires JOIN ON condition must contain both window starts " +
          "equality of input tables and window ends equality of input tables.\n" +
          s"But the current JOIN ON condition is $condition.")
      Option.empty
    } else {
      Option.empty
    }
  }
}
