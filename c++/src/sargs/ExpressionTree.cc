/**
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

#include "ExpressionTree.hh"

#include <cassert>
#include <sstream>

namespace orc {

  ExpressionTree::ExpressionTree(Operator op)
      : mOperator_(op), mLeaf_(UNUSED_LEAF), mConstant_(TruthValue::YES_NO_NULL) {}

  ExpressionTree::ExpressionTree(Operator op, std::initializer_list<TreeNode> children)
      : mOperator_(op),
        mChildren_(children.begin(), children.end()),
        mLeaf_(UNUSED_LEAF),
        mConstant_(TruthValue::YES_NO_NULL) {
    // PASS
  }

  ExpressionTree::ExpressionTree(size_t leaf)
      : mOperator_(Operator::LEAF),
        mChildren_(),
        mLeaf_(leaf),
        mConstant_(TruthValue::YES_NO_NULL) {
    // PASS
  }

  ExpressionTree::ExpressionTree(TruthValue constant)
      : mOperator_(Operator::CONSTANT), mChildren_(), mLeaf_(UNUSED_LEAF), mConstant_(constant) {
    // PASS
  }

  ExpressionTree::ExpressionTree(const ExpressionTree& other)
      : mOperator_(other.mOperator_), mLeaf_(other.mLeaf_), mConstant_(other.mConstant_) {
    for (TreeNode child : other.mChildren_) {
      mChildren_.emplace_back(std::make_shared<ExpressionTree>(*child));
    }
  }

  ExpressionTree::Operator ExpressionTree::getOperator() const {
    return mOperator_;
  }

  const std::vector<TreeNode>& ExpressionTree::getChildren() const {
    return mChildren_;
  }

  std::vector<TreeNode>& ExpressionTree::getChildren() {
    return const_cast<std::vector<TreeNode>&>(
        const_cast<const ExpressionTree*>(this)->getChildren());
  }

  const TreeNode ExpressionTree::getChild(size_t i) const {
    return mChildren_.at(i);
  }

  TreeNode ExpressionTree::getChild(size_t i) {
    return std::const_pointer_cast<ExpressionTree>(
        const_cast<const ExpressionTree*>(this)->getChild(i));
  }

  TruthValue ExpressionTree::getConstant() const {
    assert(mOperator_ == Operator::CONSTANT);
    return mConstant_;
  }

  size_t ExpressionTree::getLeaf() const {
    assert(mOperator_ == Operator::LEAF);
    return mLeaf_;
  }

  void ExpressionTree::setLeaf(size_t leaf) {
    assert(mOperator_ == Operator::LEAF);
    mLeaf_ = leaf;
  }

  void ExpressionTree::addChild(TreeNode child) {
    mChildren_.push_back(child);
  }

  TruthValue ExpressionTree::evaluate(const std::vector<TruthValue>& leaves) const {
    TruthValue result;
    switch (mOperator_) {
      case Operator::OR: {
        result = mChildren_.at(0)->evaluate(leaves);
        for (size_t i = 1; i < mChildren_.size() && !isNeeded(result); ++i) {
          result = mChildren_.at(i)->evaluate(leaves) || result;
        }
        return result;
      }
      case Operator::AND: {
        result = mChildren_.at(0)->evaluate(leaves);
        for (size_t i = 1; i < mChildren_.size() && isNeeded(result); ++i) {
          result = mChildren_.at(i)->evaluate(leaves) && result;
        }
        return result;
      }
      case Operator::NOT:
        return !mChildren_.at(0)->evaluate(leaves);
      case Operator::LEAF:
        return leaves[mLeaf_];
      case Operator::CONSTANT:
        return mConstant_;
      default:
        throw std::invalid_argument("Unknown operator!");
    }
  }

  std::string to_string(TruthValue truthValue) {
    switch (truthValue) {
      case TruthValue::YES:
        return "YES";
      case TruthValue::NO:
        return "NO";
      case TruthValue::IS_NULL:
        return "IS_NULL";
      case TruthValue::YES_NULL:
        return "YES_NULL";
      case TruthValue::NO_NULL:
        return "NO_NULL";
      case TruthValue::YES_NO:
        return "YES_NO";
      case TruthValue::YES_NO_NULL:
        return "YES_NO_NULL";
      default:
        throw std::invalid_argument("unknown TruthValue!");
    }
  }

  std::string ExpressionTree::toString() const {
    std::ostringstream sstream;
    switch (mOperator_) {
      case Operator::OR:
        sstream << "(or";
        for (const auto& child : mChildren_) {
          sstream << ' ' << child->toString();
        }
        sstream << ')';
        break;
      case Operator::AND:
        sstream << "(and";
        for (const auto& child : mChildren_) {
          sstream << ' ' << child->toString();
        }
        sstream << ')';
        break;
      case Operator::NOT:
        sstream << "(not " << mChildren_.at(0)->toString() << ')';
        break;
      case Operator::LEAF:
        sstream << "leaf-" << mLeaf_;
        break;
      case Operator::CONSTANT:
        sstream << to_string(mConstant_);
        break;
      default:
        throw std::invalid_argument("unknown operator!");
    }
    return sstream.str();
  }

}  // namespace orc
