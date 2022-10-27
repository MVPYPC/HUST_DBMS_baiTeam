#pragma once

#include "sql/operator/operator.h"

class GroupByOperator : public Operator {
public:
  GroupByOperator(size_t group_num, GroupAttr *group_attributes)
      :group_num_(group_num), group_attributes_(group_attributes)
  {
    current_tuple_count_ = 0;
  }

  virtual ~GroupByOperator() {
  }

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple * current_tuple() override;
private:
  bool in_group(Tuple * t1, Tuple * t2);
  int32_t current_tuple_count_;
  GroupTuple tuple_;
  size_t group_num_;
  GroupAttr * group_attributes_;
  std::vector<Tuple *> all_tuples_;

};