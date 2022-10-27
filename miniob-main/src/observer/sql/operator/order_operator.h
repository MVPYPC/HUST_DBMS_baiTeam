#pragma once

#include "sql/operator/operator.h"

class OrderByOperator : public Operator {
public:
  OrderByOperator(size_t order_num, OrderAttr * order_attributes)
  :order_num_(order_num), order_attributes_(order_attributes)
  {
    current_tuple_count_ = 0;
  }

  virtual ~OrderByOperator() {
//    for (auto &tuple : all_tuples_) {
//      delete tuple;
//    }
//    all_tuples_.clear();
  }

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple * current_tuple() override;
private:
  int32_t current_tuple_count_;
  Tuple * tuple_;
  size_t order_num_;
  OrderAttr * order_attributes_;
  std::vector<Tuple *> all_tuples_;
};