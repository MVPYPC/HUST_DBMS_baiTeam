#include "tables_join_operator.h"
#include "table_scan_operator.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/field.h"
#include "util/comparator.h"

RC TablesJoinPredOperator::open()
{
  for (auto it = scan_opers_.rbegin(); it != scan_opers_.rend(); it++)
  {
    auto &scan_oper = *it;
    std::vector<Record *> table_records;
    RC rc = scan_oper->open();
    if (rc == RC::SUCCESS) {
      while (scan_oper->next() == RC::SUCCESS) {
        auto tuple = static_cast<RowTuple*>(scan_oper->current_tuple());
        table_records.push_back(new Record(tuple->record().rid(), tuple->record().data()));
      }

      for (auto &field : *scan_oper->table()->table_meta().field_metas())
      {
        speces_.push_back(new TupleCellSpec(new FieldExpr(scan_oper->table(), &field)));
      }
      tuple_.add_schema(scan_oper->table(), scan_oper->table()->table_meta().field_metas());
      tuple_.add_sum(field_length_);
      field_length_ += scan_oper->table()->table_meta().field_num();
      field_lengths_.push_back(field_length_);
      index_mul_.push_back({(int32_t)table_records.size(), total_index_});
      total_index_ *= table_records.size();
      records_.push_back(table_records);
    } else {
      return rc;
    }
  }
  current_records_.clear();
  cartesian_product_dfs_(0);
  return RC::SUCCESS;
}

RC TablesJoinPredOperator::next()
{
  if (current_index_ >= (int32_t)product_records_.size()) {
    return RC::RECORD_EOF;
  }
  current_records_ = product_records_[current_index_];
  // for (int32_t table_index = 0; table_index < (int32_t) records_.size(); table_index++)
  // {
  //   auto current_row = records_[table_index];
  //   auto new_record = current_row[get_row_index_(table_index)];
  //   current_records_.push_back(new_record);
  // }
  current_index_++;
  return RC::SUCCESS;
}

RC TablesJoinPredOperator::close()
{
  RC rc = RC::SUCCESS;
  for (auto scan_oper : scan_opers_) {
    if ((rc = scan_oper->close()) != RC::SUCCESS) {
      return rc;
    }
  }
  return rc;
}

Tuple *TablesJoinPredOperator::current_tuple()
{
  tuple_.set_records(&current_records_);
  return &tuple_;
}

int32_t TablesJoinPredOperator::get_field_index_(FieldExpr * fieldExpr)
{
  const char *table_name = fieldExpr->field().table_name();
  const char *field_name = fieldExpr->field().field_name();
  for (size_t index = 0; index < speces_.size(); index++) {
    const FieldExpr * field_expr = (const FieldExpr *)speces_[index]->expression();
    const Field &field = field_expr->field();
    if ((0 == strcmp(table_name, field.table_name())) and (0 == strcmp(field_name, field.field_name()))) {
      return index;
    }
  }
  return INT32_MAX;
}

RC TablesJoinPredOperator::cartesian_product_dfs_(int table_index)
{
  if (!do_predicate_(current_records_,  table_index != 0 ? field_lengths_[table_index - 1] : 0))
    return SUCCESS;
  if (table_index == (int32_t)field_lengths_.size()) {
    product_records_.push_back(current_records_);
    return SUCCESS;
  }
  auto current_row = records_[table_index];
  for (auto rec : records_[table_index]) {
    current_records_.push_back(rec);
    cartesian_product_dfs_(table_index + 1);
    current_records_.pop_back();
  }
  return SUCCESS;
}

bool TablesJoinPredOperator::do_predicate_( std::vector<Record *> &records, int record_length)
{
  if (filter_stmt_ == nullptr || filter_stmt_->filter_units().empty()) {
    return true;
  }

  for (const FilterUnit *filter_unit : filter_stmt_->filter_units()) {
    Expression *left_expr = filter_unit->left();
    Expression *right_expr = filter_unit->right();
    if (left_expr->type() == ExprType::FIELD && get_field_index_((FieldExpr *)left_expr) >= record_length) {
      continue;
    }
    if (right_expr->type() == ExprType::FIELD && get_field_index_((FieldExpr *)right_expr) >= record_length) {
      continue;
    }
    tuple_.set_records(&records);
    CompOp comp = filter_unit->comp();
    TupleCell left_cell;
    TupleCell right_cell;
    left_expr->get_value(tuple_, left_cell);
    right_expr->get_value(tuple_, right_cell);

    const int compare = left_cell.compare(right_cell);
    bool filter_result = false;
    switch (comp) {
      case EQUAL_TO: {
        filter_result = (0 == compare);
      } break;
      case LESS_EQUAL: {
        filter_result = (compare <= 0);
      } break;
      case NOT_EQUAL: {
        filter_result = (compare != 0);
      } break;
      case LESS_THAN: {
        filter_result = (compare < 0);
      } break;
      case GREAT_EQUAL: {
        filter_result = (compare >= 0);
      } break;
      case GREAT_THAN: {
        filter_result = (compare > 0);
      } break;
      default: {
        LOG_WARN("invalid compare type: %d", comp);
      } break;
    }
    if (!filter_result) {
      return false;
    }
  }
  return true;
}