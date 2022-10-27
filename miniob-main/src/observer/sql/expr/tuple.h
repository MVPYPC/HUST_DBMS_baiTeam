/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2021/5/14.
//

#pragma once

#include <memory>
#include <vector>

#include "common/log/log.h"
#include "sql/parser/parse.h"
#include "sql/expr/tuple_cell.h"
#include "sql/expr/expression.h"
#include "storage/record/record.h"

class Table;

class TupleCellSpec
{
public:
  TupleCellSpec() = default;
  TupleCellSpec(Expression *expr) : expression_(expr)
  {}

  ~TupleCellSpec()
  {
    if (expression_) {
      delete expression_;
      expression_ = nullptr;
    }
  }

  AggType agg_type = AGG_NULL;
  AttrType attr_type = UNDEFINED;

  void set_alias(const char *alias)
  {
    this->alias_ = alias;
  }
  const char *alias() const
  {
    return alias_;
  }

  void set_table_alias(const char *table_alias)
  {
    this->table_alias_ = table_alias;
  }

  const char *table_alias() const
  {
    return table_alias_;
  }

  Expression *expression() const
  {
    return expression_;
  }

private:
  const char *alias_ = nullptr;
  const char *table_alias_ = nullptr;
  Expression *expression_ = nullptr;
};

class Tuple
{
public:
  Tuple() = default;
  virtual ~Tuple() = default;

  virtual int cell_num() const = 0; 
  virtual RC  cell_at(int index, TupleCell &cell) const = 0;
  virtual RC  find_cell(const Field &field, TupleCell &cell) const = 0;
  virtual RC  find_cell(const char * table_name, const char * field_name, TupleCell &cell) const {
    return RC::UNIMPLENMENT;
  }
  virtual RC  cell_spec_at(int index, const TupleCellSpec *&spec) const = 0;
};

class RowTuple : public Tuple
{
public:
  RowTuple() = default;
  virtual ~RowTuple()
  {
    for (TupleCellSpec *spec : speces_) {
      delete spec;
    }
    speces_.clear();
  }
  
  void set_record(Record *record)
  {
    this->record_ = record;
  }

  void set_schema(const Table *table, const std::vector<FieldMeta> *fields)
  {
    table_ = table;
    this->speces_.reserve(fields->size());
    for (const FieldMeta &field : *fields) {
      speces_.push_back(new TupleCellSpec(new FieldExpr(table, &field)));
    }
  }

  int cell_num() const override
  {
    return speces_.size();
  }

  RC cell_at(int index, TupleCell &cell) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }

    const TupleCellSpec *spec = speces_[index];
    FieldExpr *field_expr = (FieldExpr *)spec->expression();
    const FieldMeta *field_meta = field_expr->field().meta();
    cell.set_type(field_meta->type());
    cell.set_data(this->record_->data() + field_meta->offset());
    cell.set_length(field_meta->len());
    return RC::SUCCESS;
  }

  RC find_cell(const Field &field, TupleCell &cell) const override
  {
    const char *table_name = field.table_name();
    if (0 != strcmp(table_name, table_->name())) {
      return RC::NOTFOUND;
    }

    const char *field_name = field.field_name();
    for (size_t i = 0; i < speces_.size(); ++i) {
      const FieldExpr * field_expr = (const FieldExpr *)speces_[i]->expression();
      const Field &field = field_expr->field();
      if (0 == strcmp(field_name, field.field_name())) {
	return cell_at(i, cell);
      }
    }
    return RC::NOTFOUND;
  }

  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }

  Record &record()
  {
    return *record_;
  }

  const Record &record() const
  {
    return *record_;
  }
private:
  Record *record_ = nullptr;
  const Table *table_ = nullptr;
  std::vector<TupleCellSpec *> speces_;
};

/*
class CompositeTuple : public Tuple
{
public:
  int cell_num() const override; 
  RC  cell_at(int index, TupleCell &cell) const = 0;
private:
  int cell_num_ = 0;
  std::vector<Tuple *> tuples_;
};
*/

class JoinTuple : public Tuple
{
public:
  JoinTuple() = default;
  JoinTuple(JoinTuple & rhs) = default;
  virtual ~JoinTuple()
  {
    for (TupleCellSpec *spec : speces_) {
      delete spec;
    }
    speces_.clear();
  }

  void set_records(std::vector<Record *> *records)
  {
    this->records_ = *records;
  }

  void add_schema(const Table *table, const std::vector<FieldMeta> *fields)
  {
    for (auto &field : *fields)
    {
      speces_.push_back(new TupleCellSpec(new FieldExpr(table, &field)));
    }
  }

  int cell_num() const override
  {
    return speces_.size();
  }

  RC cell_at(int index, TupleCell &cell) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }

    const TupleCellSpec *spec = speces_[index];
    FieldExpr *field_expr = (FieldExpr *)spec->expression();
    const FieldMeta *field_meta = field_expr->field().meta();
    cell.set_type(field_meta->type());
    cell.set_data(records_.at(record_index(index))->data() + field_meta->offset());
    cell.set_length(field_meta->len());
    return RC::SUCCESS;
  }

  RC find_cell(const Field &field, TupleCell &cell) const override
  {
    const char *table_name = field.table_name();
    const char *field_name = field.field_name();
    for (size_t i = 0; i < speces_.size(); ++i) {
      const FieldExpr * field_expr = (const FieldExpr *)speces_[i]->expression();
      const Field &exprField = field_expr->field();
      if ((0 == strcmp(table_name, exprField.table_name())) and (0 == strcmp(field_name, exprField.field_name()))) {
        return cell_at(i, cell);
      }
    }
    return RC::NOTFOUND;
  }

  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }

  RC find_cell(const char *table_name, const char *field_name, TupleCell &cell) const override
  {
    for (size_t index = 0; index < speces_.size(); index++) {
      const FieldExpr * field_expr = (const FieldExpr *)speces_[index]->expression();
      const Field &exprField = field_expr->field();
      if (( table_name == nullptr || (0 == strcmp(table_name, exprField.table_name())))
          and (0 == strcmp(field_name, exprField.field_name()))) {
        return cell_at(index, cell);
      }
    }
    return RC::NOTFOUND;
  }
  
  // std::vector<Record *> *records()
  // {
  //   return records_;
  // }

  // const std::vector<Record *> *records() const
  // {
  //   return records_;
  // }

  int32_t record_index(int32_t index) const {
    for (int ri = 0; ri < (int32_t)sum_.size() - 1; ri++) {
      if (sum_[ri + 1] > index) {
        return ri;
      }
    }
    return (int32_t)sum_.size() - 1;
  }

  void add_sum(int32_t current_sum)
  {
    sum_.push_back(current_sum);
  }

private:
  std::vector<int32_t> sum_;
  std::vector<Record *> records_;
  std::vector<TupleCellSpec *> speces_;
};

class ProjectTuple : public Tuple
{
public:
  ProjectTuple() = default;
  virtual ~ProjectTuple()
  {
    for (TupleCellSpec *spec : speces_) {
      delete spec;
    }
    speces_.clear();
  }

  void set_tuple(Tuple *tuple)
  {
    this->tuple_ = tuple;
  }

  void add_cell_spec(TupleCellSpec *spec)
  {
    speces_.push_back(spec);
  }
  int cell_num() const override
  {
    return speces_.size();
  }

  RC cell_at(int index, TupleCell &cell) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      return RC::GENERIC_ERROR;
    }
    if (tuple_ == nullptr) {
      return RC::GENERIC_ERROR;
    }

    const TupleCellSpec *spec = speces_[index];
    return spec->expression()->get_value(*tuple_, cell);
  }

  RC find_cell(const Field &field, TupleCell &cell) const override
  {
    return tuple_->find_cell(field, cell);
  }

  RC find_cell(const char *table_name, const char *field_name, TupleCell &cell) const override
  {
    return tuple_->find_cell(table_name, field_name, cell);
  }

  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      return RC::NOTFOUND;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }
  std::vector<TupleCellSpec *> speces_;

private:
  Tuple *tuple_ = nullptr;
};

class GroupTuple: public Tuple {
public:
  ~GroupTuple() override = default;

  int cell_num() const override
  {
    return speces_.size();
  }

  RC cell_at(int index, TupleCell &cell) const override
  {
    cell = tuple_cells_[index];
    return SUCCESS;
  }

  RC find_cell(const Field &field, TupleCell &cell) const override
  {
    const char *table_name = field.table_name();
    const char *field_name = field.field_name();
    for (size_t i = 0; i < speces_.size(); ++i) {
      const FieldExpr * field_expr = (const FieldExpr *)speces_[i]->expression();
      const Field &exprField = field_expr->field();
      if ((0 == strcmp(table_name, exprField.table_name())) and (0 == strcmp(field_name, exprField.field_name()))) {
        return cell_at(i, cell);
      }
    }
    return RC::NOTFOUND;
  }

  RC find_cell(const char *table_name, const char *field_name, TupleCell &cell) const override
  {
    for (size_t i = 0; i < speces_.size(); ++i) {
      const FieldExpr * field_expr = (const FieldExpr *)speces_[i]->expression();
      const Field &exprField = field_expr->field();
      if ((0 == strcmp(table_name, exprField.table_name())) and (0 == strcmp(field_name, exprField.field_name()))) {
        return cell_at(i, cell);
      }
    }
    return RC::NOTFOUND;
  }

  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override
  {
    spec = speces_[index];
    return SUCCESS;
  }
  RC get_max(int idx, std::vector<Tuple *> tmp_tuples, AttrType attrType, TupleCell & tuple_cell)
  {
    tuple_cell.set_type(attrType);
    switch (attrType) {
      case DATES:
      case INTS: {
        int mx = INT32_MIN;
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          mx = std::max(mx, *(int *)tmp_cell.data());
        }
        tuple_cell.set_data((char *)(new int32_t(mx)));
        tuple_cell.set_length(sizeof(int32_t));
        break;
      }
      case CHARS: {
        char *mx = nullptr;
        int mx_length = 0;
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          if (mx != nullptr) {
            if (strcmp(mx, (char *)tmp_cell.data()) < 0) {
              mx = (char *)tmp_cell.data();
              mx_length = tmp_cell.length();
            }
          } else {
            mx = (char *)tmp_cell.data();
            mx_length = tmp_cell.length();
          }
        }
        char * new_data = (char *)malloc(sizeof(char) * mx_length);
        memcpy(new_data, mx, mx_length);
        tuple_cell.set_data(new_data);
        tuple_cell.set_length(mx_length);
        break;
      } 
      case FLOATS: {
        float mx = -1e20;
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          mx = std::max(mx, *(float *)tmp_cell.data());
        }
        tuple_cell.set_data((char *)(new float (mx)));
        tuple_cell.set_length(sizeof(float));
        break;
      }
      default:
        return RC::GENERIC_ERROR;
    }
    return RC::SUCCESS;
  }

  RC get_min(int idx, std::vector<Tuple *> tmp_tuples, AttrType attrType, TupleCell & tuple_cell)
  {
    tuple_cell.set_type(attrType);
    switch (attrType) {
      case DATES:
      case INTS: {
        int mn = INT32_MAX;
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          mn = std::min(mn, *(int *)tmp_cell.data());
        }
        tuple_cell.set_data((char *)(new int32_t(mn)));
        tuple_cell.set_length(sizeof(int32_t));
        break;
      }
      case CHARS: {
        char *mn = nullptr;
        int mn_length = 0;
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          if (mn != nullptr) {
            if (strcmp(mn, (char *)tmp_cell.data()) > 0) {
              mn = (char *)tmp_cell.data();
              mn_length = tmp_cell.length();
            }
          } else {
            mn = (char *)tmp_cell.data();
            mn_length = tmp_cell.length();
          }
        }
        char * new_data = (char *)malloc(sizeof(char) * mn_length);
        memcpy(new_data, mn, mn_length);
        tuple_cell.set_data(new_data);
        tuple_cell.set_length(mn_length);
        break;
      } 
      case FLOATS: {
        float mn = 1e20;
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          mn = std::min(mn, *(float *)tmp_cell.data());
        }
        tuple_cell.set_data((char *)(new float (mn)));
        tuple_cell.set_length(sizeof(float));
        break;
      }
      default:
        return RC::GENERIC_ERROR;
    }
    return RC::SUCCESS;
  }

  RC get_count(int idx, std::vector<Tuple *> tmp_tuples, AttrType attrType, TupleCell & tuple_cell)
  {
    tuple_cell.set_type(INTS);
    tuple_cell.set_data((char *)(new int32_t(tmp_tuples.size())));
    tuple_cell.set_length(sizeof(int32_t));
    return SUCCESS;
  }

  RC get_avg(int idx, std::vector<Tuple *> tmp_tuples, AttrType attrType, TupleCell & tuple_cell)
  {
    tuple_cell.set_type(FLOATS);
    float sum = 0;
    int count = 0;
    switch (attrType) {
      case INTS: {
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          sum += *(int *)tmp_cell.data();
          count++;
        }
        break;
      }
      case FLOATS: {
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          sum += *(float *)tmp_cell.data();
          count++;
        }
        break;
      }
      case CHARS: {
        for (auto tuple : tmp_tuples) {
          TupleCell tmp_cell;
          tuple->cell_at(idx, tmp_cell);
          int length = tmp_cell.length();
          int dot_count = 0;
          int s = 0;
          const char * data = tmp_cell.data();
          for (s = 0; s < length; s++) {
            if (data[s] == '.' && dot_count == 0) {
              dot_count++;
              continue;
            }
            if (!(data[s] < '0' || data[s] > '9')) {
              continue;
            }
            break;
          }
          float val;
          if (s != 0) {
            char *str = (char *)malloc(sizeof(char) * (s + 1));
            memcpy(str, data, s);
            str[s] = '\0';
            val = std::stof(str);
          } else {
            val = 0;
          }
          sum += val;
          count++;
        }
        break;
      } 
      default:
        return RC::GENERIC_ERROR;
    }
    count = (count == 0) ? 1 : count;
    float precised_ans = (int)(sum / count * 100) / 100.0;
    tuple_cell.set_data((char *)(new float(precised_ans)));
    tuple_cell.set_length(sizeof(float));
    return RC::SUCCESS;
  }

  RC get_null(int idx, std::vector<Tuple *> tmp_tuples, AttrType attrType, TupleCell & tuple_cell)
  {
    tuple_cell.set_type(attrType);
    TupleCell tmp_cell;
    tmp_tuples[0]->cell_at(idx, tmp_cell);
    tuple_cell = tmp_cell;
    return SUCCESS;
  }

  RC set_tuple_cells(int idx, std::vector<Tuple *> tmp_tuples) {
    AggType agg_type = speces_[idx]->agg_type;
    AttrType attr_type = speces_[idx]->attr_type;
    TupleCell tupleCell;
    switch (agg_type) {
      case AGG_MAX:
        get_max(idx, tmp_tuples, attr_type, tupleCell);
        break;
      case AGG_MIN:
        get_min(idx, tmp_tuples, attr_type, tupleCell);
        break;
      case AGG_COUNT:
        get_count(idx, tmp_tuples, attr_type, tupleCell);
        break;
      case AGG_AVG:
        get_avg(idx, tmp_tuples, attr_type, tupleCell);
        break;
      case AGG_NULL:
        get_null(idx, tmp_tuples, attr_type, tupleCell);
    }
    tuple_cells_.push_back(tupleCell);
    return SUCCESS;
  }
  std::vector<TupleCellSpec *> speces_;
  std::vector<TupleCell> tuple_cells_;
};