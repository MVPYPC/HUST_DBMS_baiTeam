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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/common/db.h"
#include "storage/common/table.h"

InsertStmt::InsertStmt(Table *table, const Value *values, int value_amount)
  : table_ (table), values_(values), value_amount_(value_amount)
{}

RC InsertStmt::create(Db *db, const Inserts &inserts, Stmt *&stmt)
{
  const char *table_name = inserts.relation_name;
  if (nullptr == db || nullptr == table_name || inserts.records[0].value_num <= 0) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d", 
             db, table_name, inserts.records[0].value_num);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check the fields number
  const Value *values = inserts.records[0].values;
  const int value_num = inserts.records[0].value_num;
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num() - table_meta.sys_field_num();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // check fields type
  const int sys_field_num = table_meta.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    const AttrType field_type = field_meta->type();
    const AttrType value_type = values[i].type;
    if (field_type != value_type) { // TODO try to convert the value type to field type
      LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d", 
               table_name, field_meta->name(), field_type, value_type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    } else if (value_type == DATES) {
      int val = *(int *)values[i].data;
      if (!check_date(val)) {
        LOG_WARN("date type invalid. table=%s, field=%s, field type=%d, value_type=%d",
            table_name, field_meta->name(), field_type, value_type);
        return RC::GENERIC_ERROR;
      }
    }
  }

  // everything alright
  stmt = new InsertStmt(table, values, value_num);
  return RC::SUCCESS;
}

bool InsertStmt::check_date(int val)
{
  int year = val / 10000;
  int month = val % 10000 / 100;
  int day = val % 100;
  if (year < 1000 || year > 9999) return 0;
  if (month < 1 || month > 12) return 0;
  int mday; // mday记录当月天数
  if (month == 2) {
    if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0) mday = 29; // 闰年
    else mday = 28;
  } else if (month == 4 || month == 6 || month == 9 || month == 11) {
    mday = 30;
  } else 
    mday = 31;
  if (day > mday) return 0;
  return 1;
}