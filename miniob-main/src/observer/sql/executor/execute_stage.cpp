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
// Created by Meiyi & Longda on 2021/4/13.
//

#include <string>
#include <sstream>

#include "execute_stage.h"

#include "common/io/io.h"
#include "common/log/log.h"
#include "common/lang/defer.h"
#include "common/seda/timer_stage.h"
#include "common/lang/string.h"
#include "session/session.h"
#include "event/storage_event.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "sql/expr/tuple.h"
#include "sql/operator/table_scan_operator.h"
#include "sql/operator/index_scan_operator.h"
#include "sql/operator/tables_join_operator.h"
#include "sql/operator/predicate_operator.h"
#include "sql/operator/delete_operator.h"
#include "sql/operator/project_operator.h"
#include "sql/operator/order_operator.h"
#include "sql/operator/group_operator.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/table.h"
#include "storage/common/field.h"
#include "storage/index/index.h"
#include "storage/default/default_handler.h"
#include "storage/common/condition_filter.h"
#include "storage/trx/trx.h"
#include "storage/clog/clog.h"

using namespace common;

//RC create_selection_executor(
//   Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node);

//! Constructor
ExecuteStage::ExecuteStage(const char *tag) : Stage(tag)
{}

//! Destructor
ExecuteStage::~ExecuteStage()
{}

//! Parse properties, instantiate a stage object
Stage *ExecuteStage::make_stage(const std::string &tag)
{
  ExecuteStage *stage = new (std::nothrow) ExecuteStage(tag.c_str());
  if (stage == nullptr) {
    LOG_ERROR("new ExecuteStage failed");
    return nullptr;
  }
  stage->set_properties();
  return stage;
}

//! Set properties for this object set in stage specific properties
bool ExecuteStage::set_properties()
{
  //  std::string stageNameStr(stageName);
  //  std::map<std::string, std::string> section = theGlobalProperties()->get(
  //    stageNameStr);
  //
  //  std::map<std::string, std::string>::iterator it;
  //
  //  std::string key;

  return true;
}

//! Initialize stage params and validate outputs
bool ExecuteStage::initialize()
{
  LOG_TRACE("Enter");

  std::list<Stage *>::iterator stgp = next_stage_list_.begin();
  default_storage_stage_ = *(stgp++);
  mem_storage_stage_ = *(stgp++);

  LOG_TRACE("Exit");
  return true;
}

//! Cleanup after disconnection
void ExecuteStage::cleanup()
{
  LOG_TRACE("Enter");

  LOG_TRACE("Exit");
}

void ExecuteStage::handle_event(StageEvent *event)
{
  LOG_TRACE("Enter\n");

  handle_request(event);

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::callback_event(StageEvent *event, CallbackContext *context)
{
  LOG_TRACE("Enter\n");

  // here finish read all data from disk or network, but do nothing here.

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::handle_request(common::StageEvent *event)
{
  SQLStageEvent *sql_event = static_cast<SQLStageEvent *>(event);
  SessionEvent *session_event = sql_event->session_event();
  Stmt *stmt = sql_event->stmt();
  Session *session = session_event->session();
  Query *sql = sql_event->query();

  if (stmt != nullptr) {
    switch (stmt->type()) {
    case StmtType::SELECT: {
      do_select(sql_event);
    } break;
    case StmtType::INSERT: {
      do_insert(sql_event);
    } break;
    case StmtType::UPDATE: {
      do_update((UpdateStmt *)stmt, session_event);
    } break;
    case StmtType::DELETE: {
      do_delete(sql_event);
    } break;
    default: {
      LOG_WARN("should not happen. please implenment");
    } break;
    }
  } else {
    switch (sql->flag) {
    case SCF_HELP: {
      do_help(sql_event);
    } break;
    case SCF_CREATE_TABLE: {
      do_create_table(sql_event);
    } break;
    case SCF_CREATE_INDEX: {
      do_create_index(sql_event);
    } break;
    case SCF_SHOW_TABLES: {
      do_show_tables(sql_event);
    } break;
    case SCF_DESC_TABLE: {
      do_desc_table(sql_event);
    } break;

    case SCF_DROP_TABLE:
    case SCF_DROP_INDEX:
    case SCF_LOAD_DATA: {
      default_storage_stage_->handle_event(event);
    } break;
    case SCF_SYNC: {
      /*
      RC rc = DefaultHandler::get_default().sync();
      session_event->set_response(strrc(rc));
      */
    } break;
    case SCF_BEGIN: {
      do_begin(sql_event);
      /*
      session_event->set_response("SUCCESS\n");
      */
    } break;
    case SCF_COMMIT: {
      do_commit(sql_event);
      /*
      Trx *trx = session->current_trx();
      RC rc = trx->commit();
      session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      */
    } break;
    case SCF_CLOG_SYNC: {
      do_clog_sync(sql_event);
    }
    case SCF_ROLLBACK: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->rollback();
      session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
    } break;
    case SCF_EXIT: {
      // do nothing
      const char *response = "Unsupported\n";
      session_event->set_response(response);
    } break;
    default: {
      LOG_ERROR("Unsupported command=%d\n", sql->flag);
    }
    }
  }
}

void end_trx_if_need(Session *session, Trx *trx, bool all_right)
{
  if (!session->is_trx_multi_operation_mode()) {
    if (all_right) {
      trx->commit();
    } else {
      trx->rollback();
    }
  }
}

void print_tuple_header(std::ostream &os, Operator &oper)
{
  Tuple * tuple = oper.current_tuple();
  const int cell_num = tuple->cell_num();
  const TupleCellSpec *cell_spec = nullptr;
  for (int i = 0; i < cell_num; i++) {
    tuple->cell_spec_at(i, cell_spec);
    if (i != 0) {
      os << " | ";
    }
    switch (cell_spec->agg_type) {
      case AGG_MAX:
        os << "MAX(";
        break;
      case AGG_MIN:
        os << "MIN(";
        break;
      case AGG_COUNT:
        os << "COUNT(";
        break;
      case AGG_AVG:
        os << "AVG(";
        break;
      default:
        break;
    }
    if (cell_spec->alias()) {
      os << cell_spec->alias();
    }
    if (cell_spec->agg_type != AGG_NULL) {
      os << ")";
    }
  }

  if (cell_num > 0) {
    os << '\n';
  }
}

void print_tuple_header_multi_table(std::ostream &os, Operator &oper)
{
  Tuple * tuple = oper.current_tuple();
  const int cell_num = tuple->cell_num();
  const TupleCellSpec *cell_spec = nullptr;
  for (int i = 0; i < cell_num; i++) {
    tuple->cell_spec_at(i, cell_spec);
    if (i != 0) {
      os << " | ";
    }
    switch (cell_spec->agg_type) {
      case AGG_MAX:
        os << "MAX(";
        break;
      case AGG_MIN:
        os << "MIN(";
        break;
      case AGG_COUNT:
        os << "COUNT(";
        break;
      case AGG_AVG:
        os << "AVG(";
        break;
      default:
        break;
    }
    if (cell_spec->alias()) {
      if (cell_spec->table_alias() != nullptr) {
        os << cell_spec->table_alias() << ".";
      }
      os << cell_spec->alias();
    }
    if (cell_spec->agg_type != AGG_NULL) {
      os << ")";
    }
  }

  if (cell_num > 0) {
    os << '\n';
  }
}

void tuple_to_string(std::ostream &os, const Tuple &tuple)
{
  TupleCell cell;
  RC rc = RC::SUCCESS;
  bool first_field = true;
  for (int i = 0; i < tuple.cell_num(); i++) {
    rc = tuple.cell_at(i, cell);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to fetch field of cell. index=%d, rc=%s", i, strrc(rc));
      break;
    }

    if (!first_field) {
      os << " | ";
    } else {
      first_field = false;
    }
    cell.to_string(os);
  }
}

IndexScanOperator *try_to_create_index_scan_operator(FilterStmt *filter_stmt)
{
  const std::vector<FilterUnit *> &filter_units = filter_stmt->filter_units();
  if (filter_units.empty() ) {
    return nullptr;
  }

  // ????????????????????????????????????????????????????????????????????????????????????????????????????????????
  // ?????????????????????????????????????????????????????????
  // ???????????????????????????????????????????????????????????????????????????????????????
  // ?????????????????????????????????????????????????????????????????????????????????. (???????????????????)
  const FilterUnit *better_filter = nullptr;
  for (const FilterUnit * filter_unit : filter_units) {
    if (filter_unit->comp() == NOT_EQUAL) {
      continue;
    }

    Expression *left = filter_unit->left();
    Expression *right = filter_unit->right();
    if (left->type() == ExprType::FIELD && right->type() == ExprType::VALUE) {
    } else if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
      std::swap(left, right);
    }
    FieldExpr &left_field_expr = *(FieldExpr *)left;
    const Field &field = left_field_expr.field();
    const Table *table = field.table();
    Index *index = table->find_index_by_field(field.field_name());
    if (index != nullptr) {
      if (better_filter == nullptr) {
        better_filter = filter_unit;
      } else if (filter_unit->comp() == EQUAL_TO) {
        better_filter = filter_unit;
    	break;
      }
    }
  }

  if (better_filter == nullptr) {
    return nullptr;
  }

  Expression *left = better_filter->left();
  Expression *right = better_filter->right();
  CompOp comp = better_filter->comp();
  if (left->type() == ExprType::VALUE && right->type() == ExprType::FIELD) {
    std::swap(left, right);
    switch (comp) {
    case EQUAL_TO:    { comp = EQUAL_TO; }    break;
    case LESS_EQUAL:  { comp = GREAT_THAN; }  break;
    case NOT_EQUAL:   { comp = NOT_EQUAL; }   break;
    case LESS_THAN:   { comp = GREAT_EQUAL; } break;
    case GREAT_EQUAL: { comp = LESS_THAN; }   break;
    case GREAT_THAN:  { comp = LESS_EQUAL; }  break;
    default: {
    	LOG_WARN("should not happen");
    }
    }
  }


  FieldExpr &left_field_expr = *(FieldExpr *)left;
  const Field &field = left_field_expr.field();
  const Table *table = field.table();
  Index *index = table->find_index_by_field(field.field_name());
  assert(index != nullptr);

  ValueExpr &right_value_expr = *(ValueExpr *)right;
  TupleCell value;
  right_value_expr.get_tuple_cell(value);

  const TupleCell *left_cell = nullptr;
  const TupleCell *right_cell = nullptr;
  bool left_inclusive = false;
  bool right_inclusive = false;

  switch (comp) {
  case EQUAL_TO: {
    left_cell = &value;
    right_cell = &value;
    left_inclusive = true;
    right_inclusive = true;
  } break;

  case LESS_EQUAL: {
    left_cell = nullptr;
    left_inclusive = false;
    right_cell = &value;
    right_inclusive = true;
  } break;

  case LESS_THAN: {
    left_cell = nullptr;
    left_inclusive = false;
    right_cell = &value;
    right_inclusive = false;
  } break;

  case GREAT_EQUAL: {
    left_cell = &value;
    left_inclusive = true;
    right_cell = nullptr;
    right_inclusive = false;
  } break;

  case GREAT_THAN: {
    left_cell = &value;
    left_inclusive = false;
    right_cell = nullptr;
    right_inclusive = false;
  } break;

  default: {
    LOG_WARN("should not happen. comp=%d", comp);
  } break;
  }

  IndexScanOperator *oper = new IndexScanOperator(table, index,
       left_cell, left_inclusive, right_cell, right_inclusive);

  LOG_INFO("use index for scan: %s in table %s", index->index_meta().name(), table->name());
  return oper;
}

RC ExecuteStage::do_update(UpdateStmt *stmt, SessionEvent *session_event)
{
  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  UpdateStmt *update_stmt = (UpdateStmt *)stmt;
  Table *table = update_stmt->table();
  const FieldMeta *field = table->table_meta().field(update_stmt->field_name());

  TableScanOperator scan_oper(update_stmt->table());
  PredicateOperator pred_oper(update_stmt->filter_stmt());
  pred_oper.add_child(&scan_oper);

  std::vector<Operator *> children;
  children.push_back(&pred_oper);


  if (children.size() != 1) {
    LOG_WARN("delete operator must has 1 child");
    session_event->set_response("FAILURE\n");
    return RC::INTERNAL;
  }

  Operator *child = children[0];
  RC rc = child->open();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    session_event->set_response("FAILURE\n");
    return rc;
  }

  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      session_event->set_response("FAILURE\n");
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &record = row_tuple->record();
    rc = table->update_record(nullptr, &record,field, update_stmt->value());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      session_event->set_response("FAILURE\n");
      return rc;
    }
  }

  session_event->set_response("SUCCESS\n");
  return RC::SUCCESS;
}

RC check_attr_in_group(size_t attr_num, RelAttr * attrs, size_t group_num, GroupAttr * groups)
{
  for (size_t index_1 = 0; index_1 < attr_num; index_1++) {
    RelAttr & attr = attrs[index_1];
    bool inGroup = false;
    if (attr.is_agg){
      continue;
    } 
    for (size_t index_2 = 0; index_2 < group_num; index_2++) {
      GroupAttr & group = groups[index_2];
      if ((strcmp(attr.attribute_name, group.attribute_name) == 0) &&
          ((attr.relation_name == nullptr && group.relation_name == nullptr) || 
            (attr.relation_name != nullptr && group.relation_name != nullptr && strcmp(attr.relation_name, group.relation_name) == 0))) {
        inGroup = true;
      }
    }
    if (!inGroup) {
      return GENERIC_ERROR;
    }
  }
  return SUCCESS;
}

RC add_projection(Db * db, std::vector<Table *> & tables, size_t attr_num, RelAttr * attributes, ProjectOperator *oper) {
  for (size_t index_1 = 0; index_1 < attr_num; index_1++) {
    auto & attr = attributes[index_1];
    // select *
    if (attr.relation_name == nullptr && 0 == strcmp(attr.attribute_name, "*")) {
      if (attr.aggType == AGG_COUNT) {
        oper->add_projection(nullptr, "*", AGG_COUNT);
      } else if(attr.aggType == AGG_NULL){
        for (int index_2 = (int)tables.size() - 1; index_2 >= 0; index_2--) {
          auto &table = tables[index_2];
          const TableMeta &table_meta = table->table_meta();
          const int field_num = table_meta.field_num();
          for (int index_3 = table_meta.sys_field_num(); index_3 < field_num; index_3++) {
            oper->add_projection(table, table_meta.field(index_3), attr.aggType);
          }
        }
      } else  return GENERIC_ERROR;
    } else {
      Table * table = attr.relation_name == nullptr ? tables[0] : db->find_table(attr.relation_name);
      // select id.*
      if (0 == strcmp(attr.attribute_name, "*")) {
        const TableMeta &table_meta = table->table_meta();
        const int field_num = table_meta.field_num();
        for (int index_2 = table_meta.sys_field_num(); index_2 < field_num; index_2++) {
          oper->add_projection(table, table_meta.field(index_2), attr.aggType);
        }
      } else {
        // select id.id
        oper->add_projection(table, table->table_meta().field(attr.attribute_name), attr.aggType);
      }
    }
  }
  return SUCCESS;
}

RC ExecuteStage::do_select(SQLStageEvent *sql_event)
{
  SelectStmt *select_stmt = (SelectStmt *)(sql_event->stmt());
  SessionEvent *session_event = sql_event->session_event();
  Selects & selects = sql_event->query()->sstr.selection;
  Db *db = session_event->session()->get_current_db();
  std::vector<TableScanOperator *> scan_opers;
  std::vector<Table *> tables;
  for (size_t index = 0; index < selects.relation_num; index++) {
    tables.push_back(db->find_table(selects.relations[index]));
  }
  for (auto table : tables) {
    scan_opers.push_back(new TableScanOperator(table));
  }
  TablesJoinPredOperator join_pred_oper(scan_opers, select_stmt->filter_stmt());
  OrderByOperator order_by_oper(selects.order_num, selects.order_attributes);
  order_by_oper.add_child(&join_pred_oper);
  // if (select_stmt->tables().size() != 1) {
  //   std::vector<TableScanOperator *> scan_opers;
  //   int total_fields = 0;
  //   for (auto table : select_stmt->tables()) {
  //     scan_opers.push_back(new TableScanOperator(table));
  //     total_fields += table->table_meta().field_num() - 1;
  //   }
  //   TablesJoinOperator join_oper(scan_opers, select_stmt->filter_stmt());
  //   ProjectOperator project_oper;
  //   project_oper.add_child(&join_oper);
  //   // ??????????????????????????????????????????????????? SELECT * ???????????????????????????????????????
  //   // ???????????????????????????????????????????????????????????? SELECT *
  //   // ??????????????????????????????????????? SELECT ??????????????????????????????????????????????????????
  //   if (total_fields == (int32_t)select_stmt->query_fields().size()) {
  //     int last = select_stmt->query_fields().size() - 1;
  //     for (int32_t i = (int32_t)select_stmt->query_fields().size() - 2; i >= 0; i--) {
  //       auto &now_field = select_stmt->query_fields()[i];
  //       auto &last_field = select_stmt->query_fields()[i + 1];
  //       if (strcmp(now_field.table_name(), last_field.table_name()) != 0) {
  //         for (int32_t j = i + 1; j <= last; j++) {
  //           auto & field = select_stmt->query_fields()[j];
  //           project_oper.add_projection(field.table(), field.meta());
  //         }
  //         last = i;
  //       }
  //     }
  //     for (int32_t j = 0; j <= last; j++) {
  //       auto & field = select_stmt->query_fields()[j];
  //       project_oper.add_projection(field.table(), field.meta());
  //     }
  //   } else {
  //     for (const Field &field : select_stmt->query_fields()) {
  //       project_oper.add_projection(field.table(), field.meta());
  //     }
  //   }
  //   rc = project_oper.open();
  //   std::stringstream ss;
  //   print_tuple_header_multi_table(ss, project_oper);
  //   while ((rc = project_oper.next()) == RC::SUCCESS) {
  //     Tuple * tuple = project_oper.current_tuple();
  //     if (nullptr == tuple) {
  //       rc = RC::INTERNAL;
  //       LOG_WARN("failed to get current record. rc=%s", strrc(rc));
  //       break;
  //     }

  //     tuple_to_string(ss, *tuple);
  //     ss << std::endl;
  //   }
  //   session_event->set_response(ss.str());
  //   rc = RC::SUCCESS;
  //   return rc;
  // }

  // Operator *scan_oper = try_to_create_index_scan_operator(select_stmt->filter_stmt());
  // if (nullptr == scan_oper) {
  //   scan_oper = new TableScanOperator(select_stmt->tables()[0]);
  // }

  // DEFER([&] () {delete scan_oper;});

  // PredicateOperator pred_oper(select_stmt->filter_stmt());
  // pred_oper.add_child(scan_oper);
  ProjectOperator project_oper;
  project_oper.add_child(&order_by_oper);
  // for (const Field &field : select_stmt->query_fields()) {
  //   project_oper.add_projection(field.table(), field.meta());
  // }
  RC rc = add_projection(db, tables, selects.attr_num, selects.attributes, &project_oper);
  if (rc != SUCCESS) {
    session_event->set_response("FAILURE\n");
    return rc;
  }
  Operator * output_oper = nullptr;
  bool have_agg = false;
  for (size_t index = 0; index < selects.attr_num; index++) {
    if (selects.attributes[index].is_agg) {
      have_agg = true;
    }
  }
  GroupByOperator group_by_oper(selects.group_num, selects.group_attributes);
  group_by_oper.add_child(&project_oper);
  if (have_agg) {
    rc = check_attr_in_group(selects.attr_num, selects.attributes, selects.group_num, selects.group_attributes);
    if (rc != SUCCESS) {
      session_event->set_response("FAILURE\n");
      return rc;
    }
    output_oper = &group_by_oper;
  } else {
    output_oper = &project_oper;
  }
  rc = output_oper->open();
  std::stringstream ss;
  if (select_stmt->tables().size() != 1) {
    print_tuple_header_multi_table(ss,  *output_oper);
  } else {
    print_tuple_header(ss, *output_oper);
  }

  while ((rc = output_oper->next()) == RC::SUCCESS) {
    // get current record
    // write to response
    Tuple * tuple = output_oper->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get current record. rc=%s", strrc(rc));
      break;
    }

    tuple_to_string(ss, *tuple);
    ss << std::endl;
  }

  // if (rc != RC::RECORD_EOF) {
  //   LOG_WARN("something wrong while iterate operator. rc=%s", strrc(rc));
  //   project_oper.close();
  // } else {
  //   rc = project_oper.close();
  // }
  session_event->set_response(ss.str());
  return RC::SUCCESS;
}

RC ExecuteStage::do_help(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  const char *response = "show tables;\n"
                         "desc `table name`;\n"
                         "create table `table name` (`column name` `column type`, ...);\n"
                         "create index `index name` on `table` (`column`);\n"
                         "insert into `table` values(`value1`,`value2`);\n"
                         "update `table` set column=value [where `column`=`value`];\n"
                         "delete from `table` [where `column`=`value`];\n"
                         "select [ * | `columns` ] from `table`;\n";
  session_event->set_response(response);
  return RC::SUCCESS;
}

RC ExecuteStage::do_create_table(SQLStageEvent *sql_event)
{
  const CreateTable &create_table = sql_event->query()->sstr.create_table;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  RC rc = db->create_table(create_table.relation_name,
			create_table.attribute_count, create_table.attributes);
  if (rc == RC::SUCCESS) {
    session_event->set_response("SUCCESS\n");
  } else {
    session_event->set_response("FAILURE\n");
  }
  return rc;
}
RC ExecuteStage::do_create_index(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  const CreateIndex &create_index = sql_event->query()->sstr.create_index;
  Table *table = db->find_table(create_index.relation_name);
  if (nullptr == table) {
    session_event->set_response("FAILURE\n");
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  RC rc = table->create_index(nullptr, create_index.index_name, create_index.attribute_name);
  sql_event->session_event()->set_response(rc == RC::SUCCESS ? "SUCCESS\n" : "FAILURE\n");
  return rc;
}

RC ExecuteStage::do_show_tables(SQLStageEvent *sql_event)
{
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  std::vector<std::string> all_tables;
  db->all_tables(all_tables);
  if (all_tables.empty()) {
    session_event->set_response("No table\n");
  } else {
    std::stringstream ss;
    for (const auto &table : all_tables) {
      ss << table << std::endl;
    }
    session_event->set_response(ss.str().c_str());
  }
  return RC::SUCCESS;
}

RC ExecuteStage::do_desc_table(SQLStageEvent *sql_event)
{
  Query *query = sql_event->query();
  Db *db = sql_event->session_event()->session()->get_current_db();
  const char *table_name = query->sstr.desc_table.relation_name;
  Table *table = db->find_table(table_name);
  std::stringstream ss;
  if (table != nullptr) {
    table->table_meta().desc(ss);
  } else {
    ss << "No such table: " << table_name << std::endl;
  }
  sql_event->session_event()->set_response(ss.str().c_str());
  return RC::SUCCESS;
}

RC ExecuteStage::do_insert(SQLStageEvent *sql_event)
{
  // Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  // if (stmt == nullptr) {
  //   LOG_WARN("cannot find statement");
  //   return RC::GENERIC_ERROR;
  // }

  // InsertStmt *insert_stmt = (InsertStmt *)stmt;
  // Table *table = insert_stmt->table();

  // RC rc = table->insert_record(nullptr, insert_stmt->value_amount(), insert_stmt->values());
  RC rc = RC::SUCCESS;
  Inserts & inserts = sql_event->query()->sstr.insertion;
  const char *table_name = inserts.relation_name;
  Table *table = db->find_table(table_name);

  Record record;
  Record recordlist[inserts.record_num];
  int rollback_idx=0;

  for (int index = 0; index < inserts.record_num; index++) {
    rc= table->insert_record(trx, inserts.records[index].value_num, inserts.records[index].values, record);
    // ?????????????????????operation????????????
    // operations_???trx???private???????????????table->rollback_insert()??????rollback?????????delete_operatoin???
    // delete record??????????????????
    if (rc != RC::SUCCESS) {
      rollback_idx=index;
      break;
    }else{
      recordlist[index] = record;
    };
  }
  if (rc == RC::SUCCESS) {
    // session_event->set_response("SUCCESS\n");
    if (!session->is_trx_multi_operation_mode()) {
      CLogRecord *clog_record = nullptr;
      rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
      if (rc != RC::SUCCESS || clog_record == nullptr) {
        session_event->set_response("FAILURE\n");
        return rc;
      }

      rc = clog_manager->clog_append_record(clog_record);
      if (rc != RC::SUCCESS) {
        session_event->set_response("FAILURE\n");
        return rc;
      } 

      trx->next_current_id();
      session_event->set_response("SUCCESS\n");
    } else {
      session_event->set_response("SUCCESS\n");
    }
  } else {
    session_event->set_response("FAILURE\n");
    //  rollback
    for (int index = 0; index < rollback_idx; index++) {
      table->delete_record(trx,&recordlist[index]);
    }
  }
  return rc;
}

RC ExecuteStage::do_delete(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  if (stmt == nullptr) {
    LOG_WARN("cannot find statement");
    return RC::GENERIC_ERROR;
  }

  DeleteStmt *delete_stmt = (DeleteStmt *)stmt;
  TableScanOperator scan_oper(delete_stmt->table());
  PredicateOperator pred_oper(delete_stmt->filter_stmt());
  pred_oper.add_child(&scan_oper);
  DeleteOperator delete_oper(delete_stmt,trx);
  delete_oper.add_child(&pred_oper);

  RC rc = delete_oper.open();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
    if (!session->is_trx_multi_operation_mode()) {
      CLogRecord *clog_record = nullptr;
      rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
      if (rc != RC::SUCCESS || clog_record == nullptr) {
        session_event->set_response("FAILURE\n");
        return rc;
      }

      rc = clog_manager->clog_append_record(clog_record);
      if (rc != RC::SUCCESS) {
        session_event->set_response("FAILURE\n");
        return rc;
      } 

      trx->next_current_id();
      session_event->set_response("SUCCESS\n");
    }
  }
  return rc;
}

RC ExecuteStage::do_begin(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  session->set_trx_multi_operation_mode(true);

  CLogRecord *clog_record = nullptr;
  rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_BEGIN, trx->get_current_id(), clog_record);
  if (rc != RC::SUCCESS || clog_record == nullptr) {
    session_event->set_response("FAILURE\n");
    return rc;
  }

  rc = clog_manager->clog_append_record(clog_record);
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  return rc;
}

RC ExecuteStage::do_commit(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Session *session = session_event->session();
  Db *db = session->get_current_db();
  Trx *trx = session->current_trx();
  CLogManager *clog_manager = db->get_clog_manager();

  session->set_trx_multi_operation_mode(false);

  CLogRecord *clog_record = nullptr;
  rc = clog_manager->clog_gen_record(CLogType::REDO_MTR_COMMIT, trx->get_current_id(), clog_record);
  if (rc != RC::SUCCESS || clog_record == nullptr) {
    session_event->set_response("FAILURE\n");
    return rc;
  }

  rc = clog_manager->clog_append_record(clog_record);
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  trx->next_current_id();

  return rc;
}

RC ExecuteStage::do_clog_sync(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  Db *db = session_event->session()->get_current_db();
  CLogManager *clog_manager = db->get_clog_manager();

  rc = clog_manager->clog_sync();
  if (rc != RC::SUCCESS) {
    session_event->set_response("FAILURE\n");
  } else {
    session_event->set_response("SUCCESS\n");
  }

  return rc;
}
