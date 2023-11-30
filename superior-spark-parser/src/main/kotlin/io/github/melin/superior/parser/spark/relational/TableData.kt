package io.github.melin.superior.parser.spark.relational

import io.github.melin.superior.common.AlterActionType
import io.github.melin.superior.common.PrivilegeType
import io.github.melin.superior.common.SqlType
import io.github.melin.superior.common.StatementType
import io.github.melin.superior.common.StatementType.*
import io.github.melin.superior.common.relational.Statement
import io.github.melin.superior.common.relational.TableId
import io.github.melin.superior.common.relational.alter.AlterMaterializedView
import io.github.melin.superior.common.relational.alter.AlterTable
import io.github.melin.superior.common.relational.alter.AlterView
import io.github.melin.superior.common.relational.create.CreateTableAsSelect
import io.github.melin.superior.common.relational.create.CreateView
import io.github.melin.superior.common.relational.dml.*
import io.github.melin.superior.common.relational.drop.DropMaterializedView
import io.github.melin.superior.common.relational.drop.DropTable
import io.github.melin.superior.common.relational.drop.DropView
import io.github.melin.superior.common.relational.table.RepairTable
import io.github.melin.superior.common.relational.table.TruncateTable

class TableData {

    var sqlType: SqlType? = null
    var statementType: StatementType? = null
    var privilegeType: PrivilegeType? = null
    var tableId: TableId? = null
    var inputTables: List<TableId>? = null
    var outputTables: List<TableId>? = null
    var statement: Statement? = null

    fun generateTableData(statement: Statement): TableData {
        when (statement.statementType) {
            CREATE_VIEW -> {
                statement as CreateView
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    null,
                    statement.queryStmt.inputTables,
                    arrayListOf(),
                    statement
                )
            }

            CREATE_TABLE_AS_SELECT -> {
                statement as CreateTableAsSelect
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    null,
                    statement.queryStmt.inputTables,
                    arrayListOf(),
                    statement
                )
            }

            DROP_TABLE -> {
                statement as DropTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            DROP_VIEW -> {
                statement as DropView
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            DROP_MATERIALIZED_VIEW -> {
                statement as DropMaterializedView
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            TRUNCATE_TABLE -> {
                statement as TruncateTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            ALTER_VIEW -> {
                statement as AlterView
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            ALTER_MATERIALIZED_VIEW -> {
                statement as AlterMaterializedView
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            ALTER_TABLE -> {
                statement as AlterTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            REPAIR_TABLE -> {
                statement as RepairTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            SELECT -> {
                statement as QueryStmt
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    null,
                    statement.inputTables,
                    arrayListOf(),
                    statement
                )
            }

            DELETE -> {
                statement as DeleteTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    statement.inputTables,
                    arrayListOf(),
                    statement
                )
            }

            UPDATE -> {
                statement as UpdateTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    statement.inputTables,
                    arrayListOf(),
                    statement
                )
            }

            MERGE -> {
                statement as MergeTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.targetTable,
                    statement.inputTables,
                    arrayListOf(),
                    statement
                )
            }

            INSERT -> {
                statement as InsertTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    statement.queryStmt.inputTables,
                    statement.outputTables,
                    statement
                )
            }

            CACHE -> {
                statement as CacheTable
                var insertTable: List<TableId> = arrayListOf()
                if (statement.queryStmt != null) {
                    insertTable = statement.queryStmt.inputTables
                }
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    insertTable,
                    arrayListOf(),
                    statement
                )
            }

            UNCACHE -> {
                statement as UnCacheTable
                return this.buildTableData(
                    statement.sqlType,
                    statement.statementType,
                    statement.privilegeType,
                    statement.tableId,
                    arrayListOf(),
                    arrayListOf(),
                    statement
                )
            }

            else -> return this.buildTableData(
                statement.sqlType,
                statement.statementType,
                statement.privilegeType,
                null,
                arrayListOf(),
                arrayListOf(),
                statement
            )
        }
    }

    private fun buildTableData(
        sqlType: SqlType,
        statementType: StatementType,
        privilegeType: PrivilegeType,
        tableId: TableId? = null,
        inputTables: List<TableId>,
        outputTables: List<TableId>,
        statement: Statement
    ): TableData {
        this.sqlType = sqlType
        this.statementType = statementType
        this.privilegeType = privilegeType
        this.tableId = tableId
        this.inputTables = inputTables
        this.outputTables = outputTables
        this.statement = statement
        return this
    }
}
