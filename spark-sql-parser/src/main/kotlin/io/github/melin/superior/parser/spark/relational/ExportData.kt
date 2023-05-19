package io.github.melin.superior.parser.spark.relational

import io.github.melin.superior.common.PrivilegeType
import io.github.melin.superior.common.SqlType
import io.github.melin.superior.common.relational.AbsTableStatement
import io.github.melin.superior.common.relational.TableId

data class ExportData(
    override val tableId: TableId,
    val path: String,
    var properties: Map<String, String>,
    var partitionVals: LinkedHashMap<String, String>,
    var fileFormat: String? = null,
    var compression: String? = null,
    var maxFileSize: String? = null,
    var overwrite: Boolean = false,
    var single: Boolean = false,
    var inputTables: ArrayList<TableId>
) : AbsTableStatement() {
    override val privilegeType: PrivilegeType = PrivilegeType.READ
    override val sqlType: SqlType = SqlType.DML

    constructor(
        tableId: TableId,
        path: String,
        properties: Map<String, String>,
        partitionVals: LinkedHashMap<String, String>,
        fileFormat: String? = null,
        compression: String? = null,
        maxFileSize: String? = null,
        overwrite: Boolean = false,
        single: Boolean = false,):
            this(tableId, path, properties, partitionVals, fileFormat, compression, maxFileSize, overwrite, single, arrayListOf())
}