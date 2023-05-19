package io.github.melin.superior.parser.spark.relational

import io.github.melin.superior.common.PrivilegeType
import io.github.melin.superior.common.SqlType
import io.github.melin.superior.common.relational.AbsTableStatement
import io.github.melin.superior.common.relational.TableId

data class CreateFileView(
    override val tableId: TableId,
    val path: String,
    var properties: Map<String, String>,
    var fileFormat: String? = null,
    val compression: String? = null,
    val sizeLimit: String? = null
) : AbsTableStatement() {
    override val privilegeType: PrivilegeType = PrivilegeType.OTHER
    override val sqlType: SqlType = SqlType.DML
}