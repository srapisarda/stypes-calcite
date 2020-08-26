package uk.ac.bbk.dcs.stypes.calcite.schema

import com.google.common.collect.ImmutableList
import org.apache.calcite.DataContext
import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.linq4j.{Enumerable, Linq4j}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.{ScannableTable, Schema, Statistic, Statistics}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlCall, SqlNode}
import scala.collection.JavaConverters._

case class TableR(rows: List[Array[AnyRef]] = Nil) extends ScannableTable {

  protected val protoRowType = (typeFactory: RelDataTypeFactory) =>
    typeFactory.builder
      .add("X", SqlTypeName.INTEGER)
      .add("Y", SqlTypeName.INTEGER)
      .build()

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = protoRowType(typeFactory)

  override def getStatistic: Statistic = Statistics.of(rows.size,  ImmutableList.of(), ImmutableList.of());

  override def getJdbcTableType: Schema.TableType = Schema.TableType.TABLE

  override def isRolledUp(column: String): Boolean = false

  override def rolledUpColumnValidInsideAgg(column: String, call: SqlCall, parent: SqlNode, config: CalciteConnectionConfig): Boolean = false

  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = Linq4j.asEnumerable(rows.asJava)
}
