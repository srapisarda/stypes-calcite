package uk.ac.bbk.dcs.stypes.calcite

import java.io.PrintWriter

import org.apache.calcite.adapter.enumerable.{EnumerableConvention, EnumerableRules}
import org.apache.calcite.interpreter.Bindables
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan.{ConventionTraitDef, _}
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules
import org.apache.calcite.rel.rules.{FilterJoinRule, FilterProjectTransposeRule, PruneEmptyRules, _}
import org.apache.calcite.rel.{RelCollationTraitDef, RelDistributionTraitDef, RelNode, RelRoot}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, RelBuilder}
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.calcite.schema.{TableA, TableR, TableS, TestTable}

import scala.io.Source

class Report2020CalciteSlide extends FunSpec {
  private val rootSchema: SchemaPlus = CalciteSchema.createRootSchema(true).plus
  private val schema: SchemaPlus = rootSchema.add("CALCITE_TEST", new AbstractSchema())

  private val relDataTypeX =
    (typeFactory: RelDataTypeFactory) => typeFactory
      .builder
      .add("X", SqlTypeName.INTEGER)
      .build()

  private val relDataTypeXY =
    (typeFactory: RelDataTypeFactory) => typeFactory
      .builder
      .add("X", SqlTypeName.INTEGER)
      .add("Y", SqlTypeName.INTEGER)
      .build()

  schema.add("TTLA_ONE", TestTable(getRow("1.ttl-A.csv"), relDataTypeX))
  schema.add("EMPTY_T", TestTable(getRow("1.ttl-S.csv"), relDataTypeXY))
  schema.add("TTLR_ONE", TestTable(getRow("1.ttl-R.csv"), relDataTypeXY))


  val config: FrameworkConfig = Frameworks.newConfigBuilder.defaultSchema(schema).build

  it("should execute the query validation and planning(volcano) using scan") {

    val builder = RelBuilder.create(config)

    val opTree: RelNode = builder
      .scan("TTLA_ONE")
      .scan("TTLR_ONE")
      .join(JoinRelType.INNER, "X")
      .project(builder.field(0), builder.field(1))
      .scan("EMPTY_T")
      .join(JoinRelType.INNER, "X")
      .project(builder.field(0), builder.field(2))
      .build()

    opTree.getCluster.traitSet().replace(EnumerableConvention.INSTANCE);
    val rw = new RelWriterImpl(new PrintWriter(System.out, true))
    println("Query explanation")
    opTree.explain(rw)
    println()

    // HepProgram
    val program = HepProgram.builder
      .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
      .build

    val hepPlanner = new HepPlanner(program)
    hepPlanner.setRoot(opTree)
    println("HepPlanner logical optimisation")
    hepPlanner.findBestExp.explain(rw)

    println()

    // VolcanoPlanner
    val cluster = opTree.getCluster
    val desiredTraits = cluster.traitSet.replace(EnumerableConvention.INSTANCE)
    val planner = cluster.getPlanner.asInstanceOf[VolcanoPlanner]
    val newRoot = planner.changeTraits(opTree, desiredTraits)
    planner.setRoot(newRoot)

    // add rules
    rules.foreach(planner.addRule)

    // add ConverterRule
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE)

    println("VolcanoPlanner cost based optimisation")
    val optimized = planner.findBestExp

    optimized.explain(rw)
  }


  val rules = Set(
    PruneEmptyRules.PROJECT_INSTANCE,
    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
    EnumerableRules.ENUMERABLE_JOIN_RULE,
    EnumerableRules.ENUMERABLE_SORT_RULE,
    EnumerableRules.ENUMERABLE_VALUES_RULE,
    EnumerableRules.ENUMERABLE_PROJECT_RULE,
    EnumerableRules.ENUMERABLE_FILTER_RULE,
    Bindables.BINDABLE_TABLE_SCAN_RULE,
    CoreRules.FILTER_PROJECT_TRANSPOSE,
    CoreRules.PROJECT_MERGE,
    CoreRules.FILTER_MERGE,
    CoreRules.PROJECT_TABLE_SCAN,
    CoreRules.PROJECT_INTERPRETER_TABLE_SCAN,
    CoreRules.JOIN_TO_MULTI_JOIN,
    CoreRules.JOIN_ASSOCIATE,
    CoreRules.JOIN_REDUCE_EXPRESSIONS,
    CoreRules.MULTI_JOIN_OPTIMIZE,
    CoreRules.JOIN_TO_SEMI_JOIN,
    CoreRules.AGGREGATE_JOIN_TRANSPOSE,
    MaterializedViewRules.FILTER_SCAN
  )

  def getRow(fileName: String) = Source.fromFile(s"src/test/resources/benchmark/Lines/data/csv/$fileName").getLines().map(
    line => line.split(",").toArray.asInstanceOf[Array[AnyRef]]
  ).toList
}
