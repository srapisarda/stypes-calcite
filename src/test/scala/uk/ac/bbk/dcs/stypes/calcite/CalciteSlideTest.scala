package uk.ac.bbk.dcs.stypes.calcite

import java.io.PrintWriter
import java.sql.DriverManager
import java.util
import java.util.Properties

import com.google.common.collect.ImmutableList
import org.apache.calcite.adapter.enumerable.{EnumerableConvention, EnumerableRules}
import org.apache.calcite.adapter.jdbc.{JdbcConvention, JdbcSchema}
import org.apache.calcite.config.{CalciteConnectionConfig, CalciteConnectionConfigImpl}
import org.apache.calcite.interpreter.Bindables
import org.apache.calcite.jdbc.{CalciteConnection, CalciteSchema}
import org.apache.calcite.plan.{ConventionTraitDef, _}
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.{RelCollationTraitDef, RelDistributionTraitDef, RelNode, RelRoot, RelWriter}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeSystem}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.rules.{FilterJoinRule, PruneEmptyRules, ReduceExpressionsRule, _}
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.Schemas
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.`type`.SqlTypeFactoryImpl
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlConformance, SqlConformanceEnum, SqlValidatorUtil}
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlToRelConverter, StandardConvertletTable}
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, Programs, RelBuilder}
import org.apache.calcite.util.Sources
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.calcite.schema.{TableA, TableR, TableS}

import scala.collection.JavaConverters._

class CalciteSlideTest extends FunSpec {
  private val model: String = "src/test/resources/benchmark/Lines/data/model-mysql"
  private val connection = getConnection(model)
  private val calciteConnection = connection.unwrap(classOf[CalciteConnection])
  private val typeFactory = createTypeFactory()

  private def getConnection(model: String) = {
    val info = new Properties
    info.put("model", jsonPath(model))
    DriverManager.getConnection("jdbc:calcite:", info)
  }

  private def createTypeFactory() = {
    calciteConnection.getTypeFactory
  }

  private def createCatalogReader() = {
    val calciteConnectionConfig = new CalciteConnectionConfigImpl(connection.getClientInfo)
    val rootSchema: CalciteSchema = CalciteSchema.from(calciteConnection.getRootSchema)

    new CalciteCatalogReader(rootSchema,
      List(calciteConnection.getSchema).asJava,
      typeFactory,
      calciteConnectionConfig)
  }

  private def createRexBuilder() = {
    new RexBuilder(calciteConnection.getTypeFactory)
  }

  private def jsonPath(model: String) =
    resourcePath(model + ".json")

  private def resourcePath(path: String) =
    Sources.of(new java.io.File(path)).file.getAbsolutePath

  private def getFrameworkConfig() = {
    val builder =
      Frameworks
        .newConfigBuilder
        .defaultSchema(calciteConnection.getRootSchema)
        //        .parserConfig(SqlParser.configBuilder.setCaseSensitive(false).build)
        .parserConfig(SqlParser.Config.DEFAULT)
        .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)

    builder.build()
  }

  val rules = Seq(
    Bindables.BINDABLE_TABLE_SCAN_RULE
  )

  it("should execute the query validation and planning") {
    val sql = " SELECT COUNT(*) as NUM " +
      "FROM TTLA_ONE A  " +
      "INNER JOIN TTLR_ONE B1 ON A.X = B1.X " +
      "INNER JOIN TTLR_ONE B2 ON B2.X = B1.X " +
      "INNER JOIN TTLR_ONE D1 ON D1.X = B2.Y " +
      "INNER JOIN EMPTY_T C1 ON C1.X = B2.Y " +
      "INNER JOIN EMPTY_T C2 ON C1.X = C2.X "


    // Parse the query
    val parserConfig = SqlParser.configBuilder().build()
    val parser = SqlParser.create(sql, parserConfig)
    val sqlNode = parser.parseStmt()

    // Validate query
    val catalogReader = createCatalogReader()
    val validator = SqlValidatorUtil.newValidator(
      SqlStdOperatorTable.instance(), catalogReader, typeFactory)
    val validateSqlNode = validator.validate(sqlNode)

    // convert SqlNode to RelNode
    val rexBuilder = createRexBuilder()


    val program = Programs.ofRules(rules.asJava)

    val hepPlanner = new HepPlanner(
      HepProgram.builder().addRuleCollection(rules.asJava).build())

    //    val volcanoPlanner = new VolcanoPlanner()
    //    volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    //    volcanoPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE)
    //    volcanoPlanner.registerAbstractRelationalRules()
    //
    //    rules.foreach( role => volcanoPlanner.addRule(role) )

    val planner = hepPlanner

    val cluster = RelOptCluster.create(planner, rexBuilder)
    val convertletTable = StandardConvertletTable.INSTANCE

    val configBuilder =
      SqlToRelConverter.configBuilder().withTrimUnusedFields(true)

    //    val plannerImpl: PlannerImpl  = new PlannerImpl(getFrameworkConfig)
    //    val viewExpander:PlannerImpl#ViewExpanderImpl = new  plannerImpl.ViewExpanderImpl()

    val sqlToRelConverter =
      new SqlToRelConverter(null, validator, catalogReader, cluster, convertletTable)
    val root = sqlToRelConverter.convertQuery(validateSqlNode, false, true)

    val expressionName = "mysql"
    val expression = Schemas.subSchemaExpression(calciteConnection.getRootSchema, expressionName, classOf[JdbcSchema])
    val convention = JdbcConvention.of(SqlDialect.DatabaseProduct.MSSQL.getDialect, expression, expressionName)
    val trailSet = planner.emptyTraitSet().replace(convention) // .replace( Convention)
    println("The relational expression string before optimized is:\n{}", RelOptUtil.toString(root.rel))
    val optimised = program.run(planner, root.rel, trailSet, ImmutableList.of(), ImmutableList.of())

    System.out.println("-----------------------------------------------------------");
    System.out.println("The Best relational expression string:");
    System.out.println(RelOptUtil.toString(optimised));
    System.out.println("-----------------------------------------------------------");

  }

  it("should execute the query validation and planning(volcano)") {
    val sql = " SELECT COUNT(*) as NUM " +
      "FROM TTLA_ONE A  " +
      "INNER JOIN TTLR_ONE B1 ON A.X = B1.X " +
      "INNER JOIN TTLR_ONE B2 ON B2.X = B1.X " +
      "INNER JOIN EMPTY_T C1 ON C1.X = B2.Y " +
      "INNER JOIN EMPTY_T C2 ON C1.X = C2.X " +
      "INNER JOIN TTLR_ONE D1 ON D1.Y = C2.X "

    val rootSchema = calciteConnection.getRootSchema
    val frameworkConfig = getFrameworkConfig()

    val planner = new VolcanoPlanner
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE)
    // add rules
    planner.addRule(PruneEmptyRules.PROJECT_INSTANCE)
    // add ConverterRule
    planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE)

    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE)

    rules.foreach(role => planner.addRule(role))

    try {
      val factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
      // sql parser
      val parser = SqlParser.create(sql, SqlParser.Config.DEFAULT)
      val sqlNode = parser.parseStmt()

      println("The SqlNode after parsed is:\n{}", sqlNode)

      val calciteCatalogReader = new CalciteCatalogReader(
        CalciteSchema.from(rootSchema),
        CalciteSchema.from(rootSchema).path(null),
        factory,
        new CalciteConnectionConfigImpl(new Properties()))

      // Validate query
      val catalogReader = createCatalogReader()
      val validator = SqlValidatorUtil.newValidator(
        SqlStdOperatorTable.instance(), catalogReader, typeFactory)
      val validateSqlNode = validator.validate(sqlNode)

      val validated = validator.validate(sqlNode)
      println("The SqlNode after validated is:\n{}", validated)

      val rexBuilder = new RexBuilder(factory)

      val cluster = RelOptCluster.create(planner, rexBuilder)
      // init SqlToRelConverter config
      val config = SqlToRelConverter.configBuilder()
        .withConfig(frameworkConfig.getSqlToRelConverterConfig)
        .withTrimUnusedFields(false)
        //        .withConvertTableAccess(false)
        .build()

      // SqlNode toRelNode
      val sqlToRelConverter = new SqlToRelConverter(new ViewExpanderImpl(),
        validator, calciteCatalogReader, cluster, frameworkConfig.getConvertletTable, config)
      var root = sqlToRelConverter.convertQuery(validated, false, true)

      root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
      val relBuilder = config.getRelBuilderFactory.create(cluster, null)
      root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder))
      var relNode = root.rel
      println("The relational expression string before optimized is:\n{}", RelOptUtil.toString(relNode));

      val desiredTraits = relNode.getCluster.traitSet().replace(EnumerableConvention.INSTANCE);
      relNode = planner.changeTraits(relNode, desiredTraits);

      planner.setRoot(relNode)
      relNode = planner.findBestExp()
      val rw = new RelWriterImpl(new PrintWriter(System.out, true))

      System.out.println("-----------------------------------------------------------");
      System.out.println("The Best relational expression string:");
      System.out.println(RelOptUtil.toString(relNode));
      System.out.println("-----------------------------------------------------------");

    } catch {
      case e: Exception =>
        e.printStackTrace();
    }
  }

  it("should execute the query validation and planning(volcano) using scan") {
    val rootSchema = CalciteSchema.createRootSchema(true).plus
    val schema = rootSchema.add("CALCITE_TEST", new AbstractSchema())
    schema.add("TTLA_ONE", TableA())
    schema.add("EMPTY_T", TableS())
    schema.add("TTLR_ONE", TableR())
    val config = Frameworks.newConfigBuilder.defaultSchema(schema).build
    val builder = RelBuilder.create(config)

    val opTree: RelNode = builder
      .scan("TTLA_ONE").as("a")
      .scan("TTLR_ONE").as("b")
      .join(JoinRelType.INNER, "X")
      .project(builder.field(0), builder.field(1))
      .scan("EMPTY_T").as("c")
      .join(JoinRelType.INNER, "X")
      .project(builder.field(0), builder.field(2))
      .build()

    opTree.getCluster.traitSet().replace(EnumerableConvention.INSTANCE);
    val rw = new RelWriterImpl(new PrintWriter(System.out, true))

    opTree.explain(rw)
    println()
    val program = HepProgram.builder
      .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN).build

    val hepPlanner = new HepPlanner(program)
    hepPlanner.setRoot(opTree)
    hepPlanner.findBestExp.explain(rw)

    println()

    val cluster = opTree.getCluster
    val desiredTraits = cluster.traitSet.replace(EnumerableConvention.INSTANCE)
    val planner = cluster.getPlanner.asInstanceOf[VolcanoPlanner]
    val newRoot = planner.changeTraits(opTree, desiredTraits)
    planner.setRoot(newRoot)

    // add rules
    planner.addRule(PruneEmptyRules.PROJECT_INSTANCE)
    //     add ConverterRule
    planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE)
    planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE)
    val optimized = planner.findBestExp

    optimized.explain(rw)
  }

  private def conformance(config: FrameworkConfig): SqlConformance = {
    val context = config.getContext
    if (context != null) {
      val connectionConfig = context.unwrap(classOf[CalciteConnectionConfig])
      if (connectionConfig != null) return connectionConfig.conformance
    }
    SqlConformanceEnum.DEFAULT
  }


  class ViewExpanderImpl extends RelOptTable.ViewExpander {
    override def expandView(rowType: RelDataType, queryString: String, schemaPath: util.List[String], viewPath: util.List[String]): RelRoot = null
  }


}
