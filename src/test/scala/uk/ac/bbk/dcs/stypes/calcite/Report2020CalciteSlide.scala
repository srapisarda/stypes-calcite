package uk.ac.bbk.dcs.stypes.calcite

import java.io.PrintWriter

import org.apache.calcite.adapter.enumerable.{EnumerableConvention, EnumerableRules}
import org.apache.calcite.interpreter.Bindables
import org.apache.calcite.jdbc.{CalciteSchema}
import org.apache.calcite.plan.{ConventionTraitDef, _}
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.rules.{FilterJoinRule, PruneEmptyRules, _}
import org.apache.calcite.rel.{RelCollationTraitDef, RelDistributionTraitDef, RelNode, RelRoot}
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.tools.{Frameworks, RelBuilder}
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.calcite.schema.{TableA, TableR, TableS}

class Report2020CalciteSlide extends FunSpec {

  it("should execute the query validation and planning(volcano) using scan") {
    val rootSchema = CalciteSchema.createRootSchema(true).plus
    val schema = rootSchema.add("CALCITE_TEST", new AbstractSchema())
    schema.add("TTLA_ONE", TableA())
    schema.add("EMPTY_T", TableS())
    schema.add("TTLR_ONE", TableR())
    val config = Frameworks.newConfigBuilder.defaultSchema(schema).build
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
      .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN).build

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
    planner.addRule(PruneEmptyRules.PROJECT_INSTANCE)
    // add ConverterRule
    planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE)
    planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE)
    planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE)
    planner.addRule(CoreRules.JOIN_TO_MULTI_JOIN)
    planner.addRule(CoreRules.JOIN_ASSOCIATE)
    planner.addRule(CoreRules.JOIN_REDUCE_EXPRESSIONS)
    planner.addRule(CoreRules.MULTI_JOIN_OPTIMIZE)
    planner.addRule(CoreRules.JOIN_TO_SEMI_JOIN)
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE)

    println("VolcanoPlanner cost based optimisation")
    val optimized = planner.findBestExp

    optimized.explain(rw)
  }

  val rules = Seq(
    Bindables.BINDABLE_TABLE_SCAN_RULE
  )

}
