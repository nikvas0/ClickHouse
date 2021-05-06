#include <Interpreters/WhereConstraintsOptimizer.h>

#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/AddIndexConstraintsOptimizer.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>
#include <Interpreters/TreeSMTSolver.h>

namespace DB
{

WhereConstraintsOptimizer::WhereConstraintsOptimizer(
    ASTSelectQuery * select_query_,
    const StorageMetadataPtr & metadata_snapshot_,
    const bool optimize_append_index_,
    const bool optimize_use_smt_)
    : select_query(select_query_)
    , metadata_snapshot(metadata_snapshot_)
    , optimize_append_index(optimize_append_index_)
    , optimize_use_smt(optimize_use_smt_)
{
}

namespace
{
    enum class MatchState
    {
        FULL_MATCH, /// a = b
        NOT_MATCH, /// a = not b
        NONE, /// other
    };
}

MatchState match(CNFQuery::AtomicFormula a, CNFQuery::AtomicFormula b)
{
    bool match_means_ok = true ^ a.negative ^ b.negative;

    if (a.ast->getTreeHash() == b.ast->getTreeHash())
    {
        return match_means_ok ? MatchState::FULL_MATCH : MatchState::NOT_MATCH;
    }
    return MatchState::NONE;
}

bool checkIfGroupAlwaysTrueFullMatch(const CNFQuery::OrGroup & group, const ConstraintsDescription & constraints_description)
{
    for (const auto & atom : group)
    {
        const auto constraint_atom_ids = constraints_description.getAtomIds(atom.ast);
        if (constraint_atom_ids)
        {
            for (const auto & constraint_atom : constraints_description.getAtomsById(constraint_atom_ids.value()))
            {
                const auto match_result = match(constraint_atom, atom);
                if (match_result == MatchState::FULL_MATCH)
                    return true;
            }
        }
    }
    return false;
}

bool checkIfGroupAlwaysTrueSMT(const CNFQuery::OrGroup & group, TreeSMTSolver & solver)
{
    return solver.alwaysTrue(
        TreeCNFConverter::fromCNF(
            CNFQuery(CNFQuery::AndGroup({group}))));
}

ComparisonGraph::CompareResult getExpectedCompare(const CNFQuery::AtomicFormula & atom)
{
    const auto * func = atom.ast->as<ASTFunction>();
    if (func)
    {
        auto expected = ComparisonGraph::getCompareResult(func->name);
        if (atom.negative)
            expected = ComparisonGraph::inverseCompareResult(expected);
        return expected;
    }
    return ComparisonGraph::CompareResult::UNKNOWN;
}


bool checkIfGroupAlwaysTrueGraph(const CNFQuery::OrGroup & group, const ComparisonGraph & graph)
{
    for (const auto & atom : group)
    {
        const auto * func = atom.ast->as<ASTFunction>();
        if (func && func->arguments->children.size() == 2)
        {
            const auto expected = getExpectedCompare(atom);
            return graph.isAlwaysCompare(expected, func->arguments->children[0], func->arguments->children[1]);
        }
    }
    return false;
}


bool checkIfAtomAlwaysFalseFullMatch(const CNFQuery::AtomicFormula & atom, const ConstraintsDescription & constraints_description)
{
    const auto constraint_atom_ids = constraints_description.getAtomIds(atom.ast);
    if (constraint_atom_ids)
    {
        for (const auto & constraint_atom : constraints_description.getAtomsById(constraint_atom_ids.value()))
        {
            const auto match_result = match(constraint_atom, atom);
            if (match_result == MatchState::NOT_MATCH)
                return true;
        }
    }

    return false;
}

bool checkIfAtomAlwaysFalseFullMatch(CNFQuery::AtomicFormula atom,  TreeSMTSolver & solver)
{
    atom.ast = atom.ast->clone();
    pushNotIn(atom);
    return solver.alwaysFalse(atom.ast);
}

bool checkIfAtomAlwaysFalseGraph(const CNFQuery::AtomicFormula & atom, const ComparisonGraph & graph)
{
    const auto * func = atom.ast->as<ASTFunction>();
    if (func && func->arguments->children.size() == 2)
    {
        /// TODO: special support for !=
        const auto expected = getExpectedCompare(atom);
        return !graph.isPossibleCompare(expected, func->arguments->children[0], func->arguments->children[1]);
    }

    return false;
}

void replaceToConstants(ASTPtr & term, const ComparisonGraph & graph)
{
    const auto equal_constant = graph.getEqualConst(term);
    if (equal_constant)
    {
        term = (*equal_constant)->clone();
    }
    else
    {
        for (auto & child : term->children)
            replaceToConstants(child, graph);
    }
}

CNFQuery::AtomicFormula replaceTermsToConstants(const CNFQuery::AtomicFormula & atom, const ComparisonGraph & graph)
{
    CNFQuery::AtomicFormula result;
    result.negative = atom.negative;
    result.ast = atom.ast->clone();

    replaceToConstants(result.ast, graph);

    return result;
}

void WhereConstraintsOptimizer::perform()
{
    if (select_query->where() && metadata_snapshot)
    {
        //const auto & constraint_data = metadata_snapshot->getConstraints().getConstraintData();
        TreeSMTSolver smt(TreeSMTSolver::STRICTNESS::POLYNOMIAL, metadata_snapshot->getColumns().getAll());
        for (const auto & constraint_description : metadata_snapshot->getConstraints().getConstraints())
        {
            const auto * constraint = constraint_description->as<ASTConstraintDeclaration>();
            if (!constraint)
                throw Exception("Bad constraint", ErrorCodes::LOGICAL_ERROR);
            smt.addConstraint(constraint->expr->clone());
        }

        const auto & compare_graph = metadata_snapshot->getConstraints().getGraph();
        Poco::Logger::get("BEFORE CNF ").information(select_query->where()->dumpTree());
        auto cnf = TreeCNFConverter::toCNF(select_query->where());
        Poco::Logger::get("BEFORE OPT").information(cnf.dump());
        cnf.pullNotOutFunctions()
            .filterAlwaysTrueGroups([&smt, this](const auto & group)
            {
                /// remove always true groups from CNF
                return !optimize_use_smt || !checkIfGroupAlwaysTrueSMT(group, smt);
            })
            .filterAlwaysFalseAtoms([&smt, this](const auto & atom)
            {
                /// remove always false atoms from CNF
                return !optimize_use_smt || !checkIfAtomAlwaysFalseFullMatch(atom, smt);
            })
            .filterAlwaysTrueGroups([&compare_graph, this](const auto & group)
            {
                /// remove always true groups from CNF
                return optimize_use_smt || (!checkIfGroupAlwaysTrueFullMatch(group, metadata_snapshot->getConstraints()) && !checkIfGroupAlwaysTrueGraph(group, compare_graph));
            })
            .filterAlwaysFalseAtoms([&compare_graph, this](const auto & atom)
            {
                /// remove always false atoms from CNF
                return optimize_use_smt || (!checkIfAtomAlwaysFalseFullMatch(atom, metadata_snapshot->getConstraints()) && !checkIfAtomAlwaysFalseGraph(atom, compare_graph));
            })
            .transformAtoms([&compare_graph](const auto & atom)
            {
                return replaceTermsToConstants(atom, compare_graph);
            })
            .reduce()
            .pushNotInFuntions();

        if (optimize_append_index)
            AddIndexConstraintsOptimizer(metadata_snapshot).perform(cnf);

        Poco::Logger::get("AFTER OPT").information(cnf.dump());
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, TreeCNFConverter::fromCNF(cnf));
    }
}

}
