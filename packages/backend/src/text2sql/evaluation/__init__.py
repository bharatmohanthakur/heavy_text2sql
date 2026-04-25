"""Component 13: Evaluation harness.

Runs the live pipeline against the gold corpus, grades six metrics from
spec section 14.1, emits a JSON + markdown report, and (optionally) returns
a CI-gate verdict.
"""

from text2sql.evaluation.harness import (
    CaseResult,
    EvalReport,
    Metrics,
    grade_case,
    run_evaluation,
)

__all__ = [
    "CaseResult",
    "EvalReport",
    "Metrics",
    "grade_case",
    "run_evaluation",
]
