# Eval report — 2026-04-25T09:14:47.524764+00:00

- cases: **3**
- schema linking recall: **0%**
- join path exactness:   **67%**
- SQL validity:          **100%**
- execution accuracy:    **0%**
- descriptor leakage:    **0%**
- latency p50/p95/p99:   **4897 / 5185 / 5185 ms**

## Stage p50 (ms)
| stage | p50 |
|---|---|
| `total_ms` | 4897 |
| `context+llm` | 1908 |
| `viz+desc` | 1266 |
| `few_shots` | 770 |
| `table_retrieve` | 533 |
| `validate+repair` | 32 |
| `execute` | 13 |
| `steiner` | 2 |
| `domain_route` | 0 |
| `entity_resolve` | 0 |

## Failures
- 'Hispanic students in the district'                                              — exec mismatch
- 'List all assessment results for a student'                                      — exec mismatch
- 'Average daily attendance for each school in a school year'                      — exec mismatch