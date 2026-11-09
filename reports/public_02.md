# Grade report — public_02_incidents

Fresh run after the controller hardening pass (`runs/controller_public_02_new2` vs
`runs/baseline_public_02`).

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ |
|---|---:|---:|---:|---:|---:|---:|
| ks_cancel | 0.293 | +24.6% | 0.201 | +10.8% | 0.509 | +8.9% |
| nw_kyc | 0.399 | +6.7% | 0.250 | +6.7% | 0.606 | -0.5% |
| nw_renewal | 0.453 | +4.6% | 0.268 | +2.1% | 0.566 | +2.1% |
| ALL | 0.369 | +11.6% | 0.233 | +6.3% | 0.552 | +3.6% |

Ten changes were applied through approval. The controller detected and removed
the degraded Kestrel caller ID, detected the unsafe prompt, spent one suggestion
call and two validation runs, applied the grounded P3 fix, and verified QA-rate
recovery without an engagement guardrail breach. The promoted decaying winner was
rolled back through governance. The run finished with zero ingestion lag and no
open incidents.

Credits used: 13 (one LLM suggestion and two n=500 validations).
