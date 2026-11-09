# Grade report — public_01_dev

Fresh run after the controller hardening pass (`runs/controller_public_01_new2` vs
`runs/baseline_public_01`).

| workflow | conclusion rate | Δ | pickup | Δ | engagement | Δ |
|---|---:|---:|---:|---:|---:|---:|
| ks_cancel | 0.348 | +2.0% | 0.242 | -1.0% | 0.505 | +2.6% |
| nw_kyc | 0.388 | +5.7% | 0.260 | +7.4% | 0.589 | -1.3% |
| nw_renewal | 0.412 | -1.1% | 0.260 | -1.6% | 0.544 | +0.5% |
| ALL | 0.378 | +1.7% | 0.252 | +0.3% | 0.536 | +1.1% |

Six governance-approved changes were applied; no incident was present. The run
finished unattended with zero ingestion lag. The no-winner outcomes for two
engagement experiments are intentional: the registered Holm-corrected evidence
did not support a promotion within the fixed horizon.

Measured controller ingestion was approximately 198,566 events/s on the local
machine, with a 16.6 MB SQLite database after checkpointing.
