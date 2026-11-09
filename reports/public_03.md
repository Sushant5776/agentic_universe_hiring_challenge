# Load report — public_03_load

Fresh controller run: `runs/controller_public_03_new`, database
`state/public_03_new.sqlite3`.

| measure | result |
|---|---:|
| simulated duration | 5 days / 7,200 minutes |
| wall-clock controller time | 87.43 s |
| ingestion throughput | 154,768 events/s |
| end-to-end event lag | 0 events / 0 minutes |
| SQLite size after run | 215,445,504 bytes |
| finished | yes |

The process stayed within the assignment's ten-minute one-day envelope (the
five-day run completed in under two minutes on this host). Raw calls are not
stored; feature joins and sufficient statistics dominate the database footprint.
