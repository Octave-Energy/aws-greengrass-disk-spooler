# Source of these proto files

Copied unchanged from the IBEMS monorepo, so the benchmark publishes exactly the payload format used in production:

- repository: `Octave-Energy/octave-ibems`
- path: `ibems_measurement_spec/proto/ibems_measurement/`
- last commit touching that path when copied: `f135ae18dfc80c8d3bc9eed23fbd1413aaa04031` (2026-08-26)

If the schema changes, copy the folder again and update the commit above. The Python stubs are generated at run
time (`python -m spoolbench gen-proto`), so no generated code is committed.
