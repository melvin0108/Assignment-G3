## Source data contracts

Each YAML file is the contract for one mock CSV input. It describes the Silver contractual type, requiredness, example value, data classification, executable DQ rule IDs, allowed values, and referential constraints.

`mock.config.TABLE_SCHEMAS` is the physical CSV-header source of truth. `pipeline/dq/dq_02_load_dq_rules.py` is the executable DQ-rule registry. Run `python -m unittest tests.test_source_contracts` to verify these contracts still match both.

Raw files land in Bronze as strings; Silver applies the typed contract, validation, masking, and quarantine routing. Gold/AI-output contracts remain separately versioned in `docs/models/gold`.
