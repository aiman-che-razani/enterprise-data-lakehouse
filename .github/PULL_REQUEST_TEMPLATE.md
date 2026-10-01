## Description

<!-- Briefly describe what this PR does and why. Link to any related issues or tickets. -->

Closes #

## Type of Change

<!-- Check all that apply -->

- [ ] 🐛 Bug fix
- [ ] ✨ New feature / new sprint module
- [ ] ♻️ Refactor (no functional change)
- [ ] 📊 Data schema change
- [ ] ⚡ Performance improvement
- [ ] 🧪 Test addition / improvement
- [ ] 📝 Documentation
- [ ] 🔧 Configuration / infrastructure

## Layer(s) Affected

- [ ] Bronze ingestion
- [ ] Silver processing
- [ ] Gold dimensional model
- [ ] Data quality framework
- [ ] Airflow DAGs
- [ ] Kafka streaming
- [ ] FastAPI
- [ ] Monitoring / alerting
- [ ] CI/CD
- [ ] Infrastructure (Terraform)
- [ ] Documentation

## Testing

<!-- Describe how you tested this change -->

- [ ] Unit tests added / updated (`pytest tests/unit/`)
- [ ] Integration tests added / updated (`pytest tests/integration/`)
- [ ] Pipeline test run end-to-end (`make pipeline-run`)
- [ ] `make lint` passes
- [ ] `make typecheck` passes
- [ ] Coverage remains above 90%

## Data Impact

<!-- Fill in if this PR touches data schemas or processing logic -->

| Question | Answer |
|---|---|
| Does this change schema in Bronze / Silver / Gold? | Yes / No |
| Is this backwards compatible? | Yes / No |
| Does this require a backfill? | Yes / No |
| Does this affect existing DQ checks? | Yes / No |

## Checklist

- [ ] PR title follows Conventional Commits format (`feat:`, `fix:`, `chore:`, etc.)
- [ ] No secrets or credentials committed
- [ ] No `.env` file modified (only `.env.example`)
- [ ] ROADMAP.md updated if completing a sprint task
- [ ] Relevant documentation updated

## Screenshots / Output

<!-- If applicable, add Grafana screenshots, pipeline output, or DQ report excerpts -->
