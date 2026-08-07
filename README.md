# openIMIS Backend Kobo Connect module

This module synchronizes KoboToolbox submissions with openIMIS business
objects. It currently supports grievance tickets and monitoring submissions.

## Runtime dependencies

- `openimis-be-core`
- `openimis-be-grievance_social_protection`
- `openimis-be-location`
- `openimis-be-monitoring_evaluation`
- `requests`

The common openIMIS Django dependencies are declared in `setup.py`.

## Integration points

- GraphQL queries and mutations are exposed from `kobo_connect.schema`.
- Scheduled synchronization is exposed as
  `kobo_connect.tasks.run_kobo_sync_job`.
- Kobo mappings and credentials are managed through Django models and admin.

The module must be installed through the backend assembly configuration.
