# Capsa Connectors
Currently, `capsa-connectors` supports Aspire connections to Google BigQuery.

## Disclaimer

This repository is provided as an unofficial, open-source reference implementation for connecting third-party systems. It is not an official product of, endorsed by, sponsored by, or affiliated with Aspire Software, Google, Google Cloud, or any other third-party platform referenced in this repository.

The software is provided “as is” and “as available,” without warranties of any kind, express or implied, including warranties of merchantability, fitness for a particular purpose, accuracy, reliability, availability, security, or non-infringement.

Use of this software is entirely at your own risk. You are responsible for:

* Reviewing and testing the code before using it in any production environment.
* Protecting API keys, secrets, credentials, access tokens, and customer data.
* Configuring appropriate access controls, logging, backups, retention policies, and security safeguards.
* Verifying that data extraction, storage, processing, and transfer comply with your contracts, privacy obligations, and applicable laws.
* Confirming that your use complies with the terms, documentation, rate limits, and policies of Aspire, Google Cloud, and any other applicable third-party service.
* Validating all data for completeness and accuracy before relying on it for reporting, billing, financial, operational, or other business decisions.
* Understanding the effect of options such as full refreshes, table replacement, incremental loading, and schema changes before running the software.

Capsa Intelligence Inc., the repository owner, authors, and contributors are not responsible for data loss, data corruption, unauthorized access, service interruptions, API changes, incorrect results, lost profits, business interruption, or any other direct, indirect, incidental, special, consequential, or exemplary damages arising from the use of, or inability to use, this software.

Third-party APIs and services may change without notice. No commitment is made to maintain compatibility, provide support, correct defects, or update this repository.

Nothing in this repository constitutes legal, compliance, accounting, security, or professional advice.


## Example usage
```python
from capsa_connectors.aspire import (
    AspireTable,
    establish_gbq_connection,
    retrieve_bearer_token
)

YOUR_GOOGLE_PROJECT_ID = '...'
YOUR_GOOGLE_DATASET_ID = '...'

YOUR_ASPIRE_KEY = '...'
YOUR_ASPIRE_SECRET = '...'


def pipeline():
    # Authenticate with Google Cloud.
    establish_gbq_connection(YOUR_GOOGLE_PROJECT_ID)
    # Authenticate with Aspire.
    bearer_token = retrieve_bearer_token(YOUR_ASPIRE_KEY, YOUR_ASPIRE_SECRET)
    # Load Aspire data to GBQ (NOTE: full_refresh should only be True for your first load).
    FULL_REFRESH = True
    properties = AspireTable(
        "aspire_Properties", 
        "ModifiedDate", 
        "PropertyID", 
        full_refresh=FULL_REFRESH, 
        bearer_token=bearer_token, 
        project_id=YOUR_GOOGLE_PROJECT_ID,
        dataset_id=YOUR_GOOGLE_DATASET_ID
    )
    work_tickets = AspireTable(
        "aspire_WorkTickets", 
        "LastModifiedDateTime", 
        "WorkTicketID", 
        full_refresh=FULL_REFRESH, 
        bearer_token=bearer_token, 
        project_id=YOUR_GOOGLE_PROJECT_ID,
        dataset_id=YOUR_GOOGLE_DATASET_ID
    )

# Run pipeline.
pipeline()
```
