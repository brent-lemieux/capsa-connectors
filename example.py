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