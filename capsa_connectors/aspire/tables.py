TABLE_CONFIGS = {
    "Properties": {
        "id_column": "PropertyID",
        "child_tables": ["PropertyContacts"],
        "drop_columns": ["PropertyTags", "PropertyTakeoffItems"],
        "to_bool_columns": ["SeparateInvoices", "DragDropGeoLocation"]
	},
    "PropertyContacts": {
		"id_column": "PropertyContactID",
		"child_tables": None,
		"drop_columns": ["EmailInvoiceContact"],
		"to_bool_columns": None,
	},
	"Invoices": {
		"id_column": "InvoiceID",
		"child_tables": ["InvoiceOpportunities"],
	},
    "InvoiceOpportunities": {
		"id_column": "InvoiceOpportunityID",
		"child_tables": ["InvoiceOpportunityServices"],
	},
	"InvoiceOpportunityServices": {
		"id_column": "InvoiceOpportunityServiceID",
		"child_tables": None,
        "drop_columns": ["InvoiceOpportunityServiceItems"],
	},
    "WorkTickets": {
		"id_column": "WorkTicketID",
        "drop_columns": ["WorkTicketRevenues"],
	},
    "Services": {
		"id_column": "ServiceID",
        "drop_columns": ["ServiceTaxOverrides", "ServiceBranches"],
	},
    "Opportunities": {
		"id_column": "OpportunityID",
        "drop_columns": ["ScheduleOfValueGroups", "OpportunityRevisions", "OpportunityBillings"],
	},
    "OpportunityServices": {
        "id_column": "OpportunityServiceID",
        "drop_columns": ["OpportunityServiceRoutes", "OpportunityServiceDefaultPayCodes"],
	}
}