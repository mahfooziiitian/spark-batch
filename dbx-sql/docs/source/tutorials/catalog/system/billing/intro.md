# Introduction

With system tables, your account's billable usage data is centralized and routed to all regions, so you can view your account's global usage from whichever region your workspace is in.

## Tables

Table|Description
---|----
system.billing.usage|


## Important Considerations

1. `Granular Details`: The system.billing.usage table includes metadata about resources used, custom tags, and identity details.
2. `Join with Other Tables`: You can join the billing logs with other system tables for more insights.
3. `Custom Tags`: Use tags to attribute costs accurately to business units or teams.
