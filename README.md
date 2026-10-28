
# Requirements

## 1. Goal
- To create a unified, clean, and deduplicated source of truth for customer contact information (profiles) and physical addresses, 
linking all records back to a unique Customer Golden ID.
- The pipeline must produce two core, mastered datasets: `contact` and `contact_address`.


## 2. Requirements
### 1: Data Ingestion
- The pipeline must consume data from the following sources:
    * `user.json`: User behavioral and communication preferences.
    * `delete_request.json`: Data indicating user requests for account deletion (Requires careful handling, see DQ).

### 2: Contact Mastering (`contact` table)
- The pipeline must deduplication contacts on a combination of `email`, `name`, `phone number` and `user id`
- Matching logic must be either: 
    1. Exact match on `email` and/or `phone`
    2. Fuzzy match on `names` and/or `phone` and/or `email`, if exact match fails
- Mastered records must have a primary identifier stating the 'best' or most recently updated chlid record
- All records must be linked with a `Master Id`

### 3: Address Mastering (`address` table)
- All address components (Street Lines, City, State, Zip) must be standardized according to postal service guidelines
- Street details must be recorded as individual components (`street number`, `street name`, `street prefix`, `unit identifier`)
- Addresses must be mastered regardless of the original user and address type
- Addresses must be able to be linked back to a master contact

### 4: Right to be Forgotten
- User records that have been requested to be deletetd must be removed.
- If a user has been removed, all associated address and contact parent records should be recalculated and remastered
- If a contact record loses all child records, then the contact record should be deleted. 
- Right to be forgotten requests should be recorded and categorized depending on what information was provided in the `reason` 
field of the source file. Options must be either `No Reason` or any/multiple of the following `Embarrasment`, `Privacy`, `Lack of Trust`, `Other`

### 5: Data Enrichment
- **Preferences:** Combine all user preferences into a single up to date preference payload under the master record. If all options under
a single preference is `Opt-In`, then the mastered preference should also be `Opt-In`, otherwise it should be `Opt-Out`

### 6: Quality Check
- User addreesses must contain a value for the `address_line_1`, `country`, `state`, `city` and `zip` fields
- Users cannot have multiple addresses with the same `address_type` value; Address types are limited to `Shipping` or `Billing`. 
- Users must submit profile info with at least `email` or `phone` values present

## 3. Use Cases
- The solution developed with the functional requirements above must support the following sample use cases
    *   List of all unique users in State 'CA' who have an active Billing address, are linkedvia Facebook, and whose `phone number` is available
    *   The count of unique users in a household linked through Google, who have not requesteddeletion, and are marked to receive New Product preferences
    *   List of all unique Golden IDs that are dormant for over 90 days but reside in high-growth zip codes and possess a verified emailaddress.
    *   List of unique users who interacted with the brand via paid channels but are missing key identifiers (`phone number` or `billingaddress`) and have not opted out.
    *   Identify all Golden IDs sharing a single corporate physical address that are linked to two or more different individualcontacts, indicating potential B2B accounts.
    *   List of unique households utilizing multiple separate services but whose total combined account value has not been optimizedunder one master profile.
    *   Generate a master list of all Golden IDs missing mandatory legal data points (e.g., `billing state` or `consent flag`) required forregulatory compliance audits.
    *   Identify clusters of unique Golden IDs that share identical physical addresses and highly similar contact details, suggestingpotential identity conflicts or fraud risks.

## 4. File Format

### **`user.json`**
- **Format**: JSON
- **Sync Method**: Delta Updates
- **Description**: Contains the complete state of a user record, including contact information (email, phone) at
any point in time when there is an update or change.
- **File Structure**:
```json
{
    "user_id": "string",
    "email": "string or null",
    "phone": "string or null",
    "preferred_contact_type": "Mobile | Email",
    "preferences": {
        "new_product": "bool",
        "educational": "bool",
        "discounts": "bool",
        "news": "bool",
        "insights": "bool"
    },
    "address": {
        "address_line_1": "string",
        "address_line_2": "string or null",
        "address_type": "Shipping | Billing | General",
        "city": "string",
        "state": "string",
        "zip": "string",
        "country": "string"
    }[], 
    "updated_at": "datetime (ISO 8601 format)"
}
```

### **`delete_request.json`**
- **Format**: JSON
- **Sync Method**: Delta Updates
- **Description**: Contains user requests to be forgotten, along with a reason.
- **File Structure**: 
```json
{
    "user_id":"string",
    "reason:":"string or null",
    "requested_at":"datetime (ISO 8601 format)",
}
```



# Objective
Creating a contact mastering process using contact data from `worksapce.contact_mastering.user_profile`

## Tools
- Apache Spark
- Databricks
- Spark SQL

## Considerations
- Multiple users that share either an email address or phone number will be associated with a UUID. 
- Will need to work with a streaming table that contains both fields, email or phone number
- Email or phone number can be null, but not both
- A record will be identified as primary in comparison to other records if: 
    - it has the most amount of OptIn prefrences, checking against `user_preference` table
    - the most amount of addresses, checking against `user_address` table
    - the most amount of populated values, checking against `user_profile` table
    - in the case that the above criteria are the same, the most recent update timestamp on `user_profile` table

## Schemas

### *mst_contact*
```json
{
    "master_identification_text":"string", 
    "user_profile_system_number":"bigint", //fk with user_profile_system_number on user_profile 
    "primary_contact_identifier":"boolean"
}
```

### *user_profile*
```json
{
    "user_profile_system_number":"bigint", 
    "user_profile_name":"string",
    "electronic_mail_address":"string",
    "telephone_number":"string",
    "user_first_name":"string",
    "user_last_name":"string",
    "source_system_update_timestamp":"datetime"
    "user_profile_delete_indicator":"string",
}
```

### *user_address*
```json
{
    "user_address_system_number":"bigint", 
    "user_profile_system_number":"bigint", //fk with user_profile_system_number on user_profile 
    "user_address_type_code":"string",
    "record_logical_delete_indicator":"boolean"
}
```

### *user_preference*
```json
{
    "user_preference_system_number":"bigint", 
    "user_profile_system_number":"bigint", //fk with user_profile_system_number on user_profile 
    "user_preference_type_code":"string",
    "user_preference_selection_indicator":"boolean"
}
```

