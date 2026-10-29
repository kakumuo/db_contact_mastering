
# 1. Overview
- A sample contact mastering pipeline that deduplciates contacts based off an email address and phone number
- Create, clean, and deduplicated source of truth for customer contact information (profiles) and physical addresses, 
linking all records back to a unique Customer Golden ID.
- The pipeline must produce two core, mastered datasets: `contact` and `address`.

## 2. Requirements
### 2.1: Data Ingestion
- The pipeline must consume data from the following sources:
    * `user.json`: User behavioral and communication preferences.
    * `delete_request.json`: Data indicating user requests for account deletion (Requires careful handling, see DQ).

### 2.2: Contact Mastering (`contact` table)
- The pipeline must deduplication contacts on a combination of `email`, `phone number`
- Matching will be done using an exact match on the email or phone number
- Mastered records must have a primary identifier stating the 'best' or most recently updated chlid record
- All records must be linked with a `Master Id`

### 2.3: Address Mastering (`address` table)
- All address components (Street Lines, City, State, Zip) must be standardized according to postal service guidelines
- Street details must be recorded as individual components (`street number`, `street name`, `street prefix`, `unit identifier`)
- Addresses must be mastered regardless of the original user and address type
- Addresses must be able to be linked back to a master contact

### 2.4: Right to be Forgotten
- User records that have been requested to be deletetd must be removed.
- If a user has been removed, all associated address and contact parent records should be recalculated and remastered
- If a contact record loses all child records, then the contact record should be deleted. 
- Right to be forgotten requests should be recorded and categorized depending on what information was provided in the `reason` 
field of the source file. Options must be either `No Reason` or any/multiple of the following `Embarrasment`, `Privacy`, `Lack of Trust`, `Other`

### 2.5: Data Enrichment
- **Preferences:** Combine all user preferences into a single up to date preference payload under the master record. If all options under
a single preference is `Opt-In`, then the mastered preference should also be `Opt-In`, otherwise it should be `Opt-Out`

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