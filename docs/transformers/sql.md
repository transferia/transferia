# SQL Transformer

- **Purpose**: Performs SQL-like in-memory data transformations based on a provided query in the ClickHouse® SQL dialect.
- **Configuration**:
    - `query`: The SQL query to execute.
    - `tables`: Specifies which tables to include or exclude for this transformation.
- **Example**:
  ```yaml
  - sql:
      query: SELECT * FROM table
      tables:
        includeTables:
          - public.test
        excludeTables: null
    transformerId: ""
  ```


This transformer accepts {{ CH }} SQL dialect and allows you to produce SQL-like in-memory data transformation. The solution is based on [{{ CH }} Local ![external link](../_assets/external-link.svg)](https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/)

The source table inside {{ CH }} Local is named `table`, the {{ CH }} table structure mimics the source table structure.

Since each source change item (row) contains extra metadata, we must match source and target data. Therefore, each row must have a key defined. All these keys should be unique in every batch. Do do this, we call a collapse function.

If we can't match source keys with transformed data, we mark such row as containing an error.

When writing an SQL query, you must preserve original key-columns:

```sql
SELECT
  parseDateTime32BestEffortJSONExtractString(CloudTrailEvent, 'eventTime')) AS eventTime,
  JSONExtractString(CloudTrailEvent, 'http_request.user_agent') AS http_useragent,    
  JSONExtractString(CloudTrailEvent, 'errorMessage') AS error_message,
  JSONExtractString(CloudTrailEvent, 'errorCode') AS error_kind, 
  JSONExtractString(CloudTrailEvent, 'sourceIPAddress') AS network_client_ip,
  JSONExtractString(CloudTrailEvent, 'eventVersion') AS eventVersion,
  JSONExtractString(CloudTrailEvent, 'eventSource') AS eventSource,
  JSONExtractString(CloudTrailEvent, 'eventName') AS eventName,
  JSONExtractString(CloudTrailEvent, 'awsRegion') AS awsRegion,
  JSONExtractString(CloudTrailEvent, 'sourceIPAddress') AS sourceIPAddress,
  JSONExtractString(CloudTrailEvent, 'userAgent') AS userAgent,
  JSONExtractString(CloudTrailEvent, 'requestID') AS requestID,
  JSONExtractString(CloudTrailEvent, 'eventID') AS eventID,
  JSONExtractBool(CloudTrailEvent, 'readOnly') AS readOnly,
  JSONExtractString(CloudTrailEvent, 'eventType') AS eventType,
  JSONExtractBool(CloudTrailEvent, 'managementEvent') AS managementEvent,
  JSONExtractString(CloudTrailEvent, 'recipientAccountId') AS recipientAccountId,
  JSONExtractString(CloudTrailEvent, 'eventCategory') AS eventCategory,
  JSONExtractString(CloudTrailEvent, 'aws_account') AS account,

  JSONExtractString(CloudTrailEvent, 'userIdentity.type') AS userIdentity_type,
  JSONExtractString(CloudTrailEvent, 'userIdentity.principalId') AS userIdentity_principalId,
  JSONExtractString(CloudTrailEvent, 'userIdentity.arn') AS userIdentity_arn,
  JSONExtractString(CloudTrailEvent, 'userIdentity.accountId') AS userIdentity_accountId,
  JSONExtractString(CloudTrailEvent, 'userIdentity.accessKeyId') AS userIdentity_accessKeyId,
  
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.type') AS sessionIssuer_type,
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.principalId') AS sessionIssuer_principalId,
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.arn') AS sessionIssuer_arn,
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.accountId') AS sessionIssuer_accountId,
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.sessionIssuer.userName') AS sessionIssuer_userName,
  
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.webIdFederationData.federatedProvider') AS federatedProvider,
  
  JSONExtractString(CloudTrailEvent, 'userIdentity.sessionContext.attributes.creationDate') AS attributes_creationDate,
  JSONExtractBool(CloudTrailEvent, 'userIdentity.sessionContext.attributes.mfaAuthenticated') AS attributes_mfaAuthenticated,
  
  JSONExtractString(CloudTrailEvent, 'requestParameters.commandId') AS requestParameters_commandId,
  JSONExtractString(CloudTrailEvent, 'requestParameters.instanceId') AS requestParameters_instanceId,
  
  JSONExtractString(CloudTrailEvent, 'tlsDetails.tlsVersion') AS tlsDetails_tlsVersion,
  JSONExtractString(CloudTrailEvent, 'tlsDetails.cipherSuite') AS tlsDetails_cipherSuite,
  JSONExtractString(CloudTrailEvent, 'tlsDetails.clientProvidedHostHeader') AS tlsDetails_clientProvidedHostHeader
FROM table
```

