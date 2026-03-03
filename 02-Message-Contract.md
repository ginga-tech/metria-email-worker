# Email Digest Message Contract

## Version: v1

```json
{
  "messageId": "guid",
  "correlationId": "guid",
  "userId": "guid",
  "email": "string",
  "firstName": "string | null",
  "locale": "string",
  "timeZone": "string",
  "periodStartUtc": "datetime",
  "periodEndUtc": "datetime",
  "templateKey": "string",
  "metadata": {}
}
```
Rules

messageId must be unique
correlationId must propagate to logs
userId must not be null
email must be validated
templateKey must be supported

Versioning
Future versions must:

Add fields without breaking contract
Avoid renaming existing fields