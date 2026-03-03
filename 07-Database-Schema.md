# Database Schema

## Table: EmailDispatchLog

| Column            | Type      |
|-------------------|----------|
| Id                | UUID     |
| UserId            | UUID     |
| PeriodStartUtc    | Timestamp|
| PeriodEndUtc      | Timestamp|
| TemplateKey       | Text     |
| MessageId         | UUID     |
| CorrelationId     | UUID     |
| SentAtUtc         | Timestamp|
| Status            | Text     |

## Index

CREATE UNIQUE INDEX IX_EmailDispatch_Unique
ON EmailDispatchLog(UserId, PeriodStartUtc, PeriodEndUtc, TemplateKey);