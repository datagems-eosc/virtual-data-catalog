# API Usage Examples

The API is available at:
`https://datagems-dev.scayle.es/vdc/api/v1`

> Prerequisite: all steps below require a valid access token. Complete [0) Access Token Setup](#auth-test-bearer-token-required) before running any command.

## Table of Contents

- [1) Upload a dataset to Ontop

## 1) Upload a Dataset to Ontop

```bash
curl -X POST "https://datagems-dev.scayle.es/vdc/api/v1/dataset/8f1e3238-7a21-42e9-89ce-63e74a981af7" \
-H "accept: application/json" \
-H "Authorization: Bearer $TOKEN"
```
