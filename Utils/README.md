# OpenSearch Sender

Environment variables needed:
- GRAPH_REPO
- GRAPH_SERVER
- OPENSEARCH_AUTH
- OPENSEARCH_INDEX
- OPENSEARCH_URL

Workflow:
- Delete temp index
- Run OpenSearchSender
- Delete dev/live
- Block write for temp index
- Clone temp index to dev/live