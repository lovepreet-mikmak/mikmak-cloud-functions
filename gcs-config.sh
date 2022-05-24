#!/bin/bash
echo """
type: \"service_account\",
project_id: \"$GCS_PROJECT_ID\",
private_key_id: \"$GCS_PRIVATE_KEY_ID\",
private_key: \"$GCS_PRIVATE_KEY\",
client_email: \"$GCS_CLIENT_EMAIL\",
client_id: \"$GCS_CLIENT_ID\",
auth_uri: \"$GCS_AUTH_URI\",
token_uri: \"$GCS_TOKEN_URI\",
auth_provider_x509_cert_url: \"$GCS_AUTH_PROVIDER_X509_CERT_URL\",
client_x509_cert_url: \"$GCS_CLIENT_X509_CERT_URL\",
"""