# Client credential flow

The Client Credentials Flow is one of the most common OAuth 2.0 flows used for machine-to-machine (M2M) authentication—where no user is involved. It allows an application (the client) to authenticate itself directly with the authorization server to access a resource.

## 🔁 OAuth 2.0 Client Credentials Flow – Overview

Use case:

1. Backend services
2. Microservices
3. Scheduled jobs
4. Scripts
5. Services accessing APIs without user context

## 🔐 How It Works

### 1. Client Sends Token Request

The client application sends a POST request to the token endpoint of the authorization server with:

```text
grant_type = client_credentials
client_id
client_secret
Optional: scope
```

```http
POST /oauth/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&
client_id=CLIENT_ID&
client_secret=CLIENT_SECRET&
scope=read:data
```

### 2. Authorization Server Responds

If the credentials are valid, the server responds with an access token (usually a JWT):

```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOi...",
  "token_type": "Bearer",
  "expires_in": 3600
}
```

### 3. Client Uses Access Token

The client includes the token in the Authorization header when calling protected resources:

```http
GET /api/resource
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOi...
```

## ✅ Pros

1. Simple and secure for trusted services
2. No user context required
3. Ideal for automation

## ⚠️ Cons

1. Not suitable for user login
2. You must store client_secret securely

## Providers Supporting Client Credentials

1. Auth0
2. Azure AD
3. Okta
4. Google Cloud IAM (via service accounts)
5. Salesforce
6. Keycloak
7. GitHub Apps
