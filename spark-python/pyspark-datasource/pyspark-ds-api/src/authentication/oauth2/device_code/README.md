# Device code

OAuth 2.0 Device Authorization Grant (commonly called device code flow) is used by input-constrained devices (TVs, CLI tools, etc.) that can’t open a browser easily.

In OAuth 2.0 Device Code Flow, device registration (authorization) is typically done per user session, not just once.

Here's a complete end-to-end example using the Device Code Flow, including both client-side and server-side components.

`✅ Key Point: Device code flow is per-authorization, not per-device`

🔁 You must go through the device flow each time you want a new access token, unless:

You receive a refresh token, and

You store and reuse it to get new access tokens without re-authorizing

## 🔒 OAuth 2.0 Device Code Flow Overview

Device requests a device/user code from the authorization server.

User visits a verification URL on a separate device (e.g., phone) and enters the user code.

Device polls token endpoint to check if user has authorized.

On success, the device receives access token (and optionally refresh token).

## ✅ GitHub Device Code Flow Overview

GitHub supports the Device Authorization Grant for CLI apps, TVs, or devices without browsers.

### Documentation

GitHub OAuth Device Flow

## 🔧 Prerequisites

### Register a GitHub OAuth App

Go to: <https://github.com/settings/developers> → "New OAuth App"

1. App name: My CLI App
2. Homepage URL: <http://localhost>
3. Callback URL: <http://localhost> (ignored for device flow)

Save the:

1. Client ID
2. Client Secret (optional but can be used to increase rate limits)

## 🔁 Step-by-Step Device Flow

### 1. Request Device & User Code

```http
Copy
Edit
POST https://github.com/login/device/code
Content-Type: application/x-www-form-urlencoded

client_id=YOUR_CLIENT_ID&scope=repo
```

Response

```json
{
  "device_code": "abc123...",
  "user_code": "A1B2-C3D4",
  "verification_uri": "https://github.com/login/device",
  "expires_in": 900,
  "interval": 5
}
```

### 2. Ask User to Authorize

Show this message to the user:

```cpp
To authorize, visit https://github.com/login/device and enter the code A1B2-C3D4
```

### 3. Poll for Access Token

```http
POST https://github.com/login/oauth/access_token
Accept: application/json
Content-Type: application/x-www-form-urlencoded

client_id=YOUR_CLIENT_ID&
device_code=abc123...&
grant_type=urn:ietf:params:oauth:grant-type:device_code
```

Poll every interval seconds.

Success Response:

```json
{
  "access_token": "gho_XXXXXX",
  "token_type": "bearer",
  "scope": "repo"
}
```

Errors:

authorization_pending

slow_down

expired_token

access_denied

### 4. Use Access Token

Use the token to access GitHub APIs:

```http
GET https://api.github.com/user
Authorization: Bearer gho_XXXXXX
User-Agent: my-cli-tool
```

## 📌 Provider-Specific Notes

Provider| Refresh Token in Device Flow? |Notes
---|---|---
GitHub |❌ No |Must reauthorize frequently
Microsoft |✅ Yes |Supports refresh tokens
Google |✅ Yes |Refresh token may be returned
Auth0 |✅ Optional (configurable) |Check tenant settings
Okta |✅ Yes |Scoped by policy
