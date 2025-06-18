# OAuth authorization code flow

The OAuth 2.0 Authorization Code Flow is the most common and secure OAuth flow used by web and mobile apps. It's designed for applications that can securely store a client secret and is ideal when the app needs to act on behalf of a user.

## ✅ Overview: Authorization Code Flow

### 🧭 Use Case

1. Web apps (with backend)
2. Mobile/desktop apps (with secure storage)
3. Need access to user resources (e.g. GitHub profile, Google Drive)

## 🔁 Flow Steps

### 1. User is Redirected to Authorization Server

```http
GET https://auth-server.com/authorize?
  response_type=code
  &client_id=YOUR_CLIENT_ID
  &redirect_uri=https://yourapp.com/callback
  &scope=read:user profile
  &state=secureRandom123
```

User logs in and approves the app.

### 2. User Is Redirected Back with Code

```http
GET https://yourapp.com/callback?code=AUTH_CODE&state=secureRandom123
```

Your app receives the code (temporary, short-lived).

### 3. Backend Exchanges Code for Access Token

```http
POST https://auth-server.com/token
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code
&code=AUTH_CODE
&redirect_uri=https://yourapp.com/callback
&client_id=YOUR_CLIENT_ID
&client_secret=YOUR_CLIENT_SECRET
```

Response

```json
{
  "access_token": "abc123...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "def456...",
  "scope": "read:user profile"
}
```

## 4. App Uses Access Token to Call API

```http
GET https://api.example.com/user
```

Authorization: Bearer abc123...

## 🔐 Refreshing the Access Token

Access tokens usually expire in 1 hour.

If the provider supports refresh tokens:

```http
POST /token
grant_type=refresh_token
&refresh_token=def456...
&client_id=...
&client_secret=...
```
