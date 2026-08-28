# OAuth2 Password Flow (Resource Owner Password Credentials Flow)

The `OAuth 2.0 Password Flow`, officially called the `Resource Owner Password Credentials Grant (ROPC)`, is one of the OAuth 2.0 flows where the application directly handles the user's username and password to obtain an access token.

This flow is not recommended for most use cases today due to its security implications, but it's still used in legacy systems or trusted environments (e.g., first-party apps).

## Objectives

1. ✅ Token generation with username/password
2. ✅ JWT creation and validation
3. ✅ Protected endpoints
4. ✅ Test clients using curl and Python

## 🔧 How It Works

1. The resource owner (user) provides their username and password to the client (application).
2. The client sends these credentials to the authorization server (along with the client ID and secret).
3. The server authenticates the credentials and returns an access token (and optionally a refresh token).
4. The client uses the access token to call the resource server.

## 📋 Token Request Example

HTTP POST to the token endpoint:

```makefile
POST /oauth/token HTTP/1.1
Host: authorization-server.com
Content-Type: application/x-www-form-urlencoded

grant_type=password&
username=johndoe&
password=A3ddj3w&
client_id=client123&
client_secret=secret456
```

## Response

```json
{
  "access_token": "abc123",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "def456"
}
```

## ✅ When to Use

Trusted first-party clients (e.g., mobile apps or internal tools).

When legacy systems or resource owner credentials are unavoidable.

When you control both the client and authorization server.

## ❌ When Not to Use

1. In third-party or public clients where user credentials may be intercepted.
2. When interactive user consent and authorization is needed (e.g., social login).
3. If you're aiming for modern, secure practices—consider Authorization Code Flow with PKCE instead.

## 🔐 Security Concerns

The client has full access to user credentials—breaks OAuth's core principle of never sharing passwords with clients.

No user consent screen.

Harder to rotate credentials securely.

## 🏁 Summary

Aspect |Description
----|-----
Grant Type |password
User Interaction| Client collects username/password
Tokens Returned |Access token (and optionally refresh token)
Security |Lower – only for trusted clients
Modern Alternative |Authorization Code Flow with PKCE
