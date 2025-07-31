# Authentication Methods
core-api supports two authentication methods:
 - API Token based auth by setting the `X-API-TOKEN` header
 - JWT based auth using `Authorization` and a bearer token

## API Token Auth
API Tokens provide "super admin" level access through a dedicated `api@pas` user.
API Tokens are provided by setting the `X-API-TOKEN` header with each request.
API tokens are not directly associated with an organisation, however requests to endpoints
that are organisation specific can specify the organisation by setting the
`organisation_id` query parameter.

When using an API Token, requests are intercepted at the middleware level and a JWT is
generated for the `api@pas` user, and if an `organisation_id` is specified, then the
JWT is valid for the organisation. This is handled transparently and does not require
and input from clients other than specifying an `organisation_id` when necessary.

## JWT Auth
JWT Auth provides secure authentication for users after login by providing the token
using the `Authorization` header and `Bearer` schema.

JWTs provide a subject (`sub` - a UUID for the logged in user) and organisation ID (
`organisation_id` - the UUID of the organisation the user is logged into).

Claims made in JWTs are validated with every request, which allows us to configure a
long expiry time of 30 days by default.

### Special JWTs
JWTs are used in several ways other than logins:
 - Organisation invites are issued as a JWT which must by provided as query parameter
   when accepting an invite
 - Password reset tokens which must be provided using the `Authorization` header
   when setting a new password

## Creating an organisation
Creating an organisation can only be performed by super admins and API access using the
`POST /api/auth/organisation` endpoint.

## Inviting a user to an organisation
Organisation admins (and super admins) can invite users to an organisation. A user
can be a member of more than one organisation.

The `/api/organisation/invite` endpoint us used to invite a user via email, and an
optional flag can be set as part of the invite giving the invited user admin permissions.
This is to allow organisations to be created (as above) and an initial admin user to be
invited, who can then invite other users.

## Accepting an invite
Invites are sent via email to the specified email address, and include a link with
a special JWT as a query parameter called `invite_token`. This JWT can be used to accept
the invite and has a TTL of 7 days.

In response to a valid invite token, then API will respond with login options for the
organisation. This will contain:
 - `user` details of the user
 - `organisations` organisation details, including `token` *a JWT token to use for signing into the organisation*
 - `first_time_setup` a boolean value used to determine if the user needs to set an initial password (and customise their profile)

```json
{
  "data": {
    "user": {
      "id": "2ab39c8e-9223-40d7-a68d-ffd94ecca8d3",
      "email": "james.mcminn@fullfact.org",
      "display_name": "james.mcminn"
    },
    "organisations": {
      "526fc00e-a26e-46fd-98dd-95e2f749d61e": {
        "organisation": {
          "id": "526fc00e-a26e-46fd-98dd-95e2f749d61e",
          "display_name": "Full Fact",
          "country_codes": [
            "GBR"
          ],
          "language": "en",
          "short_name": "fullfact"
        },
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTY1NDYxOTQsInN1YiI6IjJhYjM5YzhlLTkyMjMtNDBkNy1hNjhkLWZmZDk0ZWNjYThkMyIsImlhdCI6MTc1Mzk1NDE5NCwiZXh0cmFzIjp7fSwiaXNfYXBpX3VzZXIiOmZhbHNlLCJpc19wYXNzd29yZF9yZXNldCI6ZmFsc2UsIm9yZ2FuaXNhdGlvbl9pZCI6IjUyNmZjMDBlLWEyNmUtNDZmZC05OGRkLTk1ZTJmNzQ5ZDYxZSJ9.lcPHEEY_0ItqpmNLxgjQUgBjLPuWqWQrmL1MrnsQ_e4",
        "is_organisation_admin": false
      }
    },
    "first_time_setup": true
  }
}
```

The `token` value is a valid JWT identical that can then be used as though the user had
performed a login, and is valid for 30 days. If `first_time_setup` is True, then the user
should be given the ability to set a password using `PATCH /api/auth/user/password`
and to update their profile (such as display name) using `PATCH /api/auth/user`.

## Logging In
Logging in is a 2 set process:
1) Providing an email address and password to be validated
2) Selecting an organisation (however this can happen automatically if user is only a member of one organisation)

Once a valid email address and password have been submitted, the login endpiont returns
a response similar to an invite accept response (as above), however will include a list
off all organisations the user is a member of, and a token that can be used for each.

If only one organisation is returned, selecting which organisation to log into can happen
automatically, however if multiple organisations are returned, then the user should be
offer a choice of organisations and the corresponding organisation-specific token should
be used for all future requests.

