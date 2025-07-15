# core-api
The Core API for Prebunking at Scale.

The API is powered by [litestar](https://docs.litestar.dev/2/index.html). The documentation is great - I'd suggest reading it before getting too stuck into the code.

## Running the local development version
The easiest way to get started is using Docker to run a postgres instance.

You can bring up postgres using:
```bash
docker compose up pas-postgres -d
```
Once started, you'll need to create an `.env` file with the following contents:
```
API_KEYS='["abc123"]'
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_USER=pas
DATABASE_PASSWORD=s3cret
DATABASE_NAME=pas
```
These values should match those in `compose.yaml`.

You'll need `uv` installed to start the actual API. Follow instructions to do [this here](https://docs.astral.sh/uv/getting-started/installation/).

Once `uv` is installed, you can install the various Python packages with:
```bash
uv sync
```

and start the development server with
```bash
 litestar --app core.app:app run --reload
```

## Migrations
Migrations files should be placed in the `core/migrations` directory, following the naming
format:
    `{version}.{description}.{direction}.sql`
where `version` is an integer defining the order that migrations should be performed,
`description` is a short description of the migration, and `direction` is either `up`
or `down` indicating the migration direction. Examples:
  - `1.create_users_table.up.sql`
  - `1.drop_users_table.down.sql`
  - `2.add_email_to_users.up.sql`
  - `2.remove_email_from_users.down.sql`

It is recommended that migrations are wrapped in a transaction (e.g `BEGIN;` and `COMMIT;` statements), so that if a migration fails, the database state is not left
in an inconsistent state.

Once a migration is ready, update `MIGRATION_TARGET_VERSION` in `core/app.py` to match the version you want the database to be at, and it will automatically apply when next run.

## Deploying
Builds automatically happen whenever new code is pushed to the `dev` branch, and a new release is made.

Once a build has completed (check [Actions](https://github.com/Prebunking-at-Scale/core-api/actions) on GitHub to verify), you'll need to update the image version. To do this,
update the `image:` line in `deployment.yaml` so that the version matches the [release](https://github.com/Prebunking-at-Scale/core-api/releases) you want to deploy e.g.:

```
image: europe-west4-docker.pkg.dev/pas-shared/pas/core-api:v0.5.0-dev.0
```
save the file, and make sure you are authenticated against the development cluster:

```bash
gcloud container clusters get-credentials dev-cluster --project pas-development-1 --location europe-west4-b
```

Then change to the deployment folder, and apply the new change:
```
cd deployment

kubectl apply -f deployment.yaml
```