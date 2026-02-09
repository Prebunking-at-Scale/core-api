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

The app is deployed to GKE clusters using [Kustomize](https://kustomize.io/) overlays. The deployment manifests live in `deployment/`, with a shared `base/` and environment-specific `overlays/dev` and `overlays/prod`.

Docker images are pushed to Google Artifact Registry at `europe-west4-docker.pkg.dev/pas-shared/pas/core-api`.

### Automatic deploys

Pushing to `main` or `dev` triggers the **Build** workflow, which runs tests, builds a Docker image, tags it, and pushes it to Artifact Registry.

- **`dev` branch**: After a successful build, the image is automatically deployed to the dev cluster.
- **`main` branch**: The image is built and pushed, but **not** automatically deployed to production.

You can check build status on the [Actions](https://github.com/Prebunking-at-Scale/core-api/actions) page.

### Deploy via GitHub Actions (recommended)

The **Deploy** workflow can be triggered manually from the [Actions](https://github.com/Prebunking-at-Scale/core-api/actions/workflows/deploy.yml) page:

1. Click **Run workflow**
2. Optionally provide a tag (e.g. `v1.21.0`). If left empty, the latest production release tag is used.
3. The environment is determined by the tag: tags containing `-dev` deploy to dev, all others deploy to prod.

### Deploy manually without GitHub Actions

Prerequisites:
- `gcloud` CLI installed and authenticated
- `kubectl` installed
- `kustomize` installed (or use `kubectl` which includes it)

#### Using the deploy script

The simplest approach is the provided deploy script:

```bash
./deployment/deploy.sh <dev|prod>
```

This authenticates against the correct GKE cluster and applies the kustomize overlay for the given environment. The image tag deployed will be whatever is currently set in `deployment/overlays/<env>/kustomization.yaml`.