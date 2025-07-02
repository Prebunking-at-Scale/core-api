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


## Deploying
Builds automatically happen whenever new code is pushed to the `dev` branch.
Currently, all builds are tagged with `:dev`, however versioning should happen soon.

Once a build has completed (check [Actions](https://github.com/Prebunking-at-Scale/core-api/actions) on GitHub to verify), you can either delete the pods on dev to force it to pull the latest version, or perform the deploy via command line.

Get credentials for the development cluster:
```bash
gcloud container clusters get-credentials dev-cluster --project pas-development-1 --location europe-west4-b
```

Then simply `cd deployment` and run
```
kubectl apply -f deployment.yaml
```