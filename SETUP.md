# Setup Guide — Databricks Free Edition

How to run this project on Databricks Free Edition while keeping code in GitHub.
You edit here (Kiro) → push to GitHub → pull into Databricks → run on serverless.

## 0. Free Edition facts that shaped this project

- Default catalog is `workspace` (you cannot create new catalogs).
- You *can* create schemas and volumes inside `workspace`.
- Python only (no Scala/R), serverless compute.
- Outbound internet is restricted, so data files are uploaded via the UI into a
  Unity Catalog **volume**, not downloaded in code.

Defaults used everywhere in this repo:
- catalog: `workspace`
- schema: `airline_medallion`
- volume: `landing` → path `/Volumes/workspace/airline_medallion/landing`

## 1. Connect the GitHub repo to Databricks (one time)

Databricks does NOT understand the local `github-personal:` SSH alias — that
alias only exists on this machine. Use HTTPS + a token in Databricks.

1. Create a GitHub Personal Access Token (fine-grained, repo read/write):
   https://github.com/settings/tokens
2. In Databricks: top-right avatar → Settings → Linked accounts (Git integration).
   - Git provider: GitHub
   - Token: paste the PAT
3. In the sidebar: Workspace → your home folder → Create → Git folder.
   - URL: `https://github.com/vinodanumarla93/databricks-airline-medallion-project.git`
   - This clones the repo into your workspace.

## 2. Daily sync workflow

- Edit + commit + push here in Kiro (uses your `github-personal` SSH alias).
- In Databricks Git folder: click the branch name → Pull to get changes.
- If you edit in the Databricks UI: commit + push there, then run `git pull` here.

One repo, synced both ways. GitHub is the source of truth.

## 3. Upload the sample data (one time, or when data changes)

1. Run the top cells of `notebooks/bronze/ingest_landing.py` once — it creates
   the schema and the `landing` volume (or create them via Catalog Explorer).
2. In Databricks: Catalog → workspace → airline_medallion → Volumes → landing.
3. Click Upload, and add the three files from this repo's `datasets/` folder:
   `customers.csv`, `flights.csv`, `bookings.csv`.

## 4. Run the pipeline

Option A — run notebooks manually (simplest for learning):
1. Attach `notebooks/bronze/ingest_landing.py` to serverless, Run all.
2. Then `notebooks/silver/transform_silver.py`, Run all.
3. Then `notebooks/gold/aggregate_gold.py`, Run all.

Option B — run as a Job (Workflow):
- Create a Job with three notebook tasks chained bronze → silver → gold, or
  deploy the bundle (see below). Set task params catalog/schema/volume if you
  changed the defaults.

## 5. Explore results

- Catalog Explorer → workspace → airline_medallion: you'll see
  `bronze_*`, `silver_*`, `quarantine_*`, and `gold_*` tables.
- Open `sql/gold_analytics.sql` in a SQL editor. Set `catalog=workspace` and
  `schema=airline_medallion` (query params) and run.
- The quarantine tables should hold the intentionally-bad sample rows
  (unknown customer, negative seats, missing email).

## 6. (Optional) Asset Bundles from the workspace

Free Edition can author/deploy bundles from within the Git folder:
- Update the `host` in `databricks.yml` to your Free Edition workspace URL.
- From the Git folder, deploy the bundle to create the `medallion_pipeline` job.
- Or use the Databricks CLI locally: `databricks bundle deploy -t dev`.
