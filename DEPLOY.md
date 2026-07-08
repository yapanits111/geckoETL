# Deploying gecko for free (fully hosted)

This deploys the whole pipeline on free tiers with **no local machine required** —
the ETL runs on a schedule in the cloud, writes to a hosted Postgres, and a hosted
dashboard reads from the same database.

```
GitHub Actions (cron)  ──run main.py──►  Neon Postgres  ◄──read──  Streamlit Cloud
   free                                   free tier                 free
```

The key fix over a "Postgres-in-local-Docker" setup: the database is **hosted**, so
both the scheduler and the dashboard can actually reach it. That single change is
what makes the dashboard show real data instead of an empty state.

---

## 1. Database — Neon (free)

1. Create a project at <https://neon.tech> → you get a connection string like:
   `postgresql://user:pass@ep-xxx.aws.neon.tech/crypto_etl?sslmode=require`
2. Copy it. This one string is your `DATABASE_URL`.

> Supabase or Aiven free tiers work identically — anything that gives you a
> `DATABASE_URL` with `sslmode=require`. The code uses it verbatim (see
> `Config.get_db_url`), so the SSL requirement is honored automatically.

No manual schema step is needed — `main.py` creates the table, indexes, and the
`crypto_prices` view on first run.

---

## 2. Scheduler — GitHub Actions (free)

The workflow already exists at [`.github/workflows/run-etl.yml`](../.github/workflows/run-etl.yml)
and runs daily at 06:00 UTC (plus manual "Run workflow").

In the repo: **Settings → Secrets and variables → Actions → New repository secret**,
add just:

| Secret | Value |
|--------|-------|
| `DATABASE_URL` | the Neon string from step 1 |
| `COINGECKO_API_KEY` | *(optional)* a free Demo key for better rate limits |

You do **not** need the discrete `DB_*` secrets when `DATABASE_URL` is set — it wins.
Trigger a first run from the **Actions** tab to populate the database.

---

## 3. Dashboard — Streamlit Community Cloud (free)

1. Go to <https://share.streamlit.io> → **New app**.
2. Repository = your `geckoETL` repo, main file = `gecko/dashboard.py`.
3. **Advanced settings → Secrets**, paste:
   ```toml
   DATABASE_URL = "postgresql://user:pass@ep-xxx.aws.neon.tech/crypto_etl?sslmode=require"
   ```
   Streamlit exposes secrets as env vars, so `Config.get_db_url()` picks it up with
   no code change.
4. Deploy → you get a public `https://<you>-geckoetl.streamlit.app` URL.

---

## Security checklist (production hygiene)

- [x] **No hardcoded password** — `DB_PASSWORD` has no code default; the app refuses
      to start without a real credential (`Config.validate()`).
- [x] **Secrets only via env / secret store** — never committed. `.env` is gitignored
      and confirmed untracked; use GitHub/Streamlit secret stores in the cloud.
- [x] **TLS to the database** — `sslmode=require` for managed Postgres.
- [x] **Least privilege container** — the Docker image runs as a non-root user.
- [x] **No SQL injection** — all values are bound parameters, never string-formatted.
- [ ] **Rotate any password** that was previously typed into a shared/committed place.
- [ ] *(Optional)* Give the app a **read-only DB role** for the dashboard and a
      separate write role for the ETL.

---

## Local development (unchanged)

```bash
cp .env.example .env      # then edit; DB_PASSWORD is required
docker-compose up --build -d postgres dashboard
docker-compose run --rm etl python main.py
```
