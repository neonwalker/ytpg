# ytpg

ytpg is a Postgres wire-protocol proxy that stores your data on YouTube. Each table is an unlisted video. Each row is a caption cue. Every commit updates a pinned comment. There is no local database. If the process crashes it rebuilds itself from YouTube on the next start.

This is a project for fun. Please don't ban me Google.

## 1. What you need

* Python 3.11 or newer
* A Google account
* A YouTube channel you own (the ID starts with `UC`)
* `uv` or `pip`

## 2. Google Cloud setup

Over at https://console.cloud.google.com/, you need to:

1. Make a project.
2. Enable the **YouTube Data API v3**.
3. Set up the OAuth consent screen as **External**, leave it in Testing mode, and add your own Google account as a test user.
4. Create an OAuth **client ID** of type **Desktop app** and download the JSON.
5. Save it as `client_secrets.json` in the folder where you run `ytpg`.


### About quota

The YouTube API gives you **10,000 units per day** by default. The operations that matter:

| Operation           | Cost |
|---------------------|-----:|
| `videos.list`       |    1 |
| `videos.update`     |   50 |
| `captions.download` |  200 |
| `captions.update`   |  400 |
| `videos.insert`     | 1600 |

Roughly 400 units per write. So expect about 25 writes a day before the API politely declines further service. This is not a database you run a business on.

## 3. Install

```bash
git clone https://github.com/neonwalker/ytpg.git
cd ytpg
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

Or with `uv`:

```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

Drop the `client_secrets.json` from step 2.4 into this same folder, or pass `--client-secrets /path/to/file` to `ytpg init` later.

## 4. Find your channel ID

ytpg wants the `UC...` channel ID, not the `@handle`.

1. Sign in to YouTube with the account that owns the channel.
2. Go to https://www.youtube.com/account_advanced
3. Copy the **Channel ID**. It starts with `UC` and is 24 characters long.

## 5. Initialize

```bash
ytpg init --channel-id UCxxxxxxxxxxxxxxxxxxxxx
```

This pops open a browser for the OAuth dance. Sign in as one of the test users you added earlier. Then ytpg will:

1. Save refresh credentials to `~/.config/ytpg/<channel_id>.json`.
2. Create (or find) the manifest video `ytpg::__manifest__` on your channel. That is an unlisted video whose description holds the table registry as JSON. Yes, really.
3. Write a tiny bootstrap cache so next time it can find the manifest without searching.

If OAuth goes sideways, run it again with `--reauth` to force a fresh login.

## 6. Run the proxy

```bash
ytpg serve --channel-id UCxxxxxxxxxxxxxxxxxxxxx
# ytpg proxy running on localhost:5432 (channel=UC...)
```

Then connect with anything that speaks Postgres:

```bash
psql -h localhost -p 5432 -U ytpg -d UCxxxxxxxxxxxxxxxxxxxxx
```

```sql
CREATE TABLE users (id INT, name TEXT NOT NULL, active BOOL);
CREATE TABLE orders (id INT, user_id INT, total FLOAT);

INSERT INTO users (id, name, active) VALUES (1, 'Alice', true);
INSERT INTO orders (id, user_id, total) VALUES (10, 1, 99.5);

SELECT u.name, o.total
  FROM users u LEFT JOIN orders o ON u.id = o.user_id
  WHERE u.active = true
  ORDER BY o.total DESC NULLS LAST;

UPDATE users SET active = false WHERE id = 1;

\dt
\d users
```

Congrats, you just wrote a row to a video.

## 7. `ytpg status`

```bash
ytpg status --channel-id UCxxxxxxxxxxxxxxxxxxxxx
```

Shows the tables you have and how many recycled videos are sitting around ready for the next `CREATE TABLE` to claim.

## 8. What works

* **DDL**: `CREATE TABLE`, `DROP TABLE` (soft delete, the video gets recycled for later).
* **DML**: `INSERT`, `UPDATE`, `DELETE`. `UPDATE` is really a DELETE plus an INSERT under the hood.
* **SELECT**: full scan, `COUNT(*)`, `LIMIT`, multi-column `ORDER BY` with per-column `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`.
* **JOINs**: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS`. Multi-way chains too. Each joined table is one more `captions.download`.
* **WHERE**: `=`, `!=`, `<`, `>`, `<=`, `>=`, `IN`, `IS [NOT] NULL`, `LIKE`/`ILIKE`, `AND`, `OR`, `NOT`, parens.
* **Types**: `INT`, `FLOAT`, `BOOL`, `TEXT` families, with Postgres-ish implicit coercion. Bad casts raise `22P02`.
* **`NOT NULL`**: enforced at plan time. Raises `23502`.
* **psql meta commands**: `\dt`, `\l`, `\dn`, `\d <table>`.
* **Crash recovery**: the LSN rehydrates from the pinned checkpoint on startup.

## 9. What doesn't work

* Subqueries, transactions, foreign keys, `GROUP BY`/`HAVING`, window functions, CTEs, indexes.
* `JOIN` inside `UPDATE`/`DELETE`.
* `USING` and `NATURAL JOIN`.
* The extended query protocol (Parse/Bind/Execute). Some drivers will be unhappy.
