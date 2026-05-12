# rera_crawlers

## Running Crawlers

Default run:

```bash
python run_crawlers.py
```

This runs all sites with `"enabled": true` in `sites_config.py`.

Run only specific sites:

```bash
python run_crawlers.py --site kerala_rera --site bihar_rera
python run_crawlers.py --site kerala_rera,bihar_rera
```

Explicitly selected sites can be run even if they are marked `"enabled": false`, which is useful for testing sites locally while keeping the default production run limited to stable sites.

Set or remove the per-run item cap:

```bash
python run_crawlers.py --item-limit 10 --site kerala_rera
python run_crawlers.py --no-item-limit --site kerala_rera
```

`--item-limit` overrides `CRAWL_ITEM_LIMIT` for the current run. `--no-item-limit` clears any configured item cap for that run.

Speed up a run by reducing the built-in random throttling delays:

```bash
python run_crawlers.py --delay-scale 0.5
python run_crawlers.py --delay-scale 0 --site kerala_rera
```

`--delay-scale 1.0` keeps current behavior. Lower values reduce per-project sleeps across all crawlers for that run.
