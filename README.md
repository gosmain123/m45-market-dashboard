# M45 Morning Dashboard — Production Website

This is the cloud/static version. The final user should only need to open a URL.

## Recommended free architecture

1. Create a **private GitHub repository** and upload this project.
2. In GitHub, go to **Actions** and manually run `Refresh market dashboard` once.
3. Create a **Cloudflare Pages** project connected to that GitHub repository.
4. Set:
   - Build command: leave blank
   - Build output directory: `site`
5. Cloudflare gives you a `*.pages.dev` URL.
6. Optional: add **Cloudflare Access** and allow only your email.

After that:
- GitHub Actions refreshes the data 3 times each weekday.
- Cloudflare redeploys automatically when `dashboard.json` changes.
- You only open the website URL.
- No local Python or PowerShell is needed.

## Important

The price layer is a best-effort public feed. There is no single official, truly unlimited, free real-time API for the entire cross-asset universe.

The code intentionally prefers blank data to suspicious data.

## Local test

If you want to test before Cloudflare:

```bash
pip install -r requirements.txt
python scripts/build_dashboard.py
python -m http.server 8080 -d site
```

Then open `http://127.0.0.1:8080`.

## Calculation QA

```bash
pytest -q
```

The tests specifically prevent the previous bug where a long-range chart's `chartPreviousClose` was mistakenly interpreted as yesterday's close.
