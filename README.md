# GSMArena Scraper — Auto-resume on block

Fully automatic scraper that gets a new IP immediately when blocked.

## How it works

1. Scraper runs on GitHub Actions (fresh Azure IP each job)
2. If blocked (20 consecutive 429s) — saves progress, exits with code 2
3. Workflow detects block — triggers a NEW job immediately via API
4. New job = new machine = new IP — continues from saved progress
5. Repeats automatically until all 14k devices done

## One-time setup

### Step 1: Create a Personal Access Token (PAT)
- Go to GitHub → Settings → Developer Settings → Personal Access Tokens → Tokens (classic)
- Click "Generate new token (classic)"
- Name: `gsmarena-scraper`
- Scopes: check `repo` and `workflow`
- Copy the token

### Step 2: Add PAT as repo secret
- Go to your repo → Settings → Secrets and variables → Actions
- Click "New repository secret"
- Name: `PAT_TOKEN`
- Value: paste your token
- Click "Add secret"

### Step 3: Upload your local progress files
Replace the placeholder files with your actual local files:
- `seen_ids.json` — from your Downloads folder (3510 devices already scraped)
- `discovered_devices.json` — from your Downloads folder
- `gsmarena_devices.csv` — from your Downloads folder

### Step 4: Enable and run
- Go to Actions tab → enable workflows
- Click "Run workflow" to start

## Files

| File | Purpose |
|------|---------|
| `seen_ids.json` | Resume point — which devices already scraped |
| `discovered_devices.json` | Full device list — skips rediscovery |
| `gsmarena_devices.csv` | Output CSV |
| `gsmarena_devices.xlsx` | Output Excel |
| `gsmarena_run.log` | Run log |
