# Deploying Meridian Risk Lab

Two services to deploy: **backend** (Render) and **frontend** (Netlify).
Total cost: ~$7.25/mo. Both platforms have free accounts.

---

## Step 1 — Push to GitHub

```bash
cd meridian-risk-lab
git init
git add .
git commit -m "initial commit"
```

Create a new repo at github.com (click **+** → **New repository**), then:

```bash
git remote add origin https://github.com/YOUR_USERNAME/meridian-risk-lab.git
git push -u origin main
```

---

## Step 2 — Deploy backend on Render

1. Go to **render.com** → sign up → **New → Web Service**
2. Connect your GitHub account and select your `meridian-risk-lab` repo
3. Render will detect the `render.yaml` automatically and pre-fill settings
4. Review the settings (should match the YAML), then click **Deploy**
5. While it deploys, add your API keys under **Environment**:
   - `EIA_API_KEY` → your key from eia.gov/opendata
   - `NREL_API_KEY` → your key from developer.nrel.gov
   - `FRED_API_KEY` → your key from fred.stlouisfed.org
6. Wait ~3 minutes for the first deploy to finish
7. Copy your backend URL — looks like: `https://meridian-backend.onrender.com`
8. Test it: open `https://meridian-backend.onrender.com/api/health` in your browser
   - You should see: `{"status": "ok", ...}`

> **Persistent disk:** The `render.yaml` adds a 1 GB disk at `/app/data` ($0.25/mo).
> Your SQLite database (`meridian.db`) lives there and survives restarts/redeploys.

---

## Step 3 — Configure the frontend

Open `frontend/index.html` and find line ~12:

```javascript
const API_BASE = "REPLACE_WITH_RENDER_URL";
```

Replace it with your actual Render URL:

```javascript
const API_BASE = "https://meridian-backend.onrender.com";
```

Save the file.

---

## Step 4 — Deploy frontend on Netlify

**Option A — Drag and drop (fastest):**
1. Go to **netlify.com** → sign up free
2. On the dashboard, drag the entire `frontend/` folder into the browser window
3. Done — you'll get a URL like `https://amazing-name-123.netlify.app`

**Option B — Connect GitHub (auto-deploys on every push):**
1. Go to **netlify.com** → **Add new site → Import an existing project**
2. Connect GitHub and select your repo
3. Set:
   - Build command: *(leave empty)*
   - Publish directory: `frontend`
4. Click **Deploy site**

---

## Step 5 — Run the data pipeline

Once the backend is live, trigger the first full data sync:

```bash
curl -X POST https://meridian-backend.onrender.com/api/pipeline/run
```

Or click **↻ SYNC DATA** in the sidebar of the running app.

---

## Your live URLs

| Service | URL |
|---------|-----|
| Frontend | `https://YOUR_SITE.netlify.app` |
| Backend API | `https://meridian-backend.onrender.com` |
| API Docs | `https://meridian-backend.onrender.com/docs` |

---

## Cost summary

| Item | Cost |
|------|------|
| Netlify (frontend, static) | Free |
| Render Starter (backend, always-on) | $7.00/mo |
| Render Persistent Disk (1 GB SQLite) | $0.25/mo |
| Custom domain (optional) | ~$1/mo |
| **Total** | **~$7.25/mo** |

---

## Updating the app

Push changes to GitHub — Render and Netlify both redeploy automatically.

```bash
git add .
git commit -m "your change"
git push
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Failed to fetch` in browser console | Backend URL in `index.html` is wrong or backend isn't running |
| CORS error | Add your Netlify URL to `allow_origins` in `backend/api.py` |
| Database resets on redeploy | Disk not attached — check Render dashboard → Disks tab |
| Backend URL shows 502 | Deploy still in progress — wait 2–3 minutes and refresh |
| Free tier sleeps | Upgrade to Starter ($7/mo) or accept 30-second cold starts |
