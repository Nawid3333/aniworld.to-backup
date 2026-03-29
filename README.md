# AniWorld.to Anime Scraper & Index Manager

A read-only backup tool that scrapes your watched anime data from [aniworld.to](https://aniworld.to) and maintains a local JSON index. Track your watched episodes, subscriptions, watchlist status, and generate progress reports — all from the command line.

## Features

- **Full catalogue scraping** — Scrape all anime from your account or the entire site index
- **Parallel & sequential modes** — Multi-worker parallel scraping for speed, or sequential mode for reliability
- **Checkpoint & resume** — Interrupt anytime and resume where you left off
- **Change detection** — Granular diff shows new series, newly watched episodes, subscription changes, and title updates with per-category confirmation
- **Index management** — Atomic writes, file locking, rotating backups (3 generations)
- **Report generation** — Detailed analytics: completion stats, distribution charts, subscription/watchlist breakdowns
- **Batch operations** — Add single URLs or batch-import from a file
- **Adaptive rate limiting** — Automatically backs off when the server is overloaded
- **Process management** — Tracks browser worker PIDs for clean shutdown and stale process cleanup
- **Ad & popup blocking** — Built-in CSS/JS ad-blocking + optional uBlock Origin integration

## Requirements

- Python 3.9+
- Firefox browser installed
- [geckodriver](https://github.com/mozilla/geckodriver/releases) in your `PATH`
- An aniworld.to account

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/YOUR_USERNAME/aniworld-backup.git
   cd aniworld-backup
   ```

2. **Create a virtual environment (recommended):**

   ```bash
   python -m venv venv
   # Windows
   venv\Scripts\activate
   # Linux/macOS
   source venv/bin/activate
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set up credentials:**

   ```bash
   cp .env.example .env
   ```

   Edit `.env` and add your aniworld.to email and password:

   ```env
   ANIWORLD_EMAIL=your_email@example.com
   ANIWORLD_PASSWORD=your_password
   ```

## Usage

```bash
python main3.py
```

You will see an interactive menu:

```
============================================================
  ANIWORLD.TO ANIME SCRAPER & INDEX MANAGER
============================================================

Options:
  1. Scrape all anime
  2. Scrape only NEW anime
  3. Single link / batch add
  4. Generate report
  5. Scrape subscribed/watchlist anime
  6. Retry failed scrapes
  7. Pause scraping
  8. Show active workers
  9. Exit
```

### Menu Options

| Option | Description                                                                                           |
| ------ | ----------------------------------------------------------------------------------------------------- |
| **1**  | Scrape every anime on the site. Choose parallel or sequential mode.                                   |
| **2**  | Only scrape anime not already in your local index.                                                    |
| **3**  | Add a single URL or batch-import from `series_urls.txt`.                                              |
| **4**  | Generate a JSON report with completion stats, ongoing/completed/not-started breakdowns, and insights. |
| **5**  | Scrape only your subscribed and/or watchlist anime from your account page.                            |
| **6**  | Retry anime that failed in a previous run.                                                            |
| **7**  | Create a pause file — active workers will stop at the next checkpoint.                                |
| **8**  | Display active browser worker processes and optionally kill them.                                     |
| **9**  | Exit.                                                                                                 |

### Batch Import

Create a `series_urls.txt` file with one URL per line:

```
https://aniworld.to/anime/stream/one-piece
https://aniworld.to/anime/stream/solo-leveling
https://aniworld.to/anime/stream/spy-x-family
# Lines starting with # are ignored
```

Then use option **3** and press Enter to import from the default file.

## Project Structure

```
aniworld-backup/
├── main3.py                          # Entry point & CLI menu
├── requirements.txt                  # Python dependencies
├── series_urls.txt                   # Batch URL import file
├── .env.example                      # Credential template
├── .gitignore
├── Config/
│   ├── Config3.py                    # Configuration loader (credentials, paths, settings)
│   └── selectors_config3.json        # Site-specific CSS selectors & timing values
├── src/
│   ├── Scraper3.py                   # Selenium-based scraper (browser, login, episode parsing)
│   └── index_manager3.py             # Index persistence, change detection, merge logic
├── data/                             # Auto-created — stores index & reports (gitignored)
├── logs/                             # Auto-created — rotating log files (gitignored)
└── addons/                           # Optional uBlock Origin .xpi (gitignored)
```

## Configuration

### Credentials (`.env`)

| Variable            | Description                       |
| ------------------- | --------------------------------- |
| `ANIWORLD_EMAIL`    | Your aniworld.to account email    |
| `ANIWORLD_PASSWORD` | Your aniworld.to account password |

### Settings (`Config/Config3.py`)

| Setting           | Default | Description                                          |
| ----------------- | ------- | ---------------------------------------------------- |
| `HEADLESS`        | `True`  | Run Firefox in headless mode                         |
| `VERBOSE_CHANGES` | `False` | Show full change lists instead of collapsed previews |

### Selectors (`Config/selectors_config3.json`)

All CSS selectors and timing values are externalized to this JSON file. If aniworld.to changes its HTML structure, update the selectors here without touching the code.

Key sections:

- `selectors.login` — Login form fields
- `selectors.series_detail` — Episode table, season navigation, metadata
- `selectors.subscription` — Subscribe/watchlist detection
- `timing` — Delays, timeouts, retry limits, backoff parameters

## Data Storage

Scraped data is stored in `data/series_index.json` as a JSON array. Each series entry contains:

- Title (including German, English, and alternative titles)
- Season and episode data with per-episode watched status
- Subscription and watchlist flags
- Language availability per episode
- Timestamps for when the entry was added and last updated

Backups are automatically rotated (`.bak1`, `.bak2`, `.bak3`).

## How It Works

1. **Login** — Authenticates via Selenium using JavaScript form injection to bypass bot detection
2. **Discovery** — Fetches the anime catalogue or account subscription/watchlist pages
3. **Scraping** — For each series, detects available seasons, then scrapes each season's episode table
4. **Change detection** — Compares scraped data against the existing index
5. **Confirmation** — Prompts you to approve each category of change (watched, unwatched, subscribed, etc.)
6. **Merge & save** — Applies approved changes atomically with backup

## Troubleshooting

| Issue                     | Solution                                                                                    |
| ------------------------- | ------------------------------------------------------------------------------------------- |
| `geckodriver` not found   | Download from [releases](https://github.com/mozilla/geckodriver/releases) and add to `PATH` |
| Login fails               | Verify credentials in `.env`. The site may have updated its login form — check selectors.   |
| Stale browser processes   | Use option **8** to view and kill active workers                                            |
| `Checkpoint found` prompt | A previous run was interrupted. Choose to resume or discard.                                |
| Low disk space warning    | Free up space. The tool requires at least 100 MB free.                                      |

## License

This project is for personal backup purposes only. Use responsibly and in accordance with aniworld.to's terms of service.

## Disclaimer

This tool is a **read-only backup utility**. It does not modify any data on aniworld.to — it only reads your watched/subscription status and stores it locally.
