# TN Government PDF Downloader

A Python-based scraper that automatically downloads and organises PDF documents from the [Tamil Nadu Government Portal](https://www.tn.gov.in).

## 📋 Categories Covered

| Category | Source URL |
|---|---|
| Government Orders | `/godept_list.php` |
| Policy Notes / Performance Budget / Citizen Charter | `/document_dept_list.php` |
| Rules and Regulations | `/rules.php` |
| Circulars and Notifications | `/circular_notifications_dept_list.php` |
| Acts and Ordinances | `/act_ordinances.php` |

## 🚀 Features

- **Incremental downloads** — scans existing local files and skips duplicates
- **Deduplication** — recognises files even if they were previously renamed with year prefixes
- **Department-wise organisation** — files are saved into department-specific subfolders
- **Polite scraping** — configurable delay between requests
- **Detailed logging** — console + file log (`tn_unified_downloader.log`)
- **Playwright + requests** — uses headless Chromium for navigation, `requests` for fast file downloads

## 🛠️ Setup

### Prerequisites

```bash
pip install playwright requests
playwright install chromium
```

### Configuration

Edit the constants at the top of `TamilNadu.py`:

```python
MASTER_LOCAL_DIR = r"C:\Users\<your-username>\Downloads\TN GOs"  # Local folder
DELAY         = 1.5    # Seconds between downloads
DL_TIMEOUT    = 60     # HTTP timeout per file (seconds)
PAGE_TIMEOUT  = 40_000 # Playwright page-load timeout (ms)
```

### Run

```bash
python TamilNadu.py
```

## 📁 Folder Structure

```
TN GOs/
├── Government order/
│   └── <Department>/
│       └── <Year>/
├── Policy notes  Performance Budget  Citizen Charter (1)/
│   └── <Department>/
│       └── <Year>/
├── Rules and Regulations/
│   └── <Department>/
├── circulars and notifications/
│   └── <Department>/
│       └── <Year>/
└── Acts/
    └── Acts/
```

## 📝 Log File

All activity is written to `tn_unified_downloader.log` in the working directory.

## ⚠️ Notes

- The script filters out the Tamil Thai Vazhthu (state anthem) PDF that appears sitewide.
- Downloaded PDF folders should **not** be committed to Git (see `.gitignore`).
