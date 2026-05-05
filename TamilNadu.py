

import re
import time
import zipfile
import logging
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout


MASTER_LOCAL_DIR = r"C:\Users\varun\Downloads\TN GOs"   # ← your existing downloaded folder
BASE_URL      = "https://www.tn.gov.in"
DELAY         = 1.5       # seconds between downloads — be polite to server
DL_TIMEOUT    = 60        # seconds — HTTP download timeout per file
PAGE_TIMEOUT  = 40_000    # ms — Playwright page-load timeout

# URLs for each category
CATEGORY_URLS = {
    "Government Orders":                        f"{BASE_URL}/godept_list.php",
    "Policy Notes":                             f"{BASE_URL}/document_dept_list.php?cate_name=YWxs",
    "Rules and Regulations":                    f"{BASE_URL}/rules.php",
    "Circulars and Notifications":              f"{BASE_URL}/circular_notifications_dept_list.php",
    "Acts and Ordinances":                      f"{BASE_URL}/act_ordinances.php",
}

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("tn_unified_downloader.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def sanitize(name: str) -> str:
    """Make a string safe for Windows folder/file names."""
    name = name.strip()
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip(" .")


def make_absolute(href: str) -> str:
    """Convert a relative href into a full URL."""
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return urljoin(BASE_URL + "/", href.lstrip("/"))


def fname_from_url(url: str) -> str:
    """Extract a clean .pdf filename from a URL."""
    raw = urlparse(url).path.split("/")[-1].split("?")[0]
    raw = re.sub(r'[<>:"/\\|?*]', '_', raw)
    if not raw.lower().endswith(".pdf"):
        raw += ".pdf"
    return raw or "document.pdf"


def get_existing_pdfs(root: Path) -> set:
    """
    Return a set of lower-cased PDF filenames already present inside *root*
    (searches all sub-directories recursively).
    Also adds variants stripped of a leading year-prefix (e.g. 2024_file.pdf → file.pdf)
    so renamed files are still recognised as duplicates.
    """
    if not root.exists():
        return set()
    result = set()
    for f in root.rglob("*.pdf"):
        name = f.name.lower()
        result.add(name)
        # strip leading 4-digit year prefix
        stripped = re.sub(r'^\d{4}[-_]', '', name)
        if stripped != name:
            result.add(stripped)
        # also strip trailing year suffix patterns like _2024
        stripped2 = re.sub(r'[-_]\d{4}(\.pdf)$', r'\1', name)
        if stripped2 != name:
            result.add(stripped2)
    return result


def download_pdf(url: str, dest: Path, session: requests.Session) -> bool:
    """Stream-download *url* → *dest*. Returns True on success."""
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        r = session.get(url, timeout=DL_TIMEOUT, stream=True)
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "html" in ct:
            log.warning("      ⚠ Server returned HTML instead of PDF – skipping: %s", url)
            return False
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(16_384):
                fh.write(chunk)
        kb = dest.stat().st_size // 1024
        log.info("      ✓ Saved  %s  (%d KB)", dest.name, kb)
        return True
    except Exception as exc:
        log.error("      ✗ Failed %s → %s", url, exc)
        if dest.exists():
            dest.unlink(missing_ok=True)
        return False


def extract_zip_if_needed(zip_path: str, extract_to: str):
    """Extract a zip file to a folder. Skips if already extracted."""
    zip_p  = Path(zip_path)
    dest_p = Path(extract_to)
    if not zip_p.exists():
        log.warning("ZIP not found: %s — will use/create folder: %s", zip_path, extract_to)
        dest_p.mkdir(parents=True, exist_ok=True)
        return
    if dest_p.exists() and any(dest_p.iterdir()):
        log.info("Folder already extracted — skipping unzip: %s", extract_to)
        return
    log.info("Extracting %s → %s …", zip_path, extract_to)
    dest_p.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_p, "r") as zf:
        zf.extractall(dest_p)
    log.info("Extraction complete.")


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE-SCRAPING HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def collect_departments(page) -> list[dict]:
    """
    Scrape department links from any TN Gov list page.
    Tries multiple selectors to be robust against minor HTML changes.
    Returns list of {name, href}.
    """
    selectors = [
        "ul#dept_list_content li p a",
        "ul#dept_list_content li a",
        "#dept_container a[href*='dep_id']",
        "a[href*='dep_id']",
    ]
    for sel in selectors:
        try:
            items = page.eval_on_selector_all(
                sel,
                """els => els
                    .filter(a => a.getAttribute('href') && a.innerText.trim().length > 0)
                    .map(a => ({
                        name: a.innerText.trim(),
                        href: a.getAttribute('href')
                    }))
                """,
            )
            if items:
                log.info("    Dept selector '%s' → %d depts", sel, len(items))
                return items
        except Exception:
            continue
    return []


def collect_year_links(page, current_url: str) -> list[dict]:
    """
    Collect all year/archive links from a department page.
    Always includes the currently-loaded page as the first entry.
    Returns list of {year, href}.
    """
    entries = [{"year": "_current", "href": current_url}]
    seen    = {current_url}

    selectors = [
        "a[href*='year=']",
        "div.archives a",
        "div.d-flex.flex-wrap a[href*='dep_id']",
        ".archive a",
        "a[href*='dep_id'][href*='year']",
    ]
    for sel in selectors:
        try:
            items = page.eval_on_selector_all(
                sel,
                """els => els
                    .filter(a => a.getAttribute('href') && a.innerText.trim())
                    .map(a => ({
                        year: a.innerText.trim(),
                        href: a.getAttribute('href')
                    }))
                """,
            )
            for it in items:
                abs_href = make_absolute(it["href"])
                yr = it["year"].strip()
                if abs_href not in seen and yr:
                    seen.add(abs_href)
                    entries.append({"year": yr, "href": abs_href})
        except Exception:
            continue
    return entries


def collect_pdf_urls(page) -> list[str]:
    """
    Return a deduplicated list of all PDF URLs on the current page.
    Filters out the state anthem PDF that appears sitewide.
    """
    found = {}
    selectors = [
        "a[href$='.pdf']",
        "a[href*='.pdf']",
        "a[target='_blank'][href*='cms.tn']",
        "table a[href*='.pdf']",
        "tr td a",
        "div.event-info a",
        "div.event-card a",
        "p.event-detail a",
        "div.go-list a",
        "ul li a[href*='.pdf']",
    ]
    for sel in selectors:
        try:
            urls = page.eval_on_selector_all(
                sel,
                "els => els.map(a => a.href).filter(h => h && h.toLowerCase().includes('.pdf'))",
            )
            for u in urls:
                found[u] = True
        except Exception:
            continue

    # Broad final pass
    try:
        all_hrefs = page.eval_on_selector_all(
            "a",
            "els => els.map(a => a.href).filter(h => h && h.toLowerCase().endsWith('.pdf'))",
        )
        for u in all_hrefs:
            found[u] = True
    except Exception:
        pass

    # Filter out the Tamil Thai Vazhthu state anthem link (appears on every page)
    return [u for u in found if "tamilthaivazhthusong" not in u.lower()]


def nav(page, url: str, label: str = "") -> bool:
    """Navigate to *url*. Returns False if timeout."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        page.wait_for_timeout(1_500)
        return True
    except PWTimeout:
        log.warning("  ⚠ Timeout%s: %s", f" ({label})" if label else "", url)
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  CORE SCRAPING ROUTINES
# ══════════════════════════════════════════════════════════════════════════════

def process_dept_year_category(
    page,
    session: requests.Session,
    category_name: str,
    list_url: str,
    local_dir: Path,
    global_stats: dict,
    master_existing: set,
):
    """
    Generic handler for categories that follow:
      Dept list page → click dept → year list → click year → PDFs

    Used by: Government Orders, Policy Notes, Rules, Circulars.
    """
    log.info("")
    log.info("▓" * 70)
    log.info("  CATEGORY: %s", category_name)
    log.info("  URL     : %s", list_url)
    log.info("  LOCAL   : %s", local_dir)
    log.info("▓" * 70)

    local_dir.mkdir(parents=True, exist_ok=True)

    # ── Load dept list ────────────────────────────────────────────────────
    if not nav(page, list_url, "dept list"):
        log.error("  Could not load dept list for %s — skipping.", category_name)
        return

    depts = collect_departments(page)
    if not depts:
        log.error("  No departments found for %s!", category_name)
        # Dump first 3000 chars of HTML to help diagnose
        try:
            log.info("  HTML snippet:\n%s", page.content()[:3000])
        except Exception:
            pass
        return

    log.info("  Departments: %d", len(depts))

    # Use the master existing-PDF index (covers ALL categories, not just this one)
    existing = master_existing
    log.info("  Existing PDFs in master index : %d", len(existing))

    cat_stats = {"downloaded": 0, "skipped": 0, "failed": 0}

    # ── Iterate departments ───────────────────────────────────────────────
    for d_idx, dept in enumerate(depts, 1):
        dept_name   = sanitize(dept["name"])
        dept_href   = make_absolute(dept["href"])
        dept_folder = local_dir / dept_name
        dept_folder.mkdir(parents=True, exist_ok=True)

        log.info("")
        log.info("  ═" * 33)
        log.info("  [%d/%d]  %s", d_idx, len(depts), dept_name)
        log.info("  ═" * 33)

        if not nav(page, dept_href, dept_name):
            continue

        # For Rules, we ONLY want the main page PDFs, not the "What's New" archives
        if category_name == "Rules and Regulations":
            pdf_urls = collect_pdf_urls(page)
            log.info("    ▶ %-18s  PDFs on page: %d", "Main Page", len(pdf_urls))
            for pdf_url in pdf_urls:
                fname = fname_from_url(pdf_url)
                dest  = dept_folder / fname

                if fname.lower() in existing or dest.exists():
                    log.info("      ↷ SKIP   %s", fname)
                    cat_stats["skipped"]      += 1
                    global_stats["skipped"]   += 1
                    continue

                log.info("      ↓ NEW    %s", fname)
                time.sleep(DELAY)
                ok = download_pdf(pdf_url, dest, session)
                if ok:
                    cat_stats["downloaded"]    += 1
                    global_stats["downloaded"] += 1
                    existing.add(fname.lower())
                else:
                    cat_stats["failed"]        += 1
                    global_stats["failed"]     += 1
            continue

        year_entries = collect_year_links(page, dept_href)
        log.info("    Year pages: %d  %s",
                 len(year_entries),
                 [y["year"] for y in year_entries[:8]])

        first = True
        for yr in year_entries:
            yr_label  = sanitize(yr["year"])
            yr_url    = yr["href"]
            yr_folder = dept_folder / yr_label
            yr_folder.mkdir(parents=True, exist_ok=True)

            if not first:
                if not nav(page, yr_url, yr_label):
                    continue
            first = False

            pdf_urls = collect_pdf_urls(page)
            log.info("    ▶ %-18s  PDFs on page: %d", yr_label, len(pdf_urls))

            for pdf_url in pdf_urls:
                fname = fname_from_url(pdf_url)
                dest  = yr_folder / fname

                if fname.lower() in existing or dest.exists():
                    log.info("      ↷ SKIP   %s", fname)
                    cat_stats["skipped"]      += 1
                    global_stats["skipped"]   += 1
                    continue

                log.info("      ↓ NEW    %s", fname)
                time.sleep(DELAY)
                ok = download_pdf(pdf_url, dest, session)
                if ok:
                    cat_stats["downloaded"]    += 1
                    global_stats["downloaded"] += 1
                    existing.add(fname.lower())
                else:
                    cat_stats["failed"]        += 1
                    global_stats["failed"]     += 1

    log.info("")
    log.info("  ── %s Summary ──", category_name)
    log.info("     ✓ Downloaded : %d", cat_stats["downloaded"])
    log.info("     ↷ Skipped    : %d", cat_stats["skipped"])
    log.info("     ✗ Failed     : %d", cat_stats["failed"])


def process_acts(
    page,
    session: requests.Session,
    local_dir: Path,
    global_stats: dict,
    master_existing: set,
):
    """
    Handler for Acts & Ordinances.
    Structure: Year sections → PDFs  (NO department level).
    """
    acts_url = CATEGORY_URLS["Acts and Ordinances"]

    log.info("")
    log.info("▓" * 70)
    log.info("  CATEGORY: Acts and Ordinances")
    log.info("  URL     : %s", acts_url)
    log.info("  LOCAL   : %s", local_dir)
    log.info("▓" * 70)

    local_dir.mkdir(parents=True, exist_ok=True)
    existing = master_existing
    log.info("  Existing PDFs in master index : %d", len(existing))

    cat_stats = {"downloaded": 0, "skipped": 0, "failed": 0}

    if not nav(page, acts_url, "Acts main page"):
        log.error("  Could not load Acts page — skipping.")
        return

    # Just scrape PDFs directly from the main Acts page without creating year subfolders
    pdf_urls = collect_pdf_urls(page)
    log.info("  ▶ %-10s  PDFs: %d", "Acts", len(pdf_urls))

    for pdf_url in pdf_urls:
        fname = fname_from_url(pdf_url)
        dest  = local_dir / fname

        if fname.lower() in existing or dest.exists():
            log.info("    ↷ SKIP   %s", fname)
            cat_stats["skipped"]      += 1
            global_stats["skipped"]   += 1
            continue

        log.info("    ↓ NEW    %s", fname)
        time.sleep(DELAY)
        ok = download_pdf(pdf_url, dest, session)
        if ok:
            cat_stats["downloaded"]    += 1
            global_stats["downloaded"] += 1
            existing.add(fname.lower())
        else:
            cat_stats["failed"]        += 1
            global_stats["failed"]     += 1

    log.info("")
    log.info("  ── Acts & Ordinances Summary ──")
    log.info("     ✓ Downloaded : %d", cat_stats["downloaded"])
    log.info("     ↷ Skipped    : %d", cat_stats["skipped"])
    log.info("     ✗ Failed     : %d", cat_stats["failed"])


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run():
    log.info("=" * 70)
    log.info("  TN GOVERNMENT PORTAL – UNIFIED PDF DOWNLOADER")
    log.info("=" * 70)

    # Resolve the master directory  (your existing downloaded folder)
    master_dir = Path(MASTER_LOCAL_DIR)
    master_dir.mkdir(parents=True, exist_ok=True)

    # ── Map each category to the EXACT existing subfolder name ────────────
    # These names must match what is already on disk inside TN GOs\.
    categories = [
        ("Government Orders",
         CATEGORY_URLS["Government Orders"],
         master_dir / "Government order"),          # existing folder name

        ("Policy Notes / Performance Budget / Citizen Charter",
         CATEGORY_URLS["Policy Notes"],
         master_dir / "Policy notes  Performance Budget  Citizen Charter (1)"),  # existing

        ("Rules and Regulations",
         CATEGORY_URLS["Rules and Regulations"],
         master_dir / "Rules and Regulations"),     # existing folder name

        ("Circulars and Notifications",
         CATEGORY_URLS["Circulars and Notifications"],
         master_dir / "circulars and notifications"),  # existing folder name
    ]

    # Acts folder — existing name on disk
    acts_dir = master_dir / "Acts" / "Acts"

    log.info("Categories to process:")
    for name, url, folder in categories:
        log.info("  ✔ %-50s → %s", name, folder)
    log.info("  ✔ %-50s → %s", "Acts and Ordinances", acts_dir)
    log.info("")

    # ── Build MASTER existing-PDF index ONCE across the entire TN GOs tree ─
    log.info("Scanning existing PDFs in: %s", master_dir)
    master_existing = get_existing_pdfs(master_dir)
    log.info("Total existing PDFs found  : %d  (these will all be skipped)", len(master_existing))
    log.info("")

    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": BASE_URL,
    })

    global_stats = {"downloaded": 0, "skipped": 0, "failed": 0}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        # ── Process dept→year→PDF categories ─────────────────────────────
        for cat_name, cat_url, cat_dir in categories:
            process_dept_year_category(
                page, session, cat_name, cat_url, cat_dir, global_stats, master_existing
            )

        # ── Process Acts & Ordinances (year→PDF, no depts) ───────────────
        if acts_dir:
            process_acts(page, session, acts_dir, global_stats, master_existing)

        browser.close()

    # ── Grand Summary ─────────────────────────────────────────────────────
    log.info("")
    log.info("=" * 70)
    log.info("  ALL CATEGORIES COMPLETE")
    log.info("=" * 70)
    log.info("  ✓  Total Downloaded : %d  new PDFs", global_stats["downloaded"])
    log.info("  ↷  Total Skipped    : %d  (already in local folder)", global_stats["skipped"])
    log.info("  ✗  Total Failed     : %d  (see tn_unified_downloader.log)", global_stats["failed"])
    log.info("=" * 70)
    log.info("  Full log → tn_unified_downloader.log")


if __name__ == "__main__":
    run()