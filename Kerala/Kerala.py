"""
Kerala Finance Department - Incremental PDF Downloader
Website: https://finance.kerala.gov.in

Sections covered:
  1. GOs           -> gos.jsp
  2. Budget        -> budget.jsp
  3. Notifications -> notifications.jsp
  4. Circulars     -> circular.jsp
  5. Reports       -> reports.jsp

INPUT: Your Excel file (one sheet per section)
       Each sheet has columns: Title | Date | File Name | Doc ID | URL

OUTPUT 1: Kerala GOs/<Section>/<filename.pdf>
OUTPUT 2: Kerala GOs.1.xlsx
          All old rows + new rows appended per sheet

SKIP LOGIC:
  Doc ID (dId) found in Excel sheet  -> SKIP
  Doc ID NOT found                   -> DOWNLOAD + append row to sheet

SETUP:
  pip install playwright requests openpyxl pandas
  playwright install chromium

USAGE:
  python Kerala.py
"""

# HOW TO RUN THIS SCRIPT CORRECTLY
#
# Step 1 - Open Terminal / PowerShell
# Step 2 - Navigate to the folder where THIS FILE (Kerala.py) is saved:
#          cd "C:\Users\varun\OneDrive\ドキュメント\Desktop\DEPARTMENT- ORG RTI"
# Step 3 - Activate your virtual environment (if using one):
#          .\venv\Scripts\activate
# Step 4 - Run the script:
#          python Kerala.py
#
# WRONG: py Kerala.py (wrong launcher)
# WRONG: python ../Kerala.py (wrong working directory)
# RIGHT: cd to the script folder FIRST, then: python Kerala.py
#
# OUTPUTS (both saved next to Kerala.py):
#   Kerala GOs/Government order/*.pdf
#   Kerala GOs/Budgets/*.pdf
#   Kerala GOs/Circulars/*.pdf
#   Kerala GOs/Notifications/*.pdf
#   Kerala GOs/Reports/*.pdf
#   Kerala GOs.1.xlsx <- updated Excel with all old + new rows


import re
import time
import shutil
import logging
import requests
import urllib3
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from copy import copy
from urllib.parse import urljoin, urlparse, parse_qs

from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ══════════════════════════════════════════════════════════════════════════════
#  ▶▶▶  CONFIGURE THESE TWO PATHS ONLY  ◀◀◀
# ══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# YOUR EXCEL FILE PATH  ← change this one line only
# ─────────────────────────────────────────────────────────────────────────────
EXCEL_FILE_PATH = r"C:\Users\varun\OneDrive\ドキュメント\Desktop\DEPARTMENT- ORG RTI\Kerala_RTI_Document_Index.xlsx"
# ↑ Copied from your VS Code Explorer.  The file is Kerala_RTI_Document_Index.xlsx
#   inside the DEPARTMENT- ORG RTI folder.

# ─────────────────────────────────────────────────────────────────────────────
# SECTION -> SHEET -> URL MAPPING
# "sheet" must EXACTLY match the tab name in your Excel (case-sensitive).
# "name"  is the subfolder created inside Kerala GOs\ for that section.
#
# From your VS Code Explorer screenshot the folders / sheet tabs are:
#   Government order | Budgets | Circulars | Notifications | Reports
#
# ⚠  Open your Excel, look at the bottom tabs, and confirm these names match.
#    If a tab is named "GOs" instead of "Government Orders", update it below.
# ─────────────────────────────────────────────────────────────────────────────
SECTIONS = [
    {
        "sheet":  "Government Orders",    # ← Excel tab name  (edit if different)
        "name":   "Government order",     # ← subfolder name in New_Downloads\
        "url":    "https://finance.kerala.gov.in/gos.jsp",
    },
    {
        "sheet":  "Budgets",
        "name":   "Budgets",
        "url":    "https://finance.kerala.gov.in/bdgtDcs.jsp",
    },
    {
        "sheet":  "Notifications",
        "name":   "Notifications",
        "url":    "https://finance.kerala.gov.in/ntfctn.jsp",
    },
    {
        "sheet":  "Circulars",
        "name":   "Circulars",
        "url":    "https://finance.kerala.gov.in/circlr.jsp",
    },
    {
        "sheet":  "Reports",
        "name":   "Reports",
        "url":    "https://finance.kerala.gov.in/rptDocs.jsp",
    },
]

# ── Excel column names ────────────────────────────────────────────────────────
# These must match the header row in each Excel sheet.
# From your count.py output the sheets have PDF entries — adjust if headers differ.
COL_TITLE    = "Sub Folder / Department"  # document title / description
COL_DATE     = "S.No"                     # using S.No as date placeholder (will be empty)
COL_FILENAME = "PDF File Name"            # saved PDF filename
COL_DOCID    = "PDF File Name"            # unique identifier (using filename as ID)
COL_URL      = "Main Folder"              # using Main Folder as URL placeholder

# ══════════════════════════════════════════════════════════════════════════════
#  WEBSITE SETTINGS  — do not change
# ══════════════════════════════════════════════════════════════════════════════
BASE_URL      = "https://finance.kerala.gov.in"
VIEWER_BASE   = f"{BASE_URL}/includeWeb/fileViewer.jsp"
DELAY         = 1.5       # seconds between downloads
DL_TIMEOUT    = 60        # seconds per HTTP download
PAGE_TIMEOUT  = 45_000    # ms Playwright page load

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kerala_finance_downloader.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — READ EXCEL -> build per-sheet skip indexes
# ══════════════════════════════════════════════════════════════════════════════

def build_indexes(excel_path: str) -> dict[str, set]:
    """
    Read every sheet of the Excel file.
    Returns {sheet_name: set_of_doc_ids_already_downloaded}.
    Doc IDs are the primary skip key (unique per document on the website).
    Also falls back to filename matching if Doc ID column is absent.
    """
    log.info("Reading Excel: %s", excel_path)
    xf = pd.ExcelFile(excel_path)
    indexes = {}

    for sec in SECTIONS:
        sheet = sec["sheet"]
        if sheet not in xf.sheet_names:
            log.warning("  Sheet '%s' NOT found in Excel — will treat as empty", sheet)
            indexes[sheet] = set()
            continue

        df = pd.read_excel(excel_path, sheet_name=sheet, header=0, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]

        ids = set()
        # Primary: Doc ID column
        if COL_DOCID in df.columns:
            for v in df[COL_DOCID].dropna():
                v = str(v).strip().lower()
                if v and v != "nan":
                    ids.add(v)
        # Fallback: File Name column
        if COL_FILENAME in df.columns:
            for v in df[COL_FILENAME].dropna():
                v = str(v).strip().lower()
                if v and v != "nan":
                    ids.add(v)

        indexes[sheet] = ids
        log.info("  Sheet '%-20s': %d known items", sheet, len(ids))

    return indexes


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — SCRAPE ONE SECTION PAGE
#  Returns list of dicts: {title, date, doc_id, view_url}
# ══════════════════════════════════════════════════════════════════════════════

def scrape_section(page, section_url: str, section_name: str) -> list[dict]:
    """
    Load a Kerala Finance section page and extract every document entry.

    The page structure is typically a table or list of rows, each with:
      - Document title / description text
      - Date (e.g. "Dated 27-03-2025")
      - A link to fileViewer.jsp?dId=<id>

    Returns list of {title, date, doc_id, view_url}
    """
    log.info("  Loading: %s", section_url)
    try:
        page.goto(section_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        page.wait_for_timeout(2_000)
    except PWTimeout:
        log.warning("  ⚠ Timeout loading %s", section_url)
        return []

    docs = []

    # ── Strategy 1: find all links to fileViewer.jsp ──────────────────────
    try:
        items = page.eval_on_selector_all(
            "a[href*='fileViewer.jsp'], a[href*='dId=']",
            """els => els.map(a => {
                const href = a.href || '';
                const row  = a.closest('tr, li, div.row, div.item, p') || a.parentElement;
                const text = row ? row.innerText.trim() : a.innerText.trim();
                return { href, text };
            })"""
        )

        for it in items:
            href = it.get("href", "")
            text = it.get("text", "").strip()
            if not href or "dId=" not in href:
                continue
            doc_id = _extract_did(href)
            if not doc_id:
                continue
            title, date = _split_title_date(text)
            docs.append({
                "title":    title,
                "date":     date,
                "doc_id":   doc_id,
                "view_url": href,
            })
    except Exception as exc:
        log.warning("  Strategy 1 failed: %s", exc)

        
    # ── Strategy 2: broader pass — all anchors, filter by URL pattern ─────
    if not docs:
        try:
            all_links = page.eval_on_selector_all(
                "a",
                "els => els.map(a => ({href: a.href, text: a.innerText.trim()}))"
            )
            for it in all_links:
                href = it.get("href", "")
                if "dId=" not in href and "fileViewer" not in href:
                    continue
                doc_id = _extract_did(href)
                if not doc_id:
                    continue
                title, date = _split_title_date(it.get("text", ""))
                docs.append({
                    "title":    title,
                    "date":     date,
                    "doc_id":   doc_id,
                    "view_url": href,
                })
        except Exception as exc:
            log.warning("  Strategy 2 failed: %s", exc)

    # ── Strategy 3: onclick handlers for row-based downloads ────────────────
    if not docs:
        try:
            items = page.eval_on_selector_all(
                "[onclick*='dwldDoc']",
                """els => {
                    const docs = [];
                    const seen = new Set();
                    for (const el of els) {
                        const onclick = el.getAttribute('onclick') || '';
                        const m = onclick.match(/dwldDoc\\(['\"]([^'\"]+)['\"]\\)/);
                        if (!m) continue;
                        const doc_id = m[1].trim();
                        if (!doc_id || seen.has(doc_id)) continue;
                        seen.add(doc_id);
                        const row = el.closest('tr') || el.closest('tbody') || el.closest('table') || el;
                        const text = row ? row.innerText.trim() : el.innerText.trim();
                        const href = `${location.origin}/includeWeb/fileViewer.jsp?dId=${doc_id}`;
                        docs.push({href, text, doc_id});
                    }
                    return docs;
                }"""
            )
            for it in items:
                doc_id = it.get("doc_id", "").strip()
                if not doc_id:
                    continue
                title, date = _split_title_date(it.get("text", ""))
                docs.append({
                    "title":    title,
                    "date":     date,
                    "doc_id":   doc_id,
                    "view_url": it.get("href", ""),
                })
        except Exception as exc:
            log.warning("  Strategy 3 failed: %s", exc)

    # ── Strategy 4: check for pagination / "Load more" / "Next" buttons ───
    # Try clicking next page if present and collect more docs
    try:
        page_docs = list(docs)
        visited_pages = 1
        while True:
            next_btn = page.query_selector(
                "a:has-text('Next'), a:has-text('next'), "
                "a[rel='next'], .pagination .next a, "
                "a:has-text('>'), button:has-text('Load More')"
            )
            if not next_btn:
                break
            try:
                next_btn.click()
                page.wait_for_timeout(2_000)
                visited_pages += 1
                extra = page.eval_on_selector_all(
                    "a[href*='dId='], a[href*='fileViewer']",
                    "els => els.map(a => ({href: a.href, text: a.innerText.trim()}))"
                )
                for it in extra:
                    href   = it.get("href", "")
                    doc_id = _extract_did(href)
                    if not doc_id:
                        continue
                    title, date = _split_title_date(it.get("text", ""))
                    page_docs.append({
                        "title":    title,
                        "date":     date,
                        "doc_id":   doc_id,
                        "view_url": href,
                    })
                if visited_pages > 50:   # safety cap
                    break
            except Exception:
                break
        docs = page_docs
    except Exception:
        pass

    # Deduplicate by doc_id
    seen = set()
    unique = []
    for d in docs:
        if d["doc_id"] and d["doc_id"] not in seen:
            seen.add(d["doc_id"])
            unique.append(d)

    log.info("  Found %d unique documents on page", len(unique))
    return unique


def _extract_did(url: str) -> str:
    """Extract the dId value from a fileViewer URL."""
    try:
        qs = parse_qs(urlparse(url).query)
        return qs.get("dId", [""])[0].strip()
    except Exception:
        return ""


def _split_title_date(text: str) -> tuple[str, str]:
    """
    Split a combined title+date string into (title, date).
    Dates on the site look like 'Dated 27-03-2025' or '27-03-2025'.
    """
    # Match 'Dated DD-MM-YYYY' or plain 'DD-MM-YYYY'
    m = re.search(r'[Dd]ated\s+(\d{2}-\d{2}-\d{4})', text)
    if not m:
        m = re.search(r'(\d{2}-\d{2}-\d{4})', text)
    if m:
        date  = m.group(1)
        title = text[:m.start()].strip(" -·|")
    else:
        date  = ""
        title = text.strip()
    return title, date


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — DOWNLOAD ONE PDF
#  Kerala Finance serves PDFs through fileViewer.jsp
#  The actual PDF is fetched via a direct download endpoint or
#  by intercepting the PDF URL inside the viewer iframe.
# ══════════════════════════════════════════════════════════════════════════════

def download_pdf(
    page,
    doc: dict,
    dest_folder: Path,
    session: requests.Session,
) -> tuple[bool, str]:
    """
    Download the PDF for *doc*.
    Returns (success: bool, saved_filename: str).

    Kerala Finance viewer opens PDFs inline in an iframe or via a direct URL.
    We try multiple strategies:
      1. Direct PDF URL patterns derived from the dId
      2. Navigate viewer page, intercept network requests for .pdf
      3. Extract src of iframe/embed inside the viewer
    """
    doc_id   = doc["doc_id"]
    view_url = doc["view_url"]

    # ── Strategy A: direct download URL pattern ───────────────────────────
    # Kerala Finance typically exposes PDFs at:
    #   /includeWeb/getPdf.jsp?dId=<id>   or   /download?dId=<id>
    direct_patterns = [
        view_url,
        f"{BASE_URL}/includeWeb/getPdf.jsp?dId={doc_id}",
        f"{BASE_URL}/includeWeb/downloadFile.jsp?dId={doc_id}",
        f"{BASE_URL}/downloadPdf?dId={doc_id}",
        f"{BASE_URL}/getPdf?dId={doc_id}",
    ]

    for dl_url in direct_patterns:
        fname, ok = _try_http_download(dl_url, doc_id, dest_folder, session)
        if ok:
            return True, fname

    # ── Strategy B: load viewer page, intercept PDF URL from network ──────
    pdf_url_found = []

    def handle_response(response):
        if response.url.lower().endswith(".pdf") or \
                "pdf" in response.headers.get("content-type", "").lower():
            pdf_url_found.append(response.url)

    page.on("response", handle_response)
    try:
        page.goto(view_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
        page.wait_for_timeout(3_000)
    except Exception as exc:
        if "Download is starting" in str(exc):
            log.debug("    Page started download directly, skipping network interception")
        else:
            log.warning("    Strategy B failed: %s", exc)
    page.remove_listener("response", handle_response)

    if pdf_url_found:
        fname, ok = _try_http_download(pdf_url_found[0], doc_id, dest_folder, session)
        if ok:
            return True, fname

    # ── Strategy C: find iframe / embed / object src ──────────────────────
    try:
        sources = page.eval_on_selector_all(
            "iframe, embed, object",
            "els => els.map(e => e.src || e.data || '')"
        )
        for src in sources:
            if src and ("pdf" in src.lower() or "dId" in src):
                abs_src = src if src.startswith("http") \
                    else urljoin(BASE_URL + "/", src.lstrip("/"))
                fname, ok = _try_http_download(abs_src, doc_id, dest_folder, session)
                if ok:
                    return True, fname
    except Exception:
        pass

    # ── Strategy D: look for any PDF link on the viewer page ─────────────
    try:
        pdf_links = page.eval_on_selector_all(
            "a[href*='.pdf'], a[href*='getPdf'], a[href*='download']",
            "els => els.map(a => a.href)"
        )
        for link in pdf_links:
            if link:
                fname, ok = _try_http_download(link, doc_id, dest_folder, session)
                if ok:
                    return True, fname
    except Exception:
        pass

    log.error("    ✗ All strategies failed for dId=%s", doc_id)
    return False, ""


def _try_http_download(
    url: str,
    doc_id: str,
    dest_folder: Path,
    session: requests.Session,
) -> tuple[str, bool]:
    """
    Attempt to HTTP-GET *url* and save it as a PDF.
    Returns (filename, success).
    """
    try:
        try:
            r = session.get(url, timeout=DL_TIMEOUT, stream=True, allow_redirects=True)
        except requests.exceptions.SSLError:
            log.warning("    SSL verification failed for %s, retrying without verify", url)
            r = session.get(url, timeout=DL_TIMEOUT, stream=True, allow_redirects=True, verify=False)

        if r.status_code != 200:
            return "", False

        ct = r.headers.get("Content-Type", "")
        if "html" in ct and "pdf" not in ct:
            return "", False

        # Derive filename
        cd = r.headers.get("Content-Disposition", "")
        fname = ""
        m = re.search(r'filename[^;=\n]*=["\']?([^"\';\n]+)', cd)
        if m:
            fname = m.group(1).strip().strip('"\'')
        if not fname:
            fname = urlparse(url).path.split("/")[-1].split("?")[0]
        if not fname or not fname.lower().endswith(".pdf"):
            fname = f"{doc_id}.pdf"

        # Sanitize
        fname = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', fname)

        dest = dest_folder / fname
        dest_folder.mkdir(parents=True, exist_ok=True)

        with open(dest, "wb") as fh:
            for chunk in r.iter_content(16_384):
                fh.write(chunk)

        # Verify it's a real PDF (first 4 bytes = %PDF)
        with open(dest, "rb") as fh:
            header = fh.read(4)
        if header != b"%PDF":
            dest.unlink(missing_ok=True)
            return "", False

        kb = dest.stat().st_size // 1024
        log.info("    ✓ Saved  %s  (%d KB)  [%s]", fname, kb, url)
        return fname, True
    except Exception as exc:
        log.debug("    http-try failed for %s: %s", url, exc)
        return "", False


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 4 — BUILD UPDATED EXCEL
# ══════════════════════════════════════════════════════════════════════════════

def build_updated_excel(
    original_path: str,
    output_path: str,
    new_rows_by_sheet: dict[str, list],
):
    """
    Copy original Excel -> output_path.
    For each sheet, append new rows at the bottom (after the last data row).
    Preserves all existing formatting.
    New rows are styled to match existing data rows.
    """
    shutil.copy2(original_path, output_path)

    if not any(new_rows_by_sheet.values()):
        log.info("No new rows — Excel copied as-is to: %s", output_path)
        return

    wb = load_workbook(output_path)

    # Style helpers for new rows
    lft_align   = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    ctr_align   = Alignment(horizontal="center", vertical="center", wrap_text=True)

    def copy_cell_style(src_cell, dst_cell):
        dst_cell.font = copy(src_cell.font)
        dst_cell.border = copy(src_cell.border)
        dst_cell.fill = copy(src_cell.fill)
        dst_cell.number_format = src_cell.number_format
        dst_cell.protection = copy(src_cell.protection)
        dst_cell.alignment = copy(src_cell.alignment)

    for sec in SECTIONS:
        sheet_name = sec["sheet"]
        rows       = new_rows_by_sheet.get(sheet_name, [])
        if not rows:
            continue

        if sheet_name not in wb.sheetnames:
            # Create the sheet if it doesn't exist yet
            wb.create_sheet(sheet_name)
            ws = wb[sheet_name]
            # Write header
            headers = [COL_TITLE, COL_DATE, COL_FILENAME, COL_DOCID, COL_URL]
            for c, h in enumerate(headers, 1):
                cell = ws.cell(row=1, column=c, value=h)
                cell.font = Font(bold=True)
        else:
            ws = wb[sheet_name]

        # Find last row with data
        last_row = ws.max_row
        # Skip completely empty trailing rows
        while last_row > 1:
            if any(ws.cell(row=last_row, column=c).value
                   for c in range(1, 6)):
                break
            last_row -= 1

        insert_at = last_row + 1

        # Detect column order from header row
        header_map = {}
        for cell in ws[1]:
            if cell.value:
                header_map[str(cell.value).strip()] = cell.column

        col_title  = header_map.get(COL_TITLE,    1)
        col_date   = header_map.get(COL_DATE,     2)
        col_fname  = header_map.get(COL_FILENAME, 3)
        col_docid  = header_map.get(COL_DOCID,    4)
        col_url    = header_map.get(COL_URL,       5)

        template_row = last_row if last_row > 1 else 1

        for offset, r in enumerate(rows):
            row_num = insert_at + offset
            data = {
                col_title: r.get("title",    ""),
                col_date:  r.get("date",     ""),
                col_fname: r.get("filename", ""),
                col_url:   r.get("view_url", ""),
            }
            for col, val in data.items():
                cell = ws.cell(row=row_num, column=col, value=val)
                template = ws.cell(row=template_row, column=col)
                try:
                    copy_cell_style(template, cell)
                except Exception:
                    cell.alignment = ctr_align if col in (col_date, col_docid) else lft_align

            if ws.row_dimensions.get(template_row) and ws.row_dimensions[template_row].height is not None:
                ws.row_dimensions[row_num].height = ws.row_dimensions[template_row].height

        log.info("  Sheet '%-20s': %d new rows appended (row %d onward)",
                 sheet_name, len(rows), insert_at)

    wb.save(output_path)
    log.info("Updated Excel saved -> %s", output_path)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run():
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log.info("=" * 70)
    log.info("  KERALA FINANCE DEPT – INCREMENTAL PDF DOWNLOADER")
    log.info("  Started : %s", ts)
    log.info("  Excel   : %s", EXCEL_FILE_PATH)
    log.info("=" * 70)

    excel_path = Path(EXCEL_FILE_PATH)
    if not excel_path.exists():
        log.error("Excel file NOT found: %s", EXCEL_FILE_PATH)
        return

    # ── Output paths ──────────────────────────────────────────────────────
    out_dir       = excel_path.parent
    script_dir    = Path(__file__).parent   # folder where Kerala.py lives
    dl_root       = script_dir / "Kerala GOs"            # OUTPUT 1
    updated_excel = script_dir / "Kerala GOs.1.xlsx"
    #                                                  OUTPUT 2
    dl_root.mkdir(parents=True, exist_ok=True)

    log.info("  OUTPUT 1 (new PDFs)    : %s", dl_root)
    log.info("  OUTPUT 2 (updated xlsx): %s", updated_excel)
    log.info("")

    # ── Step 1: Build skip indexes from Excel ─────────────────────────────
    indexes = build_indexes(EXCEL_FILE_PATH)

    # ── Step 2: HTTP session ──────────────────────────────────────────────
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": BASE_URL,
        "Accept":  "application/pdf,application/octet-stream,*/*",
    })

    global_stats       = {"downloaded": 0, "skipped": 0, "failed": 0}
    new_rows_by_sheet  = {sec["sheet"]: [] for sec in SECTIONS}

    # ── Step 3: Scrape & download ─────────────────────────────────────────
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            accept_downloads=True,
        )
        page = ctx.new_page()

        for sec in SECTIONS:
            sheet_name   = sec["sheet"]
            section_name = sec["name"]
            section_url  = sec["url"]
            known_ids    = indexes[sheet_name]
            section_dl   = dl_root / section_name
            section_dl.mkdir(parents=True, exist_ok=True)

            log.info("")
            log.info("▓" * 70)
            log.info("  SECTION : %s", section_name)
            log.info("  URL     : %s", section_url)
            log.info("  Known   : %d items (will be skipped)", len(known_ids))
            log.info("▓" * 70)

            # Scrape list of documents from the section page
            docs = scrape_section(page, section_url, section_name)

            if not docs:
                log.warning("  No documents found — check URL or page structure.")
                continue

            sec_stats = {"downloaded": 0, "skipped": 0, "failed": 0}

            for doc in docs:
                doc_id = doc["doc_id"]
                title  = doc["title"]
                date   = doc["date"]

                # ── SKIP CHECK ────────────────────────────────────────────
                if (doc_id.lower() in known_ids or
                        doc_id in known_ids or
                        title.lower() in known_ids):
                    log.info("  ↷ SKIP   [%s]  %s", date, title[:70])
                    sec_stats["skipped"]    += 1
                    global_stats["skipped"] += 1
                    continue

                # ── NEW DOC -> DOWNLOAD ────────────────────────────────────
                log.info("  ↓ NEW    [%s]  %s", date, title[:70])
                time.sleep(DELAY)

                ok, fname = download_pdf(page, doc, section_dl, session)

                if ok:
                    sec_stats["downloaded"]    += 1
                    global_stats["downloaded"] += 1
                    known_ids.add(doc_id.lower())

                    new_rows_by_sheet[sheet_name].append({
                        "title":    title,
                        "date":     date,
                        "filename": fname,
                        "doc_id":   doc_id,
                        "view_url": doc["view_url"],
                    })
                else:
                    sec_stats["failed"]    += 1
                    global_stats["failed"] += 1

            log.info("")
            log.info("  ── %s Section Summary ──", section_name)
            log.info("     ✓ Downloaded : %d", sec_stats["downloaded"])
            log.info("     ↷ Skipped    : %d", sec_stats["skipped"])
            log.info("     ✗ Failed     : %d", sec_stats["failed"])

        browser.close()

    # ── Step 4: Build updated Excel (OUTPUT 2) ────────────────────────────
    log.info("")
    log.info("Building updated Excel …")
    try:
        build_updated_excel(
            original_path     = EXCEL_FILE_PATH,
            output_path       = str(updated_excel),
            new_rows_by_sheet = new_rows_by_sheet,
        )
    except Exception as exc:
        log.error("Excel build failed: %s", exc)
        log.error("Unwritten rows (save manually if needed):")
        for sh, rows in new_rows_by_sheet.items():
            for r in rows:
                log.error("  [%s] %s", sh, r)

    # ── Grand summary ─────────────────────────────────────────────────────
    total_new = sum(len(v) for v in new_rows_by_sheet.values())
    log.info("")
    log.info("=" * 70)
    log.info("  ALL DONE  —  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 70)
    log.info("")
    log.info("  RESULTS:")
    log.info("  ✓  Downloaded      : %d  new PDFs", global_stats["downloaded"])
    log.info("  ↷  Skipped         : %d  (already in Excel)", global_stats["skipped"])
    log.info("  ✗  Failed          : %d  (see log for details)", global_stats["failed"])
    log.info("  📄 Excel rows added: %d  (across all sheets)", total_new)
    log.info("")
    log.info("  OUTPUTS:")
    log.info("  📁 New PDFs  -> %s", dl_root)
    log.info("     Structure:")
    for sec in SECTIONS:
        log.info("       Kerala GOs\\\\%s\\\\<filename.pdf>", sec["name"])
    log.info("")
    log.info("  📊 Updated Excel -> %s", updated_excel)
    log.info("     Saved as: Kerala GOs.1.xlsx")
    log.info("     One sheet per section, new rows appended at bottom of each sheet")
    log.info("=" * 70)
    log.info("  Full log -> kerala_finance_downloader.log")


if __name__ == "__main__":
    run()