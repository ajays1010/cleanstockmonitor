import os
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging

# Configure logging for financial data APIs
logging.basicConfig(level=logging.INFO)
api_logger = logging.getLogger('financial_api')
api_logger.setLevel(logging.INFO if os.environ.get('YAHOO_VERBOSE', '0') == '1' or os.environ.get('BSE_VERBOSE', '0') == '1' else logging.WARNING)

# Patch httpx to support 'proxy' kwarg by remapping to 'proxies' for older httpx versions
try:
    import httpx as _httpx
    _OrigClient = _httpx.Client
    class _PatchedClient(_OrigClient):
        def __init__(self, *args, **kwargs):
            if 'proxy' in kwargs:
                proxy_val = kwargs.pop('proxy')
                if proxy_val is not None and 'proxies' not in kwargs:
                    kwargs['proxies'] = proxy_val
            super().__init__(*args, **kwargs)
    _httpx.Client = _PatchedClient

    _OrigAsyncClient = _httpx.AsyncClient
    class _PatchedAsyncClient(_OrigAsyncClient):
        def __init__(self, *args, **kwargs):
            if 'proxy' in kwargs:
                proxy_val = kwargs.pop('proxy')
                if proxy_val is not None and 'proxies' not in kwargs:
                    kwargs['proxies'] = proxy_val
            super().__init__(*args, **kwargs)
    _httpx.AsyncClient = _PatchedAsyncClient
    if os.environ.get("YAHOO_VERBOSE", "0") == "1":
        print("Applied httpx proxy compatibility patch (database.py).")
except Exception:
    pass

from supabase import create_client, Client
from gotrue.errors import AuthApiError
import firebase_admin
from firebase_admin import credentials, auth
from datetime import datetime, timezone, timedelta

# --- Firebase Admin SDK Initialization ---
firebase_app = None

def initialize_firebase():
    """Initializes the Firebase Admin SDK.
    Supports either a file path via GOOGLE_APPLICATION_CREDENTIALS or raw JSON
    in FIREBASE_SERVICE_ACCOUNT_JSON (written to /tmp/firebase_sa.json).
    Gracefully handles missing credentials for development environments."""
    global firebase_app
    if firebase_app:
        print("Firebase Admin SDK already initialized.")
        return True

    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    # If the path is missing or file doesn't exist, try JSON env var -> write to /tmp
    if not key_path or not os.path.exists(key_path):
        json_blob = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
        if json_blob:
            try:
                # Use temp directory for Windows compatibility
                import tempfile
                tmp_dir = tempfile.gettempdir()
                tmp_path = os.path.join(tmp_dir, "firebase_sa.json")
                with open(tmp_path, "w", encoding="utf-8") as f:
                    f.write(json_blob)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp_path
                key_path = tmp_path
                print(f"Firebase credentials written to temporary file: {tmp_path}")
            except Exception as e:
                print(f"Failed to write FIREBASE_SERVICE_ACCOUNT_JSON to temp directory: {e}")

    # Fallback to local service account file in repo if still missing
    if (not key_path or not os.path.exists(key_path)) and os.path.exists(
        "bsemonitoring-64a8e-firebase-adminsdk-fbsvc-6898240c34.json"
    ):
        key_path = "bsemonitoring-64a8e-firebase-adminsdk-fbsvc-6898240c34.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
        print(f"Using local Firebase service account file: {key_path}")

    if not key_path or not os.path.exists(key_path):
        print("WARNING: Firebase service account key not found.")
        print("The application will continue without Firebase authentication.")
        print("To enable Firebase, set either:")
        print("  - GOOGLE_APPLICATION_CREDENTIALS environment variable to the path of your service account key")
        print("  - FIREBASE_SERVICE_ACCOUNT_JSON environment variable with the JSON content")
        return False

    try:
        cred = credentials.Certificate(key_path)
        firebase_app = firebase_admin.initialize_app(cred)
        print("Firebase Admin SDK initialized successfully.")
        return True
    except Exception as e:
        print(f"ERROR: Failed to initialize Firebase Admin SDK: {e}")
        print("The application will continue without Firebase authentication.")
        return False

# --- Supabase Client Initialization ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# Workaround for environments where proxy env vars cause supabase/httpx init issues
# e.g., "Client.__init__() got an unexpected keyword argument 'proxy'"
_PROXY_ENV_VARS = [
    "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"
]

def _suppress_proxy_env_for_supabase():
    changed = []
    for k in _PROXY_ENV_VARS:
        if os.environ.pop(k, None) is not None:
            changed.append(k)
    if changed:
        print(f"Notice: Temporarily ignoring proxy env vars for Supabase client: {', '.join(changed)}")

# --- Telegram Bot Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Yahoo Finance session and cache
_YAHOO_SESSION = None
_YAHOO_CACHE_SERIES = {}
_YAHOO_CACHE_TTL = int(os.environ.get("YAHOO_CACHE_TTL", "60"))

# ---- Price helpers (CMP vs previous close with robust fallbacks) ----
import re as _re
from bs4 import BeautifulSoup as _BS
import requests as _requests
import yfinance as _yf

def _yahoo_symbol_to_bse_code(sym: str):
    try:
        base = sym.split('.')[0]
        if base.isdigit():
            return base
    except Exception:
        pass
    return None

def _fetch_chart_meta(sym: str):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=1d&interval=1m"
        r = _requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        result = (data or {}).get('chart', {}).get('result')
        if not result:
            return None
        meta = result[0].get('meta') or {}
        return meta
    except Exception:
        return None

def _fetch_quote_price(sym: str):
    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={sym}"
        r = _requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        result = (data or {}).get('quoteResponse', {}).get('result') or []
        if not result:
            return None
        row = result[0]
        for key in ('regularMarketPrice', 'postMarketPrice', 'preMarketPrice'):
            if row.get(key) is not None:
                try:
                    return float(row[key])
                except Exception:
                    pass
        return None
    except Exception:
        return None

def _scrape_screener_cmp(sym: str):
    bse_code = _yahoo_symbol_to_bse_code(sym)
    if not bse_code:
        return None
    url = f"https://www.screener.in/company/{bse_code}/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
        'Referer': 'https://www.screener.in/'
    }
    try:
        r = _requests.get(url, headers=headers, timeout=12)
        if r.status_code != 200:
            return None
        html = r.text
        m = _re.search(r'Current\s*Price[^0-9]*([0-9]+(?:,[0-9]{2,3})*(?:\.[0-9]+)?)', html, _re.I | _re.S)
        if not m:
            m = _re.search(r'\bCMP\b[^0-9]*([0-9]+(?:,[0-9]{2,3})*(?:\.[0-9]+)?)', html, _re.I | _re.S)
        if m:
            txt = m.group(1).replace(',', '')
            try:
                return float(txt)
            except Exception:
                pass
        soup = _BS(html, 'lxml')
        numbers = []
        for span in soup.select('span.number, span.value'):
            t = (span.get_text() or '').strip().replace(',', '')
            if _re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', t):
                try:
                    numbers.append(float(t))
                except Exception:
                    pass
        if numbers:
            numbers.sort()
            return numbers[len(numbers)//2]
        return None
    except Exception:
        return None

def _last_today_value(series):
    if series is None or series.empty:
        return None
    try:
        idx = series.index
        idx_ist = idx.tz_localize('UTC').tz_convert(IST_TZ) if getattr(idx, 'tz', None) is None else idx.tz_convert(IST_TZ)
        s2 = series.copy()
        s2.index = idx_ist
        s2 = s2.dropna()
        now = ist_now()
        s2 = s2[s2.index.date == now.date()]
        s2 = s2[s2.index <= now]
        if not s2.empty:
            return float(s2.iloc[-1])
        return None
    except Exception:
        return None

def _latest_cmp(sym: str):
    # Try intraday up to 30m for today only
    for rng, iv in [('1d','1m'),('1d','5m'),('1d','15m'),('1d','30m')]:
        s = yahoo_chart_series_cached(sym, rng, iv)
        val = _last_today_value(s)
        if val is not None:
            return val, f"chart_{rng}_{iv}"
    # Meta/quote
    meta = _fetch_chart_meta(sym)
    if meta and meta.get('regularMarketPrice') is not None:
        try:
            return float(meta['regularMarketPrice']), 'meta_rmp'
        except Exception:
            pass
    qp = _fetch_quote_price(sym)
    if qp is not None:
        return qp, 'quote_v7'
    # yfinance
    try:
        t = _yf.Ticker(sym)
        fi = getattr(t, 'fast_info', None)
        if fi and fi.get('last_price') is not None:
            return float(fi['last_price']), 'yf_fast_info'
        for iv, label in [('1m','yf_hist_1m'),('5m','yf_hist_5m'),('15m','yf_hist_15m'),('30m','yf_hist_30m')]:
            try:
                hist = t.history(period='1d', interval=iv)
                if hist is not None and not hist.empty:
                    close = hist.get('Close')
                    if close is not None:
                        close = close.dropna()
                        if len(close) > 0:
                            return float(close.iloc[-1]), label
            except Exception as hist_err:
                if os.environ.get("YAHOO_VERBOSE", "0") == "1":
                    print(f"yfinance history error for {sym} {iv}: {hist_err}")
                continue
    except Exception as yf_err:
        if os.environ.get("YAHOO_VERBOSE", "0") == "1":
            # Check for common delisted/invalid symbol patterns
            err_msg = str(yf_err).lower()
            if any(phrase in err_msg for phrase in ['delisted', 'no data', 'invalid symbol', 'not found']):
                print(f"yfinance symbol {sym} appears to be delisted or invalid: {yf_err}")
            else:
                print(f"yfinance error for {sym}: {yf_err}")
        pass
    return None, None

def get_cmp_with_fallback(symbol: str, fallback_message: str = "Data unavailable"):
    """Get current market price with graceful fallback for unavailable data"""
    try:
        price, prev_close, source = get_cmp_and_prev(symbol)
        if price is not None:
            return {
                'success': True,
                'price': price,
                'prev_close': prev_close,
                'source': source,
                'symbol': symbol
            }
    except Exception as e:
        if os.environ.get("YAHOO_VERBOSE", "0") == "1":
            api_logger.warning(f"Error getting price for {symbol}: {e}")
    
    # Fallback response when data is unavailable
    return {
        'success': False,
        'price': None,
        'prev_close': None,
        'source': 'unavailable',
        'symbol': symbol,
        'message': fallback_message
    }

def is_symbol_likely_delisted(symbol: str) -> bool:
    """Check if a symbol appears to be delisted based on recent data availability"""
    try:
        # Try multiple timeframes to determine if symbol is consistently unavailable
        timeframes = [('1d', '1m'), ('5d', '1d'), ('1mo', '1d')]
        failures = 0
        
        for range_str, interval in timeframes:
            result = yahoo_chart_series_cached(symbol, range_str, interval)
            if result is None or result.empty:
                failures += 1
        
        # If all timeframes fail, likely delisted
        return failures == len(timeframes)
    except Exception:
        return True  # Assume delisted if we can't even test

def _daily_closes(sym: str):
    s_daily = yahoo_chart_series_cached(sym, '10d', '1d')
    closes = s_daily.dropna() if (s_daily is not None and not s_daily.empty) else None
    last_close = float(closes.iloc[-1]) if (closes is not None and len(closes) >= 1) else None
    prev_close = float(closes.iloc[-2]) if (closes is not None and len(closes) >= 2) else None
    prev_prev_close = float(closes.iloc[-3]) if (closes is not None and len(closes) >= 3) else None
    return last_close, prev_close, prev_prev_close

def get_cmp_and_prev(sym: str):
    """Return (cmp, prev_close, source_label) based on IST time rules.
    - Before 09:00 IST: cmp = last_close; prev = prev_close (fallback prev_prev)
    - 09:00-15:30 IST: cmp = intraday/latest/meta/quote/yf/screener; prev = last_close
    - After 15:30 IST: cmp = last_close; prev = prev_close
    """
    last_close, prev_close, prev_prev_close = _daily_closes(sym)
    is_open, open_dt, close_dt = ist_market_window()
    now = ist_now()
    if now < open_dt:
        cmp_price = last_close
        prev = prev_close if prev_close is not None else prev_prev_close
        return cmp_price, prev, 'preopen_last_close'
    elif is_open:
        cmp_price, src = _latest_cmp(sym)
        if cmp_price is None:
            cmp_price = _scrape_screener_cmp(sym)
            src = src or ('screener' if cmp_price is not None else 'no_intraday')
        return cmp_price, last_close, src
    else:
        return last_close, prev_close, 'postclose_last_close'

def get_close_3m_ago(sym: str):
    """Return close price around 3 months ago (nearest working day within ±3 days).
    Uses 6 months of daily data.
    """
    try:
        import pandas as pd
        from datetime import timedelta
        target = ist_now().date() - timedelta(days=90)
        s = yahoo_chart_series_cached(sym, '6mo', '1d')
        if s is None or s.empty:
            return None
        # Convert index to IST date
        idx = s.index
        idx_ist = idx.tz_localize('UTC').tz_convert(IST_TZ) if getattr(idx, 'tz', None) is None else idx.tz_convert(IST_TZ)
        s2 = s.copy()
        s2.index = idx_ist
        s2 = s2.dropna()
        if s2.empty:
            return None
        # Find nearest date
        dates = pd.Series(s2.index.date, index=s2.index)
        # Compute absolute day diff
        diffs = dates.apply(lambda d: abs((d - target).days))
        # Filter within ±3 days
        within = diffs[diffs <= 3]
        if within.empty:
            return None
        # Pick the smallest diff; get corresponding value
        nearest_index = within.sort_values().index[0]
        return float(s2.loc[nearest_index])
    except Exception:
        return None

# Lazy-loaded company dataframe for symbol lookups
_COMPANY_DF = None

def get_company_df():
    global _COMPANY_DF
    if _COMPANY_DF is None:
        import pandas as pd
        try:
            _COMPANY_DF = pd.read_csv('indian_stock_tickers.csv')
        except Exception:
            _COMPANY_DF = None
    return _COMPANY_DF

def bse_code_to_yahoo_symbol(bse_code):
    df = get_company_df()
    if df is None:
        return None
    try:
        bse_code_int = int(str(bse_code))
        row = df[df['BSE Code'] == bse_code_int]
    except Exception:
        row = df[df['BSE Code'].astype(str) == str(bse_code)]
    if row is None or row.empty:
        return None
    sym = str(row.iloc[0].get('Yahoo Symbol', '')).strip()
    return sym or None

def get_yahoo_session():
    global _YAHOO_SESSION
    if _YAHOO_SESSION is None:
        import requests
        s = requests.Session()
        s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'})
        _YAHOO_SESSION = s
    return _YAHOO_SESSION

def yahoo_chart_series_cached(symbol: str, range_str: str, interval: str):
    # Returns pandas Series of closes indexed by datetime, or None
    import time
    import pandas as pd
    session = get_yahoo_session()
    key = (symbol, range_str, interval)
    # Check cache
    cached = _YAHOO_CACHE_SERIES.get(key)
    now = time.time()
    if cached is not None:
        ts, series = cached
        if now - ts < _YAHOO_CACHE_TTL:
            return series
    # Fetch
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={range_str}&interval={interval}"
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            if os.environ.get("YAHOO_VERBOSE", "0") == "1":
                print(f"Chart API HTTP {r.status_code} for {symbol} {range_str}/{interval}: {r.text[:120]}")
            return None
        
        # Improved JSON parsing with better error handling
        try:
            data = r.json()
        except ValueError as json_err:
            if os.environ.get("YAHOO_VERBOSE", "0") == "1":
                print(f"Chart API JSON parsing failed for {symbol}: {json_err} - Response: {r.text[:200]}")
            return None
        
        result = (data or {}).get('chart', {}).get('result')
        if not result:
            # Check for delisted/invalid symbol errors in the response
            error = (data or {}).get('chart', {}).get('error')
            if error and os.environ.get("YAHOO_VERBOSE", "0") == "1":
                print(f"Chart API error for {symbol}: {error.get('description', 'Unknown error')}")
            return None
        result = result[0]
        closes = result.get('indicators', {}).get('quote', [{}])[0].get('close') or []
        timestamps = result.get('timestamp') or []
        if not closes or not timestamps:
            return None
        s = pd.Series(closes, index=pd.to_datetime(timestamps, unit='s')).dropna()
        _YAHOO_CACHE_SERIES[key] = (now, s)
        return s
    except Exception as e:
        if os.environ.get("YAHOO_VERBOSE", "0") == "1":
            print(f"Chart API error for {symbol} {range_str}/{interval}: {e}")
        return None

supabase_anon: Client = None
supabase_service: Client = None

def get_supabase_client(service_role=False):
    """Initializes and returns the appropriate Supabase client.
    Returns None if configuration is missing or initialization fails.
    """
    global supabase_anon, supabase_service
    if service_role:
        if supabase_service is None:
            if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
                print("CRITICAL: Supabase Service Key not set.")
                return None
            try:
                _suppress_proxy_env_for_supabase()
                supabase_service = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
            except Exception as e:
                print(f"CRITICAL: Failed to initialize Supabase service client: {e}")
                supabase_service = None
                return None
        return supabase_service
    else:
        if supabase_anon is None:
            if not SUPABASE_URL or not SUPABASE_KEY:
                print("CRITICAL: Supabase Anon Key not set.")
                return None
            try:
                _suppress_proxy_env_for_supabase()
                supabase_anon = create_client(SUPABASE_URL, SUPABASE_KEY)
            except Exception as e:
                print(f"CRITICAL: Failed to initialize Supabase anon client: {e}")
                supabase_anon = None
                return None
        return supabase_anon

# --- Unified User Authentication Logic ---
def find_or_create_supabase_user(decoded_token):
    """
    Finds a user in Supabase by their Firebase/Google UID or email.
    If not found, creates a new user. Returns a new Supabase session.
    """
    # Ensure Firebase Admin SDK is initialized
    initialize_firebase()

    sb_admin = get_supabase_client(service_role=True)
    if not sb_admin:
        return {"session": None, "error": "Admin client not configured."}

    provider_uid = decoded_token['uid']
    
    # Prefer values present in the verified token
    email = decoded_token.get('email')
    phone_number = decoded_token.get('phone_number')

    try:
        # Only call Admin API if we still miss fields
        if not email or not phone_number:
            firebase_user_record = auth.get_user(provider_uid)
            email = email or firebase_user_record.email
            phone_number = phone_number or firebase_user_record.phone_number

        if not email and firebase_user_record.provider_data:
            for provider_info in firebase_user_record.provider_data:
                if provider_info.email:
                    email = provider_info.email
                    break
    except Exception:
        # Ignore Admin lookup failures; we keep whatever we have from the token
        pass

    provider = decoded_token['firebase']['sign_in_provider']
    uid_column = 'google_uid' if provider == 'google.com' else 'firebase_uid'

    # 1. Try to find an existing user
    profile_response = sb_admin.table('profiles').select('id, email').eq(uid_column, provider_uid).execute()
    profile = profile_response.data[0] if profile_response.data else None
    
    if not profile and email:
        profile_response = sb_admin.table('profiles').select('id, email').eq('email', email).execute()
        profile = profile_response.data[0] if profile_response.data else None
        if profile:
            sb_admin.table('profiles').update({uid_column: provider_uid}).eq('id', profile['id']).execute()

    # If we found an existing profile, return identifiers and allow app session login
    if profile:
        # If we have a better email now, update profiles and auth.users when placeholder is present
        if email and (not profile.get('email') or profile.get('email', '').endswith('@yourapp.com')):
            try:
                sb_admin.table('profiles').update({'email': email}).eq('id', profile['id']).execute()
                try:
                    sb_admin.auth.admin.update_user(profile['id'], {'email': email})
                except Exception:
                    # Non-fatal if auth update fails
                    pass
                profile['email'] = email
            except Exception:
                pass
        return {
            "session": None,
            "email": profile['email'],
            "user_id": profile['id'],
            "phone": phone_number,
            "error": None,
        }

    # 3. If no user is found, create a new one
    try:
        user_attrs = {}
        if email:
            user_attrs['email'] = email
        elif phone_number:
            user_attrs['phone'] = phone_number
            user_attrs['email'] = f"{phone_number}@yourapp.com"
        else:
            user_attrs['email'] = f"{provider_uid}@yourapp.com"

        new_user_response = sb_admin.auth.admin.create_user(user_attrs)
        new_user = new_user_response.user
        
        sb_admin.table('profiles').update({uid_column: provider_uid}).eq('id', new_user.id).execute()
        
        # Skip generating Supabase session links; authenticate app-side via Flask session
        return {
            "session": None,
            "email": new_user.email,
            "user_id": new_user.id,
            "phone": phone_number,
            "error": None,
        }

    except Exception as e:
        return {"session": None, "email": email, "user_id": None, "phone": phone_number, "error": str(e)}


# --- User-Specific Data Functions (Remain the same) ---

def get_user_category_prefs(user_client, user_id: str):
    """Return a list of enabled categories for this user.
    If no prefs stored, default to all allowed categories.
    """
    try:
        resp = (
            user_client.table('bse_category_prefs')
            .select('categories')
            .eq('user_id', user_id)
            .limit(1)
            .execute()
        )
        # Handle case where no record exists
        if not resp.data:
            return list(ALLOWED_ANNOUNCEMENT_CATEGORIES)
        
        cats = resp.data[0].get('categories') if resp.data else None
        if not cats:
            return list(ALLOWED_ANNOUNCEMENT_CATEGORIES)
        # Ensure only known categories are returned
        return [c for c in cats if c in ALLOWED_ANNOUNCEMENT_CATEGORIES]
    except Exception:
        return list(ALLOWED_ANNOUNCEMENT_CATEGORIES)

def set_user_category_prefs(user_client, user_id: str, categories: list[str]):
    """Upsert user category preferences. Filters to allowed set."""
    try:
        cats = [c for c in categories if c in ALLOWED_ANNOUNCEMENT_CATEGORIES]
        payload = {'user_id': user_id, 'categories': cats}
        # Try update, fallback insert
        existing = user_client.table('bse_category_prefs').select('user_id').eq('user_id', user_id).limit(1).execute().data or []
        if existing:
            user_client.table('bse_category_prefs').update(payload).eq('user_id', user_id).execute()
        else:
            user_client.table('bse_category_prefs').insert(payload).execute()
        return True
    except Exception:
        return False
def get_user_scrips(user_client, user_id: str):
    return (
        user_client
        .table('monitored_scrips')
        .select('bse_code, company_name')
        .eq('user_id', user_id)
        .execute()
        .data or []
    )

def get_user_recipients(user_client, user_id: str):
    """Get all telegram recipients for a user, including user_name field."""
    return (
        user_client
        .table('telegram_recipients')
        .select('chat_id, user_name')
        .eq('user_id', user_id)
        .execute()
        .data or []
    )

def add_user_scrip(user_client, user_id: str, bse_code: str, company_name: str):
    user_client.table('monitored_scrips').insert({'user_id': user_id, 'bse_code': bse_code, 'company_name': company_name}).execute()

def delete_user_scrip(user_client, user_id: str, bse_code: str):
    user_client.table('monitored_scrips').delete().eq('user_id', user_id).eq('bse_code', bse_code).execute()

def add_user_recipient(user_client, user_id: str, chat_id: str, user_name: str = None):
    """
    Add a chat_id with user_name to a user's recipients list.
    Multiple users can now share the same chat_id with different user_names.
    Returns a dict with 'success' boolean and 'message' string.
    """
    chat_id_str = str(chat_id).strip()
    user_name_str = str(user_name).strip() if user_name else f'User_{user_id[:8]}'
    
    try:
        # Check if exact triplet (user_id, chat_id, user_name) already exists
        existing = (
            user_client.table('telegram_recipients')
            .select('user_id')
            .eq('user_id', user_id)
            .eq('chat_id', chat_id_str)
            .eq('user_name', user_name_str)
            .limit(1)
            .execute()
        )
        if existing.data:
            return {'success': True, 'message': f'Recipient "{user_name_str}" with Chat ID {chat_id_str} is already registered for this account.'}
        
        # Attempt to insert with user_name
        user_client.table('telegram_recipients').insert({
            'user_id': user_id, 
            'chat_id': chat_id_str, 
            'user_name': user_name_str
        }).execute()
        return {'success': True, 'message': f'Successfully added recipient "{user_name_str}" with Chat ID {chat_id_str}.'}
        
    except Exception as e:
        error_msg = str(e).lower()
        if '409' in error_msg or 'conflict' in error_msg or 'unique' in error_msg:
            return {
                'success': False, 
                'message': f'The combination of Chat ID {chat_id_str} and name "{user_name_str}" already exists for this account. Please use a different name.'
            }
        else:
            return {
                'success': False, 
                'message': f'Failed to add recipient: {str(e)}'
            }

def delete_user_recipient(user_client, user_id: str, chat_id: str, user_name: str = None):
    """Delete a specific recipient by user_id, chat_id, and optionally user_name."""
    if user_name:
        # Delete specific recipient by all three fields
        user_client.table('telegram_recipients').delete().eq('user_id', user_id).eq('chat_id', chat_id).eq('user_name', user_name).execute()
    else:
        # Legacy behavior: delete by user_id and chat_id only (removes all matching records)
        user_client.table('telegram_recipients').delete().eq('user_id', user_id).eq('chat_id', chat_id).execute()


# --- Admin helpers ---
def admin_get_all_users():
    sb_admin = get_supabase_client(service_role=True)
    resp = sb_admin.table('profiles').select('id, email').order('email').execute()
    return resp.data or []

def admin_get_user_details(user_id: str):
    sb_admin = get_supabase_client(service_role=True)
    profile = sb_admin.table('profiles').select('id, email').eq('id', user_id).single().execute().data
    scrips = sb_admin.table('monitored_scrips').select('bse_code, company_name').eq('user_id', user_id).execute().data or []
    recipients = sb_admin.table('telegram_recipients').select('chat_id, user_name').eq('user_id', user_id).execute().data or []
    return {
        'id': profile['id'],
        'email': profile.get('email', '') or '',
        'scrips': scrips,
        'recipients': recipients,
    }

def admin_add_scrip_for_user(user_id: str, bse_code: str, company_name: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('monitored_scrips').insert({'user_id': user_id, 'bse_code': bse_code, 'company_name': company_name}).execute()

def admin_delete_scrip_for_user(user_id: str, bse_code: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('monitored_scrips').delete().eq('user_id', user_id).eq('bse_code', bse_code).execute()

def admin_add_recipient_for_user(user_id: str, chat_id: str, user_name: str = None):
    """Admin function to add a recipient with user_name to any user."""
    sb_admin = get_supabase_client(service_role=True)
    chat_id_str = str(chat_id).strip()
    user_name_str = str(user_name).strip() if user_name else f'Admin_{user_id[:8]}'
    
    # Check if exact triplet already exists
    existing = sb_admin.table('telegram_recipients').select('user_id').eq('user_id', user_id).eq('chat_id', chat_id_str).eq('user_name', user_name_str).limit(1).execute()
    if existing.data:
        return
    
    # Insert with user_name
    sb_admin.table('telegram_recipients').insert({
        'user_id': user_id, 
        'chat_id': chat_id_str, 
        'user_name': user_name_str
    }).execute()

def admin_delete_recipient_for_user(user_id: str, chat_id: str, user_name: str = None):
    """Admin function to delete a specific recipient."""
    sb_admin = get_supabase_client(service_role=True)
    if user_name:
        # Delete specific recipient by all three fields
        sb_admin.table('telegram_recipients').delete().eq('user_id', user_id).eq('chat_id', chat_id).eq('user_name', user_name).execute()
    else:
        # Legacy behavior: delete by user_id and chat_id only
        sb_admin.table('telegram_recipients').delete().eq('user_id', user_id).eq('chat_id', chat_id).execute()

# --- Telegram Helper Functions ---
def send_telegram_message(chat_id: str, message: str):
    """
    Sends a message to a Telegram chat using the bot API.
    Returns True if successful, False otherwise.
    """
    import requests
    import json

    if not TELEGRAM_BOT_TOKEN:
        print("❌ Telegram bot token missing. Set TELEGRAM_BOT_TOKEN in your .env and restart the app.")
        return False
    
    try:
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown'
        }
        
        response = requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('ok'):
                print(f"✅ Message sent successfully to Telegram {chat_id}")
                return True
            else:
                print(f"❌ Telegram API error: {result.get('description', 'Unknown error')}")
                return False
        else:
            print(f"❌ HTTP error {response.status_code}: {response.text}")
            if response.status_code == 404:
                print("Hint: 404 from Telegram often means an invalid bot token or malformed URL. Double-check TELEGRAM_BOT_TOKEN and ensure you started a chat with the bot.")
            return False
            
    except Exception as e:
        print(f"❌ Error sending Telegram message: {e}")
        return False

def send_telegram_message_with_user_name(chat_id: str, message: str, user_name: str = None):
    """
    Sends a message to a Telegram chat with user name personalization.
    Returns True if successful, False otherwise.
    """
    if user_name:
        # Add user name header to the message
        personalized_message = f"👤 {user_name}\n" + "─" * 20 + "\n" + message
    else:
        personalized_message = message
    
    return send_telegram_message(chat_id, personalized_message)

# --- Script Message Functions ---

# --- Hourly price/volume spike alerts ---
ALERTS_TABLE = 'daily_alerts_sent'

ALERTS_SQL_SCHEMA = """
-- Suggested schema to create in Supabase
create table if not exists public.daily_alerts_sent (
  user_id uuid not null,
  bse_code text not null,
  alert_date date not null,
  alert_type text not null,
  created_at timestamptz not null default now(),
  primary key (user_id, bse_code, alert_date, alert_type)
);
"""

def _has_sent_alert_today(user_client, user_id: str, bse_code: str, alert_type: str) -> bool:
    try:
        from datetime import date
        today = date.today().isoformat()
        resp = (
            user_client.table(ALERTS_TABLE)
            .select('user_id', count='exact')
            .eq('user_id', user_id)
            .eq('bse_code', str(bse_code))
            .eq('alert_date', today)
            .eq('alert_type', alert_type)
            .execute()
        )
        return (getattr(resp, 'count', 0) or 0) > 0
    except Exception:
        return False

def _record_alert_today(user_client, user_id: str, bse_code: str, alert_type: str):
    try:
        from datetime import date
        today = date.today().isoformat()
        user_client.table(ALERTS_TABLE).insert({
            'user_id': user_id,
            'bse_code': str(bse_code),
            'alert_date': today,
            'alert_type': alert_type,
        }).execute()
    except Exception:
        pass

def send_hourly_spike_alerts(user_client, user_id: str, monitored_scrips, telegram_recipients, price_threshold_pct: float = 5.0, volume_threshold_pct: float = 400.0) -> int:
    """Enhanced multi-threshold price spike alerts.

    Sends notifications at progressive thresholds: 5%, 10%, 15%, 20%
    Each threshold triggers once per direction per day.
    Volume spike alerts work as before (once per day).

    Returns number of messages sent.
    Requires a table 'daily_alerts_sent' with schema in ALERTS_SQL_SCHEMA.
    """
    # Import the multi-threshold tracker
    try:
        from multi_threshold_alerts import should_send_price_alert, mark_price_alert_sent
    except ImportError:
        # Fallback to original behavior if tracker not available
        def should_send_price_alert(*args, **kwargs): return (False, None, "tracker_unavailable")
        def mark_price_alert_sent(*args, **kwargs): pass

    messages_sent = 0
    if not monitored_scrips or not telegram_recipients:
        return 0

    # Only send during market hours
    is_open, _, _ = ist_market_window()
    if not is_open:
        return 0

    # Pre-resolve symbols once
    symbols = {}
    for s in monitored_scrips:
        bse_code = str(s['bse_code'])
        sym = bse_code_to_yahoo_symbol(bse_code)
        if sym:
            symbols[bse_code] = sym

    for s in monitored_scrips:
        bse_code = str(s['bse_code'])
        company_name = s.get('company_name') or bse_code
        sym = symbols.get(bse_code)
        if not sym:
            continue

        price_change_pct, volume_spike_pct, price, prev_close, today_vol, prev_vol = _get_price_change_and_volume(sym)

        # Handle price alerts with multi-threshold system
        price_alert_sent = False
        if price_change_pct is not None and abs(price_change_pct) >= price_threshold_pct:
            # Check if this threshold should trigger
            should_send, threshold_crossed, alert_type = should_send_price_alert(
                user_client, user_id, bse_code, price_change_pct
            )

            if should_send and threshold_crossed is not None:
                # Build enhanced price alert message
                def fmt(v):
                    try:
                        return f"{float(v):.2f}"
                    except Exception:
                        return "N/A"

                direction = "🔼" if price_change_pct > 0 else "🔻"
                sign = "+" if price_change_pct >= 0 else ""
                threshold_emoji = "🚨" if threshold_crossed >= 20 else "⚠️" if threshold_crossed >= 15 else "📈" if threshold_crossed >= 10 else "📊"

                parts = [
                    f"{threshold_emoji} Price Alert: {company_name} ({bse_code})",
                    f"Price: ₹{fmt(price)} ({direction} {sign}{fmt(price_change_pct)}%) vs prev close ₹{fmt(prev_close)}",
                    f"Threshold Crossed: {threshold_crossed}%"
                ]

                text = "\n".join(parts)

                # Send to all recipients
                for rec in telegram_recipients:
                    try:
                        user_name = rec.get('user_name', 'User')
                        send_telegram_message_with_user_name(rec['chat_id'], text, user_name)
                        messages_sent += 1
                    except Exception:
                        pass

                # Mark this threshold as sent
                mark_price_alert_sent(user_client, user_id, bse_code, price_change_pct, threshold_crossed)
                price_alert_sent = True

        # Handle volume spike alerts (original logic)
        volume_alert_sent = False
        if volume_spike_pct is not None and volume_spike_pct >= volume_threshold_pct:
            trigger = 'volume_spike'

            # Check if volume alert already sent today (original logic)
            if not _has_sent_alert_today(user_client, user_id, bse_code, trigger):
                # Build volume alert message
                def fmt(v):
                    try:
                        return f"{float(v):.2f}"
                    except Exception:
                        return "N/A"

                # Include price info if available
                price_info = ""
                if price_change_pct is not None:
                    arrow = '🔼' if price_change_pct > 0 else ('🔻' if price_change_pct < 0 else '➖')
                    sign = '+' if price_change_pct >= 0 else ''
                    price_info = f"Price: ₹{fmt(price)} ({arrow} {sign}{fmt(price_change_pct)}%) vs prev close ₹{fmt(prev_close)}\n"

                parts = [
                    f"📊 Volume Alert: {company_name} ({bse_code})",
                    price_info,
                    f"Volume spike: {fmt(volume_spike_pct)}% vs yesterday (today {fmt(today_vol)}, prev {fmt(prev_vol)})"
                ]

                # Remove empty lines
                parts = [p for p in parts if p.strip()]
                text = "\n".join(parts)

                # Send to all recipients
                for rec in telegram_recipients:
                    try:
                        user_name = rec.get('user_name', 'User')
                        send_telegram_message_with_user_name(rec['chat_id'], text, user_name)
                        messages_sent += 1
                    except Exception:
                        pass

                _record_alert_today(user_client, user_id, bse_code, trigger)
                volume_alert_sent = True

    return messages_sent


# --- BSE Announcements Integration ---
BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
PDF_BASE_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
BSE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    'Referer': 'https://www.bseindia.com/'
}

IST_OFFSET = timedelta(hours=5, minutes=30)
IST_TZ = timezone(IST_OFFSET, name="IST")

def ist_now():
    return datetime.now(IST_TZ)

def ist_market_window(now=None):
    """Return tuple (is_market_hours, open_dt, close_dt) in IST.
    Market hours: 09:00 to 15:30 inclusive.
    """
    if now is None:
        now = ist_now()
    open_dt = now.replace(hour=9, minute=16, second=0, microsecond=0)
    close_dt = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return (open_dt <= now <= close_dt), open_dt, close_dt

def ist_now():
    # Return timezone-aware IST datetime
    return datetime.now(IST_TZ)

def db_seen_announcement_exists(user_client, user_id: str, news_id: str) -> bool:
    """Check if announcement already recorded.
    Enhanced with better duplicate prevention and logging.
    """
    try:
        # First check user-specific record
        resp = (
            user_client
            .table('seen_announcements')
            .select('news_id, created_at', count='exact')
            .eq('news_id', news_id)
            .eq('user_id', user_id)
            .execute()
        )
        
        if (getattr(resp, 'count', 0) or 0) > 0:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"BSE DUPLICATE PREVENTION: Found existing announcement {news_id} for user {user_id[:8]}")
            return True
            
        # Additional check: look for recent announcements (last 10 minutes) to prevent race conditions
        from datetime import datetime, timedelta
        ten_minutes_ago = (datetime.now() - timedelta(minutes=10)).isoformat()
        
        recent_resp = (
            user_client
            .table('seen_announcements')
            .select('news_id, created_at', count='exact')
            .eq('news_id', news_id)
            .eq('user_id', user_id)
            .gte('created_at', ten_minutes_ago)
            .execute()
        )
        
        if (getattr(recent_resp, 'count', 0) or 0) > 0:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"BSE DUPLICATE PREVENTION: Found recent announcement {news_id} for user {user_id[:8]} (within 10 min)")
            return True
            
        return False
        
    except Exception as e:
        msg = str(e).lower()
        # Fallback when user_id column is missing: de-dup globally by news_id
        if 'user_id' in msg and ('column' in msg or 'does not exist' in msg):
            try:
                resp2 = (
                    user_client
                    .table('seen_announcements')
                    .select('news_id', count='exact')
                    .eq('news_id', news_id)
                    .execute()
                )
                exists = (getattr(resp2, 'count', 0) or 0) > 0
                if exists and os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"BSE DUPLICATE PREVENTION: Found global announcement {news_id} (fallback check)")
                return exists
            except Exception:
                return False
        # Otherwise do not block sending
        try:
            print(f"BSE DUPLICATE PREVENTION: Lookup failed for {news_id}, treating as new: {e}")
        except Exception:
            pass
        return False

from typing import Optional

def db_save_seen_announcement(user_client, user_id: str, news_id: str, scrip_code: str, headline: str, pdf_name: str, ann_dt_iso: str, caption: str, category: Optional[str] = None):
    """Save seen announcement with enhanced duplicate prevention and logging."""
    payload = {
        'user_id': user_id,
        'news_id': news_id,
        'scrip_code': scrip_code,
        'headline': headline,
        'pdf_name': pdf_name,
        'ann_date': ann_dt_iso,
        'caption': caption,
    }
    
    # Try insert with category first
    if category is not None:
        payload_with_cat = dict(payload)
        payload_with_cat['category'] = category
    else:
        payload_with_cat = payload
        
    try:
        # Double-check before saving to prevent race conditions
        if db_seen_announcement_exists(user_client, user_id, news_id):
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"BSE DUPLICATE PREVENTION: Skipping save - announcement {news_id} already exists for user {user_id[:8]}")
            return
            
        user_client.table('seen_announcements').insert(payload_with_cat).execute()
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"BSE DUPLICATE PREVENTION: Saved announcement {news_id} for user {user_id[:8]} - {headline[:50]}...")
        return
        
    except Exception as e:
        msg = str(e).lower()
        
        # Handle duplicate key errors (expected for race conditions)
        if any(dup_keyword in msg for dup_keyword in ['duplicate', 'unique', 'constraint', 'already exists']):
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"BSE DUPLICATE PREVENTION: Duplicate key prevented for {news_id} - user {user_id[:8]} (expected behavior)")
            return
            
        # Retry without category if the column doesn't exist
        if 'category' in msg and ('column' in msg or 'does not exist' in msg):
            try:
                # Double-check again before retry
                if db_seen_announcement_exists(user_client, user_id, news_id):
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"BSE DUPLICATE PREVENTION: Skipping retry save - announcement {news_id} already exists for user {user_id[:8]}")
                    return
                    
                user_client.table('seen_announcements').insert(payload).execute()
                
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"BSE DUPLICATE PREVENTION: Saved announcement {news_id} for user {user_id[:8]} (without category)")
                return
            except Exception as retry_error:
                retry_msg = str(retry_error).lower()
                if any(dup_keyword in retry_msg for dup_keyword in ['duplicate', 'unique', 'constraint', 'already exists']):
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"BSE DUPLICATE PREVENTION: Duplicate key prevented on retry for {news_id} - user {user_id[:8]}")
                    return
                    
        # Log unexpected errors but don't block the process
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"BSE DUPLICATE PREVENTION: Unexpected error saving {news_id} for user {user_id[:8]}: {e}")
        return

ALLOWED_ANNOUNCEMENT_CATEGORIES = {
    'financials',
    'rating',
    'investor_presentation',
    'Board Meeting',
    'Happening',
}

def classify_bse_headline(headline: str):
    """Return one of the allowed categories or None if it should be ignored.
    Heuristics based on keywords in the headline.
    """
    if not headline:
        return None
    h = headline.lower()

    # Investor presentation
    if "investor presentation" in h:
        return 'investor_presentation'

    # Financials (strict rule)
    if "unaudited" in h and ("financial" in h or "result" in h or "results" in h):
        return 'financials'

    # Rating
    if ("rating" in h) or ("credit" in h):
        return 'rating'

    # Board Meeting
    if "board meeting" in h or "meeting of the board of directors" in h:
        return 'Board Meeting'

    # Company Update / Regulatory announcements (Regulation 30, etc.)
    if (
        "regulation 30" in h or "regulation 31" in h or "regulation 33" in h or "regulation 34" in h
        or "regulation 39" in h or "regulation 46" in h or "regulation 47" in h
        or "lodr" in h or "newspaper publication" in h or "newspaper advertisement" in h
        or "shareholding pattern" in h or "share purchase" in h or "share acquisition" in h
        or "financial results" in h or "audited" in h or "unaudited" in h
        or "company update" in h or "corporate action" in h or "compliance" in h
    ):
        return 'Company Update'

    # Happening (events like LOI, order, award)
    if (
        "letter of intent" in h or " loi " in h or h.startswith("loi ") or "(loi)" in h
        or "award" in h or "awarded" in h or "award of" in h or "letter of award" in h or "receipt of letter of award" in h
        or "order received" in h or "received order" in h or "purchase order" in h or "work order" in h or "contract" in h
        or "thermal power project" in h or "power project" in h
    ):
        return 'Happening'

    return None

def fetch_bse_announcements_for_scrip(scrip_code: str, since_dt, allowed_categories: list[str] | None = None) -> list[dict]:
    import requests
    results = []
    try:
        from_date_str = (ist_now() - timedelta(days=7)).strftime('%Y%m%d')
        to_date_str = ist_now().strftime('%Y%m%d')
        params = {
            'strCat': '-1', 'strPrevDate': from_date_str, 'strToDate': to_date_str,
            'strScrip': scrip_code, 'strSearch': 'P', 'strType': 'C'
        }
        r = requests.get(BSE_API_URL, headers=BSE_HEADERS, params=params, timeout=30)
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            try:
                print(f"BSE fetch {scrip_code}: HTTP {r.status_code} url={r.url}")
            except Exception:
                pass
        
        # Improved response handling
        if r.status_code != 200:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"BSE fetch {scrip_code}: HTTP error {r.status_code} - {r.text[:200]}")
            return results
        
        try:
            data = r.json()
        except ValueError as json_err:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"BSE fetch {scrip_code}: JSON parsing failed - {json_err}")
            return results
            
        table = data.get('Table') or []
        if not table and os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"BSE fetch {scrip_code}: empty table. Response keys: {list(data.keys())[:5]}")
        # Fallback: retry with no strSearch filter if empty
        if not table:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"BSE fetch {scrip_code}: empty table. Retrying with relaxed params...")
            params2 = {
                'strCat': '-1', 'strPrevDate': from_date_str, 'strToDate': to_date_str,
                'strScrip': scrip_code, 'strSearch': '', 'strType': 'C'
            }
            try:
                r2 = requests.get(BSE_API_URL, headers=BSE_HEADERS, params=params2, timeout=30)
                if r2.status_code == 200:
                    try:
                        data2 = r2.json()
                        table = data2.get('Table') or []
                    except ValueError as json_err2:
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            print(f"BSE fetch fallback {scrip_code}: JSON parsing failed - {json_err2}")
                        table = []
                else:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"BSE fetch fallback {scrip_code}: HTTP error {r2.status_code}")
                    table = []
                    
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"BSE fetch fallback {scrip_code}: HTTP {r2.status_code} items={len(table)}")
            except Exception as retry_err:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"BSE fetch fallback {scrip_code}: Request failed - {retry_err}")
                table = []
        for ann in table:
            news_id = ann.get('NEWSID')
            pdf_name = ann.get('ATTACHMENTNAME')
            if not news_id or not pdf_name:
                continue
            ann_date_str = ann.get('NEWS_DT') or ann.get('DissemDT')
            if not ann_date_str:
                continue
            # Parse announcement date (several formats observed)
            dt_parsed = None
            formats = (
                '%d %b %Y %I:%M:%S %p',  # e.g., 08 Nov 2024 05:25:00 PM
                '%d %b %Y %I:%M %p',     # e.g., 08 Nov 2024 05:25 PM
                '%d %b %Y %H:%M:%S',     # e.g., 08 Nov 2024 17:25:00
                '%d %b %Y %H:%M',        # e.g., 08 Nov 2024 17:25
                '%Y-%m-%d %H:%M:%S',     # e.g., 2024-11-08 17:25:00
                '%Y-%m-%d %H:%M',        # e.g., 2024-11-08 17:25
                '%Y-%m-%dT%H:%M:%S.%f',  # e.g., 2024-11-08T17:25:00.000
                '%Y-%m-%dT%H:%M:%S',     # e.g., 2024-11-08T17:25:00
            )
            for fmt in formats:
                try:
                    dt_parsed = datetime.strptime(ann_date_str.strip(), fmt)
                    break
                except Exception:
                    continue
            if not dt_parsed:
                # Try dateutil as a robust fallback (day-first common in BSE)
                try:
                    from dateutil import parser as _dtparser  # provided via pandas dependency
                    dt_parsed = _dtparser.parse(ann_date_str, dayfirst=True)
                except Exception:
                    # Try ISO split
                    try:
                        dt_parsed = datetime.fromisoformat(ann_date_str.split('.')[0])
                    except Exception:
                        continue
            # Localize to IST if naive
            if dt_parsed.tzinfo is None:
                ann_dt = dt_parsed.replace(tzinfo=IST_TZ)
            else:
                ann_dt = dt_parsed.astimezone(IST_TZ)
            
            # Ensure since_dt has timezone info for comparison
            if since_dt.tzinfo is None:
                since_dt = since_dt.replace(tzinfo=IST_TZ)
            elif since_dt.tzinfo != IST_TZ:
                since_dt = since_dt.astimezone(IST_TZ)
                
            if ann_dt < since_dt:
                continue
            headline = ann.get('NEWSSUB') or ann.get('HEADLINE', 'N/A')
            category = classify_bse_headline(headline)
            if not category:
                continue  # ignore announcements outside our categories
            if allowed_categories is not None and category not in allowed_categories:
                continue  # filtered out by user preferences
            results.append({
                'news_id': news_id,
                'scrip_code': scrip_code,
                'headline': headline,
                'pdf_name': pdf_name,
                'ann_dt': ann_dt,
                'category': category,
            })
    except Exception as e:
        error_msg = f"BSE fetch error {scrip_code}: {str(e)}"
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(error_msg)
        api_logger.warning(error_msg)
        pass
    return results

def send_bse_announcements_consolidated(user_client, user_id: str, monitored_scrips, telegram_recipients, hours_back: int = 24) -> int:
    # Import the PERSISTENT deduplication tracker (survives worker restarts)
    try:
        from bse_persistent_tracker import is_persistent_bse_duplicate, mark_persistent_bse_sent
        def is_bse_duplicate(*args, **kwargs):
            return is_persistent_bse_duplicate(*args, **kwargs)
        def mark_bse_sent(*args, **kwargs):
            return mark_persistent_bse_sent(*args, **kwargs)
    except ImportError:
        # Fallback to original tracker if persistent tracker not available
        try:
            from bse_dedup_tracker import is_bse_duplicate, mark_bse_sent
        except ImportError:
            # Final fallback if no tracker available
            def is_bse_duplicate(*args, **kwargs): return (False, "tracker_unavailable")
            def mark_bse_sent(*args, **kwargs): pass

    # Build a lookup from bse_code to company_name for friendly messages
    code_to_name = {}
    try:
        for s in monitored_scrips:
            code_to_name[str(s.get('bse_code'))] = s.get('company_name') or str(s.get('bse_code'))
    except Exception:
        pass
    import requests
    import pandas as pd
    messages_sent = 0
    since_dt = ist_now() - timedelta(hours=hours_back)

    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"BSE PROCESSING: Starting for user {user_id[:8]}, {len(monitored_scrips)} scrips, {hours_back}h back")

    # Track processed announcements in this run to prevent duplicates within the same execution
    processed_in_this_run = set()

    # Fetch announcements for all scrips
    all_new = []
    for scrip in monitored_scrips:
        scrip_code = scrip['bse_code']
        # Apply per-user category preferences
        allowed = get_user_category_prefs(user_client, user_id)
        ann = fetch_bse_announcements_for_scrip(scrip_code, since_dt, allowed_categories=allowed)
        for item in ann:
            news_id = item['news_id']
            headline = item.get('headline', '')
            company_name = code_to_name.get(str(scrip_code), str(scrip_code))
            ann_dt = item.get('ann_dt', '')

            # Skip if already processed in this run
            if news_id in processed_in_this_run:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"BSE DUPLICATE PREVENTION: Skipping {news_id} - already processed in this run")
                continue

            # ENHANCED: Check with memory-based deduplication tracker first (PRIMARY CHECK)
            is_dup, dup_reason = is_bse_duplicate(user_id, news_id, headline, company_name, ann_dt)
            if is_dup:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"🚫 BSE ENHANCED DUPLICATE: {news_id} - {dup_reason}")
                continue

            # Only proceed if not a duplicate according to enhanced tracker
            all_new.append(item)
            processed_in_this_run.add(news_id)

            # Mark as sent in the enhanced tracker IMMEDIATELY
            mark_bse_sent(user_id, news_id, headline, company_name, ann_dt)

            # Also mark in database as backup (but don't rely on this for duplicate prevention)
            try:
                category = item.get('category', '')
                db_save_seen_announcement(user_client, user_id, news_id, scrip_code, headline,
                                        item.get('pdf_name', ''), ann_dt, '', category)
            except Exception as e:
                # Don't fail if database save fails - enhanced tracker is primary
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"BSE WARNING: Database save failed for {news_id}: {e}")

            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"BSE PROCESSING: Added new announcement {news_id} for processing")

    recipients_count = len(telegram_recipients)
    ann_count = len(all_new)
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        try:
            print(f"BSE: user={user_id} new_items={ann_count} recipients={recipients_count}")
        except Exception:
            pass

    if not all_new:
        return 0

    # Group items per scrip for nicer formatting
    from collections import defaultdict
    by_scrip = defaultdict(list)
    for item in sorted(all_new, key=lambda x: x['ann_dt'], reverse=True):
        by_scrip[item['scrip_code']].append(item)

    # Prepare price and % change per scrip using Yahoo fallback
    symbol_map = {}
    price_info = {}
    try:
        company_df = pd.read_csv('indian_stock_tickers.csv')
        def get_symbol(bse_code):
            try:
                bse_code_int = int(bse_code)
                row = company_df[company_df['BSE Code'] == bse_code_int]
            except Exception:
                row = company_df[company_df['BSE Code'].astype(str) == str(bse_code)]
            if row.empty:
                return None
            sym = str(row.iloc[0].get('Yahoo Symbol', '')).strip()
            return sym or None

        # Compute for unique scrip codes in announcements
        from math import isfinite
        for scrip_code in set(str(k) for k in by_scrip.keys()):
            sym = get_symbol(scrip_code)
            if not sym:
                continue
            symbol_map[scrip_code] = sym
            # Use robust CMP vs previous close logic
            price, prev_close, _src = get_cmp_and_prev(sym)
            pct = None
            if price is not None and prev_close not in (None, 0):
                try:
                    pct = ((price - prev_close) / prev_close) * 100.0
                except Exception:
                    pct = None
            price_info[scrip_code] = (price, prev_close, pct)
    except Exception:
        pass

    # Build a consolidated message summary
    header = [
        "📰 BSE Announcements",
        f"🕐 {ist_now().strftime('%Y-%m-%d %H:%M:%S')} IST",
        "",
    ]
    lines = header[:]
    for scrip_code, items in by_scrip.items():
        company_name = code_to_name.get(str(scrip_code)) or str(scrip_code)
        price, prev_close, pct = price_info.get(str(scrip_code), (None, None, None))
        def fmt_price(val):
            try:
                return f"₹{float(val):.2f}"
            except Exception:
                return "N/A"
        change_str = ""
        if pct is not None:
            arrow = "🔼" if pct > 0 else ("🔻" if pct < 0 else "➖")
            sign = "+" if pct > 0 else ("" if pct == 0 else "")
            change_str = f" {arrow} ({sign}{pct:.2f}%)"
        price_line = f" — {fmt_price(price)}{change_str}" if price is not None else ""
        lines.append(f"• {company_name}{price_line}")
        for it in items[:5]:
            lines.append(f"  - {it['ann_dt'].strftime('%d-%m %H:%M')} — {it['headline']}")
        lines.append("")
    summary_text = "\n".join(lines).strip()

    # Send summary first with user names
    for rec in telegram_recipients:
        chat_id = rec['chat_id']
        user_name = rec.get('user_name', 'User')
        
        # Add user name header to summary
        personalized_summary = f"👤 {user_name}\n" + "─" * 20 + "\n" + summary_text
        
        from requests import post
        post(f"{TELEGRAM_API_URL}/sendMessage", json={'chat_id': chat_id, 'text': personalized_summary, 'parse_mode': 'HTML'}, timeout=10)
        messages_sent += 1

    # Send documents (PDFs) with price and % change in caption
    for item in all_new:
        friendly_name = code_to_name.get(str(item['scrip_code'])) or str(item['scrip_code'])
        price, prev_close, pct = price_info.get(str(item['scrip_code']), (None, None, None))
        def fmt_price(val):
            try:
                return f"₹{float(val):.2f}"
            except Exception:
                return "N/A"
        pct_str = ""
        if pct is not None:
            arrow = "🔼" if pct > 0 else ("🔻" if pct < 0 else "➖")
            sign = "+" if pct > 0 else ("" if pct == 0 else "")
            pct_str = f" ({arrow} {sign}{pct:.2f}%)"
        price_line = f"\nPrice: {fmt_price(price)}{pct_str}" if price is not None else ""
        # Include category in caption for clarity
        category_label = item.get('category') or ''
        if category_label:
            category_label = f"\nCategory: {category_label}"
        # 3M-ago price for financials
        extra_3m = ""
        if item.get('category') == 'financials' and sym:
            p3 = get_close_3m_ago(sym)
            if p3 is not None:
                extra_3m = f"\n3M ago: ₹{p3:,.2f}"
        caption = (
            f"Company: {friendly_name}\n"
            f"Announcement: {item['headline']}\n"
            f"Date: {item['ann_dt'].strftime('%d-%m-%Y %H:%M')} IST"
            f"{price_line}"
            f"{category_label}"
            f"{extra_3m}"
        )
        pdf_url = f"{PDF_BASE_URL}{item['pdf_name']}"
        try:
            resp = requests.get(pdf_url, headers=BSE_HEADERS, timeout=30)
            if resp.status_code == 200 and resp.content:
                # Check if this is a quarterly results document for AI analysis
                try:
                    from ai_service import is_quarterly_results_document, analyze_pdf_bytes_with_gemini, format_structured_telegram_message
                    
                    is_quarterly = is_quarterly_results_document(item.get('headline', ''), item.get('category', ''))
                    
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"AI: Checking {item['pdf_name']} - headline: '{item.get('headline', '')}', category: '{item.get('category', '')}', is_quarterly: {is_quarterly}")
                    
                    # ALWAYS run AI analysis for ALL announcements
                    try:
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            print(f"AI: Starting analysis for {item['pdf_name']} (category: {item.get('category', 'unknown')})...")
                        
                        analysis_result = analyze_pdf_bytes_with_gemini(
                            resp.content, 
                            item['pdf_name'], 
                            str(item['scrip_code'])
                        )
                        
                        if analysis_result:
                            if os.environ.get('BSE_VERBOSE', '0') == '1':
                                print(f"AI: Analysis successful for {item['pdf_name']}, generating message...")
                            
                            # Send AI-analyzed message first
                            ai_message = format_structured_telegram_message(
                                analysis_result,
                                str(item['scrip_code']),
                                item['headline'],
                                item['ann_dt'],
                                is_quarterly  # Pass quarterly flag for special formatting
                            )
                            
                            if os.environ.get('BSE_VERBOSE', '0') == '1':
                                print(f"AI: Sending summary to {len(telegram_recipients)} recipients...")
                                print(f"AI: Message preview: {ai_message[:200]}...")
                            
                            messages_sent_count = 0
                            for rec in telegram_recipients:
                                try:
                                    user_name = rec.get('user_name', 'User')
                                    # Add user name header to AI message
                                    personalized_ai_message = f"👤 {user_name}\n" + "─" * 20 + "\n" + ai_message
                                    
                                    response = requests.post(
                                        f"{TELEGRAM_API_URL}/sendMessage", 
                                        json={
                                            'chat_id': rec['chat_id'], 
                                            'text': personalized_ai_message, 
                                            'parse_mode': 'HTML'
                                        }, 
                                        timeout=10
                                    )
                                    if response.status_code == 200:
                                        result = response.json()
                                        if result.get('ok'):
                                            messages_sent_count += 1
                                            if os.environ.get('BSE_VERBOSE', '0') == '1':
                                                print(f"AI: Successfully sent summary to {user_name} at {rec['chat_id']}")
                                        else:
                                            print(f"AI: Telegram API error for {user_name} at {rec['chat_id']}: {result.get('description', 'Unknown error')}")
                                    else:
                                        print(f"AI: HTTP error {response.status_code} for {user_name} at {rec['chat_id']}: {response.text}")
                                except Exception as send_error:
                                    print(f"AI: Error sending to {rec.get('user_name', 'User')} at {rec['chat_id']}: {send_error}")
                            
                            if os.environ.get('BSE_VERBOSE', '0') == '1':
                                print(f"AI: Summary sent to {messages_sent_count}/{len(telegram_recipients)} recipients")
                        else:
                            if os.environ.get('BSE_VERBOSE', '0') == '1':
                                print(f"AI: Analysis returned no results for {item['pdf_name']}")
                    except Exception as ai_error:
                        # If AI analysis fails, continue with regular PDF sending
                        print(f"AI: Analysis failed for {item['pdf_name']}: {ai_error}")
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            import traceback
                            traceback.print_exc()
                except ImportError as import_error:
                    # AI service not available, continue with regular processing
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"AI: Import error - {import_error}")
                except Exception as outer_error:
                    # Unexpected error in AI processing
                    print(f"AI: Unexpected error processing {item['pdf_name']}: {outer_error}")
                
                # Send PDF document (always, regardless of AI analysis) with user name
                for rec in telegram_recipients:
                    user_name = rec.get('user_name', 'User')
                    
                    # Add user name to caption
                    personalized_caption = f"👤 {user_name}\n" + "─" * 20 + "\n" + caption
                    
                    files = {"document": (item['pdf_name'], resp.content, "application/pdf")}
                    data = {"chat_id": rec['chat_id'], "caption": personalized_caption, "parse_mode": "HTML"}
                    requests.post(f"{TELEGRAM_API_URL}/sendDocument", data=data, files=files, timeout=45)
                
                # Record as seen for this user
                db_save_seen_announcement(user_client, user_id, item['news_id'], item['scrip_code'], item['headline'], item['pdf_name'], item['ann_dt'].isoformat(), caption, item.get('category'))
            else:
                # Could not fetch PDF, still mark as seen to avoid repeated attempts
                db_save_seen_announcement(user_client, user_id, item['news_id'], item['scrip_code'], item['headline'], item['pdf_name'], item['ann_dt'].isoformat(), caption, item.get('category'))
        except Exception:
            # On errors, we still mark as seen to limit retries (could adjust behavior)
            db_save_seen_announcement(user_client, user_id, item['news_id'], item['scrip_code'], item['headline'], item['pdf_name'], item['ann_dt'].isoformat(), caption, item.get('category'))

    # Final log line for Render logs
    try:
        total_documents = ann_count * recipients_count
        print(f"BSE: user={user_id} summary_messages={recipients_count} documents_sent={total_documents} (items={ann_count} x recipients={recipients_count})")
    except Exception:
        pass

    return messages_sent
def _get_price_change_and_volume(sym: str):
    """Return tuple (price_change_pct, volume_spike_pct, price, prev_close, today_vol, prev_vol)
    price_change_pct: percent change vs previous close
    volume_spike_pct: today's volume as a percentage of previous day's volume (e.g., 400 means 4x)
    """
    try:
        s_intraday = yahoo_chart_series_cached(sym, '1d', '1m')
        price = float(s_intraday.dropna().iloc[-1]) if s_intraday is not None and not s_intraday.empty else None
        # Daily OHLCV for last few days
        s_daily = yahoo_chart_series_cached(sym, '10d', '1d')
        prev_close = None
        today_vol = None
        prev_vol = None
        price_change_pct = None
        volume_spike_pct = None
        if s_daily is not None and not s_daily.empty:
            closes = s_daily.dropna()
            # We don't have volume in this series; fetch via direct chart API
            import requests
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=10d&interval=1d"
            r = requests.get(url, timeout=10)
            vols = None
            if r.status_code == 200:
                data = r.json()
                try:
                    vols = data['chart']['result'][0]['indicators']['quote'][0]['volume']
                except Exception:
                    vols = None
            if vols:
                vols = [v for v in vols if v is not None]
                if len(vols) >= 2:
                    prev_vol = float(vols[-2])
                    today_vol = float(vols[-1])
                    if prev_vol and prev_vol > 0:
                        volume_spike_pct = (today_vol / prev_vol) * 100.0
            if len(closes) >= 2:
                prev_close = float(closes.iloc[-2])
                if price is None:
                    try:
                        price = float(closes.iloc[-1])
                    except Exception:
                        price = None
                if price is not None and prev_close not in (None, 0):
                    price_change_pct = ((price - prev_close) / prev_close) * 100.0
        return price_change_pct, volume_spike_pct, price, prev_close, today_vol, prev_vol
    except Exception:
        return None, None, None, None, None, None

def send_script_messages_to_telegram(user_client, user_id: str, monitored_scrips, telegram_recipients):
    """
    Sends a single consolidated Telegram message with current price and moving averages
    for all monitored scrips. Uses batch requests to Yahoo Finance to reduce rate limits.
    Returns the number of messages sent (one per recipient).
    """
    import yfinance as yf
    import pandas as pd
    from datetime import datetime

    def safe_fmt(val):
        try:
            return f"₹{float(val):.2f}"
        except Exception:
            return "N/A"

    # Helper to safely pull a series from a yfinance.download DataFrame
    def get_series(df, symbol, field='Close'):
        if df is None or df.empty:
            return None
        try:
            if isinstance(df.columns, pd.MultiIndex):
                # Try group_by='ticker' layout first
                if (symbol, field) in df.columns:
                    return df[(symbol, field)].dropna()
                # Some versions may return the inverse
                if (field, symbol) in df.columns:
                    return df[(field, symbol)].dropna()
                return None
            else:
                if field in df.columns:
                    return df[field].dropna()
                return None
        except Exception:
            return None

    try:
        # Load the stock tickers CSV to get Yahoo Finance symbols
        company_df = pd.read_csv('indian_stock_tickers.csv')

        # Map BSE codes -> Yahoo symbols and keep order/context
        symbol_map = {}
        ordered_symbols = []
        for scrip in monitored_scrips:
            bse_code = scrip['bse_code']
            company_name = scrip['company_name']

            # Find symbol for BSE code
            try:
                bse_code_int = int(bse_code)
                ticker_match = company_df[company_df['BSE Code'] == bse_code_int]
            except (ValueError, TypeError):
                ticker_match = company_df[company_df['BSE Code'].astype(str) == str(bse_code)]

            if ticker_match.empty:
                print(f"Warning: No Yahoo Finance symbol found for BSE code {bse_code}")
                continue

            symbol = str(ticker_match.iloc[0]['Yahoo Symbol']).strip()
            if not symbol:
                print(f"Warning: Empty Yahoo Finance symbol for BSE code {bse_code}")
                continue

            if os.environ.get("YAHOO_VERBOSE", "0") == "1":
                print(f"Using Yahoo symbol: {symbol} for {company_name} ({bse_code})")
            symbol_map[symbol] = {'bse_code': bse_code, 'company_name': company_name}
            ordered_symbols.append(symbol)

        if not ordered_symbols:
            print("No valid Yahoo symbols found for monitored scrips.")
            return 0

        # Prepare session
        session = get_yahoo_session()

        # Chunk symbols in groups of 10
        def chunks(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i+size]

        # Batch current prices via Yahoo Chart API (prefer chart API to avoid Quote API 401)
        prices = {}
        for batch in chunks(ordered_symbols, 10):
            for sym in batch:
                s_intraday = yahoo_chart_series_cached(sym, '1d', '1m')
                if s_intraday is not None and not s_intraday.empty:
                    prices[sym] = s_intraday.iloc[-1]
                else:
                    s_daily = yahoo_chart_series_cached(sym, '5d', '1d')
                    if s_daily is not None and not s_daily.empty:
                        prices[sym] = s_daily.iloc[-1]

        # Build consolidated message
        lines = []
        lines.append("📊 Market Update")
        lines.append(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        failed_symbols = []

        for symbol in ordered_symbols:
            meta = symbol_map[symbol]
            bse_code = meta['bse_code']
            company_name = meta['company_name']

            # Current price with robust logic
            cmp_price, prev_close, _src = get_cmp_and_prev(symbol)
            current_price = cmp_price if cmp_price is not None else 'N/A'

            # Calculate percentage change
            pct_change = None
            change_str = ""
            if cmp_price is not None and prev_close is not None and prev_close != 0:
                try:
                    pct_change = ((cmp_price - prev_close) / prev_close) * 100.0
                    arrow = "🔼" if pct_change > 0 else ("🔻" if pct_change < 0 else "➖")
                    sign = "+" if pct_change > 0 else ""
                    change_str = f" {arrow} ({sign}{pct_change:.2f}%)"
                except Exception:
                    pass

            # Moving averages from daily history via chart API (cached)
            ma_50 = 'N/A'
            ma_200 = 'N/A'
            s_hist = yahoo_chart_series_cached(symbol, '1y', '1d')
            if s_hist is not None and not s_hist.empty:
                closes = s_hist.dropna()
                if len(closes) >= 50:
                    ma_50 = closes.tail(50).mean()
                if len(closes) >= 200:
                    ma_200 = closes.tail(200).mean()

            if current_price == 'N/A' and ma_50 == 'N/A' and ma_200 == 'N/A':
                failed_symbols.append(f"{company_name} ({symbol})")

            # Append section for this symbol
            lines.append(f"• {company_name} ({bse_code})")
            # Color indicator logic
            indicator = '🟢'
            try:
                if ma_200 != 'N/A' and current_price != 'N/A' and float(current_price) < float(ma_200):
                    indicator = '🔴'
                elif ma_50 != 'N/A' and current_price != 'N/A' and float(current_price) < float(ma_50):
                    indicator = '🟠'
            except Exception:
                pass

            lines.append(f"  - Price: {safe_fmt(current_price)}{change_str} {indicator}")
            lines.append(f"  - MA50: {safe_fmt(ma_50)} | MA200: {safe_fmt(ma_200)}")
            if prev_close is not None:
                lines.append(f"  - Previous Close: {safe_fmt(prev_close)}")
            lines.append("")

        if failed_symbols:
            lines.append("⚠️ Could not fetch data for: " + ", ".join(failed_symbols))

        consolidated_message = "\n".join(lines).strip()

        # Send one message per recipient with user name
        messages_sent = 0
        for recipient in telegram_recipients:
            chat_id = recipient['chat_id']
            user_name = recipient.get('user_name', 'User')
            try:
                if send_telegram_message_with_user_name(chat_id, consolidated_message, user_name):
                    messages_sent += 1
                else:
                    print(f"❌ Failed to send consolidated message to {user_name} at Telegram {chat_id}")
            except Exception as e:
                print(f"❌ Error sending consolidated message to {user_name} at Telegram {chat_id}: {e}")

        return messages_sent

    except Exception as e:
        print(f"Error in send_script_messages_to_telegram: {e}")
        raise e

