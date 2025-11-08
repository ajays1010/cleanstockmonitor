import os
from dotenv import load_dotenv
from functools import wraps
import sys
load_dotenv()

from flask import Flask, request, render_template, redirect, url_for, session, flash, jsonify
# from supabase import create_client  # not used directly
import pandas as pd
import database as db
from firebase_admin import auth
from admin import admin_bp
from enhanced_bse_endpoint import enhanced_bse_bp
import uuid
from sentiment_analyzer import get_sentiment_analysis_for_stock, create_sentiment_visualizations
from logging_config import github_logger
from multi_threshold_alerts import get_alert_tracking_stats, add_alert_endpoints
import logging
import traceback

# Reduce httpx logging noise from Supabase
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("supabase").setLevel(logging.WARNING)
import atexit
import gc
import threading
import weakref
from contextlib import contextmanager
import psutil
from functools import lru_cache
import time
from typing import List, Dict

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "a-super-secret-key-for-local-testing")
app.register_blueprint(admin_bp)
app.register_blueprint(enhanced_bse_bp)

# Global variables for optimization
_connection_pool = {}
_rss_memory_tracker = weakref.WeakSet()

# Initialize logging
github_logger.log_app_start()

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded environment variables from: {env_path}")
    else:
        print(f"No .env file found at: {env_path}")
except ImportError:
    print("python-dotenv not available, trying to load .env manually...")
    # Manual .env loading as fallback
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip('"\'')
                    os.environ[key] = value
        print(f"Manually loaded environment variables from: {env_path}")

# Error handling decorator
def log_errors(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            github_logger.log_error(e, f"Route: {request.endpoint}")
            raise
    return decorated_function

# Global error handlers
@app.errorhandler(404)
def not_found_error(error):
    return {'error': 'Not found', 'message': 'The requested URL was not found on the server.'}, 404

@app.errorhandler(500)
def internal_error(error):
    github_logger.log_error(error, "Internal Server Error")
    return {'error': 'Internal server error', 'timestamp': str(error)}, 500

@app.errorhandler(Exception)
def handle_exception(e):
    # Don't log 404 errors as exceptions
    if hasattr(e, 'code') and e.code == 404:
        return {'error': 'Not found', 'message': 'The requested URL was not found on the server.'}, 404
    
    github_logger.log_error(e, "Unhandled Exception")
    return {'error': 'Application error', 'details': str(e)}, 500

# Log memory usage periodically and push logs to GitHub
def cleanup_and_log():
    github_logger.log_memory_usage()
    github_logger.push_logs_to_github()

atexit.register(cleanup_and_log)

# Ensure Firebase Admin SDK is initialized when the app starts (works under Gunicorn too)
db.initialize_firebase()

# Database Connection Pooling Class
class DatabaseConnectionPool:
    """Simple connection pool for Supabase clients"""
    
    def __init__(self, max_connections=5):
        self.max_connections = max_connections
        self.connections = []
        self.in_use = set()
        self.lock = threading.Lock()
    
    def get_connection(self, service_role=False):
        """Get a connection from the pool"""
        with self.lock:
            # Try to reuse existing connection
            for i, conn in enumerate(self.connections):
                if i not in self.in_use and conn.get('service_role') == service_role:
                    self.in_use.add(i)  # Use index instead of dict
                    return conn.get('client')
            
            # Create new connection if under limit
            if len(self.connections) < self.max_connections:
                try:
                    client = db.get_supabase_client(service_role=service_role)
                    conn = {'client': client, 'service_role': service_role, 'created': time.time()}
                    self.connections.append(conn)
                    conn_index = len(self.connections) - 1
                    self.in_use.add(conn_index)  # Use index instead of dict
                    return client
                except:
                    pass
            
            # Fallback to direct creation
            return db.get_supabase_client(service_role=service_role)
    
    def return_connection(self, client):
        """Return connection to pool"""
        with self.lock:
            for i, conn in enumerate(self.connections):
                if conn.get('client') == client and i in self.in_use:
                    self.in_use.remove(i)
                    break
    
    def cleanup_old_connections(self):
        """Remove old connections (older than 1 hour)"""
        with self.lock:
            current_time = time.time()
            new_connections = []
            new_in_use = set()
            
            for i, conn in enumerate(self.connections):
                # Keep connection if it's recent AND not in use, OR if it's currently in use
                if (current_time - conn.get('created', 0) < 3600 and i not in self.in_use) or i in self.in_use:
                    new_index = len(new_connections)
                    new_connections.append(conn)
                    if i in self.in_use:
                        new_in_use.add(new_index)
            
            self.connections = new_connections
            self.in_use = new_in_use

# Initialize connection pool
_db_pool = DatabaseConnectionPool()


# Add multi-threshold alert endpoints
add_alert_endpoints(app)

# Fast memory usage function
@lru_cache(maxsize=1)
def _get_memory_usage_fast():
    """Cached memory usage - updates every few seconds"""
    try:
        process = psutil.Process(os.getpid())
        return round(process.memory_info().rss / (1024**2), 1)
    except:
        return 0

# Clear cache every 30 seconds - OPTIMIZED for memory efficiency
def _clear_memory_cache():
    try:
        _get_memory_usage_fast.cache_clear()

        # Force garbage collection to free memory
        import gc
        gc.collect()

        # Log memory usage if verbose mode enabled
        if os.environ.get('MEMORY_VERBOSE', '0') == '1':
            current_memory = _get_memory_usage_fast()
            print(f"Memory cleanup completed: {current_memory} MB")

        # Re-enabled with safer threading approach and longer interval
        if not app.debug and not os.environ.get('DISABLE_AUTO_CLEANUP'):
            timer = threading.Timer(30.0, _clear_memory_cache)  # Increased from 10s to 30s
            timer.daemon = True  # Set as daemon thread to prevent blocking shutdown
            timer.start()
    except Exception as e:
        print(f"Memory cache clear error: {e}")

# Start the cache clearing timer only in production
if not os.environ.get('FLASK_DEBUG') == '1' and not os.environ.get('DISABLE_AUTO_CLEANUP'):
    try:
        _clear_memory_cache()
    except Exception as e:
        print(f"Failed to start cache clearing timer: {e}")

# RSS Memory Management Context Manager
@contextmanager
def rss_memory_manager():
    """Context manager for RSS operations with automatic cleanup"""
    initial_memory = _get_memory_usage_fast()
    rss_objects = []
    
    try:
        # Track RSS objects
        _rss_memory_tracker.add(rss_objects)
        yield rss_objects
    finally:
        # Force cleanup
        for obj in rss_objects:
            try:
                del obj
            except:
                pass
        rss_objects.clear()
        
        # Force garbage collection if memory increased significantly
        current_memory = _get_memory_usage_fast()
        if current_memory - initial_memory > 50:  # 50MB increase
            gc.collect()
            print(f"🧹 RSS Memory cleanup: {initial_memory}MB → {_get_memory_usage_fast()}MB")

# Memory-Efficient RSS News Function
def send_rss_news_optimized(sb, user_id, scrips, recipients):
    """Ultra memory-efficient RSS news processing with aggressive timeout protection"""
    messages_sent = 0
    initial_memory = _get_memory_usage_fast()
    
    print(f"🧠 RSS MEMORY: Starting with {initial_memory}MB for user {user_id[:8]}...")
    
    # Strict time tracking - much shorter limits
    import time
    start_time = time.time()
    max_total_time = 15  # Reduced from 20 to 15 seconds max for entire RSS processing
    
    try:
        # Process only FIRST 2 companies to minimize memory usage
        limited_scrips = scrips[:2]  # Limit to first 2 companies only
        
        print(f"📰 RSS MEMORY: Processing {len(limited_scrips)} companies (limited from {len(scrips)})")
        
        for i, scrip in enumerate(limited_scrips):
            company_name = scrip.get('company_name', '')
            if not company_name:
                continue
            
            print(f"📰 RSS MEMORY: Processing {i+1}/{len(limited_scrips)}: {company_name}")
            current_memory = _get_memory_usage_fast()
            
            # Very strict memory limit to prevent SIGKILL
            if current_memory > 300:  # Reduced from 350MB to 300MB
                print(f"🧠 MEMORY LIMIT REACHED: {current_memory}MB - skipping remaining companies")
                break
            
            try:
                # Check overall timeout
                elapsed_time = time.time() - start_time
                if elapsed_time > max_total_time:
                    print(f"⏰ OVERALL TIMEOUT: RSS processing exceeded {max_total_time}s - stopping")
                    break
                
                # Process this company with strict memory limits
                company_messages = process_single_company_memory_safe(
                    sb, user_id, company_name, recipients
                )
                messages_sent += company_messages
                
                # Aggressive cleanup after each company
                import gc
                for _ in range(3):  # Multiple cleanup cycles
                    gc.collect()
                
                after_memory = _get_memory_usage_fast()
                print(f"🧠 Memory: {current_memory}MB → {after_memory}MB (sent {company_messages} messages)")
                
                # Extra cleanup if memory increased at all
                if after_memory > current_memory + 20:  # Reduced from 30MB to 20MB
                    print(f"🧠 MEMORY INCREASE DETECTED - forcing extra cleanup")
                    for _ in range(5):  # Extra cleanup cycles
                        gc.collect()
                    time.sleep(0.5)  # Short wait
                    
                    # Check if cleanup worked
                    final_memory = _get_memory_usage_fast()
                    print(f"🧠 Cleanup result: {after_memory}MB → {final_memory}MB")
                    
                    # If still high, stop processing immediately
                    if final_memory > 350:  # Reduced threshold
                        print(f"🧠 MEMORY STILL HIGH: {final_memory}MB - stopping RSS processing")
                        break
                
            except Exception as e:
                print(f"❌ Error processing {company_name}: {e}")
                # Cleanup on error
                import gc
                for _ in range(3):
                    gc.collect()
                continue
    
    except Exception as e:
        print(f"❌ RSS MEMORY ERROR: {e}")
        # Don't print full traceback to save memory/time
        pass
    
    finally:
        # Final aggressive cleanup
        import gc
        for _ in range(5):  # More cleanup cycles
            gc.collect()
        
        final_memory = _get_memory_usage_fast()
        memory_diff = final_memory - initial_memory
        print(f"🧠 RSS MEMORY: Completed. {initial_memory}MB → {final_memory}MB (diff: {memory_diff:+.1f}MB)")
    
    return messages_sent

def process_single_company_memory_safe(sb, user_id: str, company_name: str, recipients: List[Dict]) -> int:
    """Process a single company with strict memory management and simple timeout protection"""
    messages_sent = 0
    
    try:
        # Simple time tracking for timeout - much shorter limits
        import time
        start_time = time.time()
        timeout_seconds = 8  # Reduced from 12 to 8 seconds
        
        # Check memory before starting
        pre_fetch_memory = _get_memory_usage_fast()
        if pre_fetch_memory > 350:  # Reduced from 400MB to 350MB
            print(f"🧠 MEMORY LIMIT: {pre_fetch_memory}MB - skipping RSS fetch for {company_name}")
            return 0
        
        print(f"🔍 RSS FETCH: Starting for {company_name} (memory: {pre_fetch_memory}MB, timeout: {timeout_seconds}s)")
        
        # Use lightweight RSS processing instead of heavy fetcher
        try:
            # Simple RSS fetch without heavy dependencies
            import requests
            import feedparser
            from urllib.parse import quote_plus
            
            # Check timeout before starting
            if time.time() - start_time > timeout_seconds:
                print(f"⏰ TIMEOUT: RSS processing for {company_name} exceeded {timeout_seconds}s before starting")
                return 0
            
            # Single search query to minimize processing
            search_query = f'"{company_name}" India stock news'
            search_encoded = quote_plus(search_query)
            url = f'https://news.google.com/rss/search?q={search_encoded}&hl=en&gl=IN&ceid=IN:en'
            
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)'}
            
            # Quick fetch with short timeout
            response = requests.get(url, headers=headers, timeout=5)
            
            # Check timeout after fetch
            if time.time() - start_time > timeout_seconds:
                print(f"⏰ TIMEOUT: RSS processing for {company_name} exceeded {timeout_seconds}s after fetch")
                return 0
            
            if response.status_code != 200:
                print(f"❌ RSS fetch failed for {company_name}: HTTP {response.status_code}")
                return 0
            
            # Parse feed quickly
            feed = feedparser.parse(response.content)
            
            # Check timeout after parsing
            if time.time() - start_time > timeout_seconds:
                print(f"⏰ TIMEOUT: RSS processing for {company_name} exceeded {timeout_seconds}s after parsing")
                return 0
            
            # Process only first 3 entries to save time
            articles = []
            for entry in feed.entries[:3]:
                # Check timeout during processing
                if time.time() - start_time > timeout_seconds:
                    print(f"⏰ TIMEOUT: RSS processing for {company_name} exceeded {timeout_seconds}s during article processing")
                    break
                
                title = entry.get('title', '').strip()
                link = entry.get('link', '').strip()
                pub_date = entry.get('published', '')
                
                if not title or len(title) < 15:
                    continue
                
                # Quick relevance check
                title_lower = title.lower()
                company_lower = company_name.lower()
                if company_lower not in title_lower:
                    continue
                
                # Extract source from Google News title format
                source = 'Google News'
                if ' - ' in title:
                    parts = title.split(' - ')
                    if len(parts) >= 2:
                        source = parts[-1].strip()
                        title = ' - '.join(parts[:-1]).strip()
                
                articles.append({
                    'title': title[:100],  # Truncate to save memory
                    'source': source,
                    'link': link,
                    'pubDate': pub_date,
                    'company': company_name
                })
            
            # Check memory after processing
            post_fetch_memory = _get_memory_usage_fast()
            print(f"🔍 RSS FETCH: Completed for {company_name} (memory: {pre_fetch_memory}MB → {post_fetch_memory}MB, articles: {len(articles)})")
            
            if not articles:
                return 0
            
            # Process recipients quickly
            for recipient in recipients:
                try:
                    # Check timeout before each recipient
                    if time.time() - start_time > timeout_seconds:
                        print(f"⏰ TIMEOUT: RSS processing for {company_name} exceeded {timeout_seconds}s during recipient processing")
                        break
                    
                    recipient_messages = process_single_recipient_memory_safe(
                        sb, user_id, company_name, articles, recipient
                    )
                    messages_sent += recipient_messages
                    
                except Exception as e:
                    print(f"❌ Error processing recipient {recipient.get('chat_id', 'unknown')}: {e}")
                    continue
            
            # Clear from memory
            articles.clear()
            del articles
            
        except requests.Timeout:
            print(f"⏰ TIMEOUT: RSS request timeout for {company_name}")
            return 0
        except Exception as e:
            print(f"❌ Error in RSS processing for {company_name}: {e}")
            return 0
        
    except Exception as e:
        print(f"❌ Error in process_single_company_memory_safe: {e}")
    finally:
        # Force garbage collection
        import gc
        gc.collect()
    
    return messages_sent

def process_single_recipient_memory_safe(sb, user_id: str, company_name: str, articles: List[Dict], recipient: Dict) -> int:
    """Process a single recipient with memory safety and duplicate checking"""
    try:
        recipient_id = recipient['chat_id']
        user_name = recipient.get('user_name', 'User')
        
        # Import duplicate checking functions
        from simple_rss_fix import (
            is_relevant_news, generate_rss_article_hash, 
            is_rss_duplicate_in_memory, is_rss_duplicate_in_database,
            mark_rss_sent_in_memory, record_rss_sent_in_database,
            format_clean_rss_message
        )
        
        # Filter articles for this recipient
        new_articles = []
        
        for article in articles:
            # FILTER 1: Relevance check
            if not is_relevant_news(article, company_name):
                continue
            
            # FILTER 2: Memory duplicate check
            article_hash = generate_rss_article_hash(article, company_name, recipient_id)
            if is_rss_duplicate_in_memory(article_hash):
                continue
            
            # FILTER 3: Database duplicate check
            if is_rss_duplicate_in_database(sb, article, company_name, user_id):
                mark_rss_sent_in_memory(article_hash)
                continue
            
            # Article is new and relevant
            new_articles.append(article)
        
        if not new_articles:
            return 0
        
        # Optimized logging - just show count and first article title
        if len(new_articles) > 0:
            first_title = new_articles[0].get('title', 'No title')[:50]
            if len(new_articles) == 1:
                print(f"📰 Sending to {user_name}: {first_title}...")
            else:
                print(f"📰 Sending to {user_name}: {len(new_articles)} articles (first: {first_title}...)")
        else:
            print(f"📰 No new articles for {user_name}")
        
        # Generate and send message
        telegram_message = format_clean_rss_message(company_name, new_articles)
        
        try:
            from database import send_telegram_message_with_user_name
            if send_telegram_message_with_user_name(recipient_id, telegram_message, user_name):
                # Mark articles as sent
                for article in new_articles:
                    article_hash = generate_rss_article_hash(article, company_name, recipient_id)
                    mark_rss_sent_in_memory(article_hash)
                    record_rss_sent_in_database(sb, article, company_name, user_id)
                
                return 1
            else:
                print(f"❌ Failed to send to {user_name}")
                return 0
                
        except Exception as e:
            print(f"❌ Error sending to {user_name}: {e}")
            return 0
    
    except Exception as e:
        print(f"❌ Error in process_single_recipient_memory_safe: {e}")
        return 0

# --- Load local company data into memory for searching ---
try:
    company_df = pd.read_csv('indian_stock_tickers.csv')
    company_df['BSE Code'] = company_df['BSE Code'].astype(str).fillna('')
except FileNotFoundError:
    print("[CRITICAL ERROR] The company list 'indian_stock_tickers.csv' was not found. Search will not work.")
    company_df = pd.DataFrame(columns=['BSE Code', 'Company Name'])

# --- Helper function to get an authenticated Supabase client ---
def get_authenticated_client():
    """
    Creates a Supabase client instance for the current user session using connection pooling.
    Prioritizes a full Supabase session, but falls back to a service role client
    if the user is logged in via a Flask session (e.g., email-only).
    """
    access_token = session.get('access_token')
    refresh_token = session.get('refresh_token')
    if access_token and refresh_token:
        try:
            sb = _db_pool.get_connection(service_role=False)
            sb.auth.set_session(access_token, refresh_token)
            return sb
        except Exception as e:
            print(f"Session authentication error: {e}")
            # If session is invalid, clear it to force re-login
            session.pop('access_token', None)
            session.pop('refresh_token', None)

    # Fallback for users logged in without a full Supabase session
    if session.get('user_email'):
        return _db_pool.get_connection(service_role=True)

    return None

# --- Decorator for Protected Routes ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        sb_client = get_authenticated_client()
        if sb_client is None:
            flash("You must be logged in to view this page.", "warning")
            return redirect(url_for('login'))
        # Pass the authenticated client to the decorated route function
        return f(sb_client, *args, **kwargs)
    return decorated_function

# --- Unified Authentication Logic ---
def _process_firebase_token():
    """Helper function to verify a Firebase token and set the user session."""
    id_token = request.json.get('token')
    if not id_token:
        return jsonify({"success": False, "error": "No token provided."}), 400

    try:
        decoded_token = auth.verify_id_token(id_token)
        user_result = db.find_or_create_supabase_user(decoded_token)

        if user_result.get('error'):
            return jsonify({"success": False, "error": user_result['error']}), 401

        # Set user data that is always present
        session['user_id'] = user_result.get('user_id')
        session['user_phone'] = user_result.get('phone')

        # Handle Supabase session data if it exists
        if session_data := user_result.get('session'):
            session['access_token'] = session_data.get('access_token')
            session['refresh_token'] = session_data.get('refresh_token')
            session['user_email'] = session_data.get('user', {}).get('email') or user_result.get('email')
        else:
            # Fallback for email if no full Supabase session
            session['user_email'] = user_result.get('email')

        # Final check to ensure a user context was established
        if session.get('user_email') or session.get('user_phone'):
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "error": "Authentication succeeded but no user context could be established."}), 500

    except Exception as e:
        # Catch specific Firebase auth errors if needed, otherwise generic
        return jsonify({"success": False, "error": f"An unexpected error occurred: {str(e)}"}), 500

# --- Firebase Config Endpoint (for frontend) ---
@app.route('/firebase-config')
def get_firebase_config():
    """Serve Firebase configuration from environment variables."""
    try:
        config = {
            'apiKey': os.environ.get('FIREBASE_API_KEY', ''),
            'authDomain': os.environ.get('FIREBASE_AUTH_DOMAIN', ''),
            'projectId': os.environ.get('FIREBASE_PROJECT_ID', ''),
            'storageBucket': os.environ.get('FIREBASE_STORAGE_BUCKET', ''),
            'messagingSenderId': os.environ.get('FIREBASE_MESSAGING_SENDER_ID', ''),
            'appId': os.environ.get('FIREBASE_APP_ID', '')
        }
        
        # Validate that all required config values are present
        missing_keys = [key for key, value in config.items() if not value]
        if missing_keys:
            return jsonify({
                'error': f'Missing Firebase configuration: {", ".join(missing_keys)}'
            }), 500
        
        return jsonify(config)
    except Exception as e:
        return jsonify({'error': f'Failed to load Firebase config: {str(e)}'}), 500

# --- Authentication Routes ---
@app.route('/login')
def login():
    """Renders the new unified login page."""
    return render_template('login_unified.html')

@app.route('/cron/master')
@log_errors
def cron_master():
    """
    UNIFIED CRON MASTER CONTROLLER - TIMEOUT OPTIMIZED
    
    Returns immediate response to prevent UptimeRobot timeouts.
    Processes jobs efficiently with timeout protection.
    """
    key = request.args.get('key')
    # Support both environment variable and hardcoded key for UptimeRobot compatibility
    expected = os.environ.get('CRON_SECRET_KEY', 'c78b684067c74784364e352c391ecad3')
    
    # DEBUG: Log who's calling the cron endpoint
    from datetime import datetime
    caller_ip = request.environ.get('HTTP_X_FORWARDED_FOR', request.environ.get('REMOTE_ADDR', 'unknown'))
    user_agent = request.environ.get('HTTP_USER_AGENT', 'unknown')
    print(f"🔍 CRON CALL: IP={caller_ip}, User-Agent={user_agent}, Time={datetime.now().isoformat()}")
    
    if not expected or key != expected:
        return jsonify({
            "status": "unauthorized",
            "message": "Invalid or missing key",
            "timestamp": datetime.now().isoformat()
        }), 401

    # Always use service client for cron
    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        from datetime import datetime, timedelta
        import uuid
        
        # Get current IST time and market status
        now_ist = db.ist_now()
        is_market_hours, market_open, market_close = db.ist_market_window(now_ist)
        is_working_day = now_ist.weekday() < 5  # Monday=0, Friday=4
        
        # Initialize response
        run_id = str(uuid.uuid4())
        results = {
            'timestamp': now_ist.isoformat(),
            'run_id': run_id,
            'market_hours': is_market_hours,
            'working_day': is_working_day,
            'executed_jobs': [],
            'skipped_jobs': [],
            'errors': []
        }
        
        # Job execution flags
        jobs_to_run = []
        
        # BSE ANNOUNCEMENTS - REMOVED from master cron to prevent duplicates
        # BSE announcements are now handled by dedicated /cron/bse_announcements endpoint
        # This prevents double-triggering when both price spike and BSE crons run simultaneously
        
        # 1. LIVE PRICE MONITORING - Only during market hours on working days
        if is_working_day and is_market_hours:
            jobs_to_run.append({
                'name': 'live_price_monitoring', 
                'condition': True,
                'reason': f'Market hours: {market_open.strftime("%H:%M")} - {market_close.strftime("%H:%M")}'
            })
        else:
            results['skipped_jobs'].append({
                'name': 'live_price_monitoring',
                'reason': f'Outside market hours or non-working day. Market: {is_market_hours}, Working day: {is_working_day}'
            })
        
        # 2. NEWS MONITORING - Every 30 minutes (at :00 and :30)
        # Check if current minute is 0 or 30 (within ±2 minutes for tolerance)
        current_minute = now_ist.minute
        should_run_news = (
            current_minute in range(0, 5) or        # :00-:04 (wider window)
            current_minute in range(25, 35) or      # :25-:34 (wider window for :30)
            current_minute in range(55, 61)         # :55-:00 (wider window)
        )
        
        if should_run_news:
            # Check if already run in the last 25 minutes to prevent duplicates
            last_25_min = now_ist - timedelta(minutes=25)
            try:
                recent_runs = sb.table('cron_run_logs').select('created_at').eq('job', 'news_monitoring').gte('created_at', last_25_min.isoformat()).execute()
                if recent_runs.data:
                    results['skipped_jobs'].append({
                        'name': 'news_monitoring',
                        'reason': 'Already executed within last 25 minutes'
                    })
                else:
                    jobs_to_run.append({
                        'name': 'news_monitoring',
                        'condition': True,
                        'reason': f'30-minute schedule: {now_ist.strftime("%H:%M")} (target: :00/:30)'
                    })
            except Exception:
                # If we can't check, run anyway to be safe
                jobs_to_run.append({
                    'name': 'news_monitoring',
                    'condition': True,
                    'reason': f'30-minute schedule: {now_ist.strftime("%H:%M")} (could not verify recent runs)'
                })
        else:
            results['skipped_jobs'].append({
                'name': 'news_monitoring',
                'reason': f'Not scheduled time. Current: {now_ist.strftime("%H:%M")}, Target: :00/:30 (±2min)'
            })
        
        # 3. DAILY SUMMARY - Once per day at 16:30 (after market close)
        summary_time_target = now_ist.replace(hour=16, minute=30, second=0, microsecond=0)
        time_diff = abs((now_ist - summary_time_target).total_seconds() / 60)  # difference in minutes
        
        # Run daily summary if:
        # - It's a working day
        # - Current time is within 2 minutes of 16:30 (16:28-16:32) - NARROW WINDOW
        # - Haven't run it today already
        should_run_summary = (
            is_working_day and 
            time_diff <= 2 and  # Changed from 10 to 2 minutes
            now_ist >= summary_time_target.replace(minute=28)  # After 16:28
        )
        
        if should_run_summary:
            # Check if already run today - improved duplicate detection
            today_start = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
            try:
                existing_runs = sb.table('cron_run_logs').select('created_at').eq('job', 'daily_summary').gte('created_at', today_start.isoformat()).execute()
                if existing_runs.data:
                    # Check if any run was within the last 30 minutes (much shorter window)
                    for run in existing_runs.data:
                        try:
                            # Handle both timezone-aware and naive datetime strings
                            run_time_str = run['created_at']
                            if run_time_str.endswith('Z'):
                                run_time_str = run_time_str.replace('Z', '+00:00')
                            elif '+' not in run_time_str and 'T' in run_time_str:
                                run_time_str += '+00:00'
                            
                            run_time = datetime.fromisoformat(run_time_str)
                            
                            # Convert to IST for comparison
                            if run_time.tzinfo is not None:
                                run_time_ist = run_time.astimezone(now_ist.tzinfo)
                            else:
                                run_time_ist = run_time.replace(tzinfo=now_ist.tzinfo)
                            
                            time_since_run = (now_ist - run_time_ist).total_seconds()
                            
                            if time_since_run < 7200:  # 2 hours - only one daily summary per day
                                results['skipped_jobs'].append({
                                    'name': 'daily_summary',
                                    'reason': f'Already executed today at {run_time_ist.strftime("%H:%M")} ({time_since_run/60:.0f} min ago)'
                                })
                                should_run_summary = False
                                break
                        except Exception as parse_error:
                            # If we can't parse the time, assume it's old and continue
                            continue
                
                if should_run_summary:  # Still should run
                    jobs_to_run.append({
                        'name': 'daily_summary',
                        'condition': True,
                        'reason': f'Scheduled time reached: {now_ist.strftime("%H:%M")}'
                    })
            except Exception:
                # If we can't check, run anyway to be safe
                jobs_to_run.append({
                    'name': 'daily_summary',
                    'condition': True,
                    'reason': 'Scheduled time reached (could not verify if already run)'
                })
        else:
            results['skipped_jobs'].append({
                'name': 'daily_summary', 
                'reason': f'Not scheduled time. Current: {now_ist.strftime("%H:%M")}, Target: 16:30 (±2min)'
            })
        
        # Execute the jobs
        if not jobs_to_run:
            results['message'] = 'No jobs scheduled for execution'
            return jsonify(results)
        
        # Get user data once
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []
        
        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({
                'bse_code': r.get('bse_code'), 
                'company_name': r.get('company_name')
            })

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'),
                'user_name': r.get('user_name', 'User')
            })
        
        # Execute each job
        daily_summary_sent_users = set()  # Track users who already received daily summary in this run
        
        for job in jobs_to_run:
            job_name = job['name']
            job_result = {
                'name': job_name,
                'reason': job['reason'],
                'users_processed': 0,
                'notifications_sent': 0,
                'users_skipped': 0,
                'errors': []
            }
            
            try:
                for uid, scrips in scrips_by_user.items():
                    recipients = recs_by_user.get(uid) or []
                    if not scrips or not recipients:
                        job_result['users_skipped'] += 1
                        continue
                    
                    # SPECIAL HANDLING FOR DAILY SUMMARY - prevent multiple sends per day using database
                    if job_name == 'daily_summary':
                        # Check if daily summary already sent today for this user
                        today_date = now_ist.strftime('%Y-%m-%d')
                        try:
                            existing_summary = sb.table('cron_run_logs').select('id').eq(
                                'user_id', uid
                            ).eq('job_name', 'daily_summary').gte(
                                'created_at', f'{today_date}T00:00:00+05:30'
                            ).lt(
                                'created_at', f'{today_date}T23:59:59+05:30'
                            ).execute()
                            
                            if existing_summary.data:
                                # Already sent daily summary today - skip
                                job_result['users_skipped'] += 1
                                print(f"📊 DAILY SUMMARY: Skipping user {uid[:8]} - already sent today")
                                continue
                            else:
                                # Mark in memory tracking as well
                                daily_summary_sent_users.add(uid)
                                print(f"📊 DAILY SUMMARY: Processing user {uid[:8]} - first time today")
                        except Exception as db_check_error:
                            print(f"📊 DAILY SUMMARY: DB check failed for {uid[:8]}, proceeding: {db_check_error}")
                            # If DB check fails, use memory tracking as fallback
                            if uid in daily_summary_sent_users:
                                job_result['users_skipped'] += 1
                                continue
                            else:
                                daily_summary_sent_users.add(uid)
                    
                    try:
                        # Execute appropriate function based on job type
                        # Note: bse_announcements removed to prevent duplicates with dedicated /cron/bse_announcements
                        if job_name == 'live_price_monitoring':
                            # Enhanced price spike alerts with debugging and lower thresholds
                            print(f"🔍 PRICE SPIKE: Processing {len(scrips)} scrips for user {uid[:8]}...")
                            from database import ist_market_window
                            is_open, open_dt, close_dt = ist_market_window()
                            print(f"🔍 PRICE SPIKE: Market open: {is_open} (Current time in IST)")
                            
                            if is_open:
                                # Lower thresholds for better detection: 5% price change, 300% volume spike
                                sent = db.send_hourly_spike_alerts(sb, uid, scrips, recipients, price_threshold_pct=5.0, volume_threshold_pct=300.0)
                                print(f"🔍 PRICE SPIKE: Messages sent: {sent}")
                            else:
                                print(f"🔍 PRICE SPIKE: Market closed, skipping alerts")
                                sent = 0
                        elif job_name == 'daily_summary':
                            sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                        elif job_name == 'bulk_deals_monitoring':
                            # Import and use bulk deals monitoring
                            from bulk_deals_monitor import send_bulk_deals_alerts
                            sent = send_bulk_deals_alerts(sb, uid, scrips, recipients)
                        elif job_name == 'news_monitoring':
                            # TEMPORARILY DISABLED RSS PROCESSING TO PREVENT SIGKILL
                            print(f"🚨 RSS NEWS: DISABLED to prevent worker crashes - user {uid[:8]}")
                            print(f"🚨 RSS NEWS: Use dedicated /cron/rss_news endpoint instead")
                            sent = 0
                        else:
                            continue
                        
                        job_result['users_processed'] += 1
                        job_result['notifications_sent'] += sent
                        
                        # Log individual job execution
                        try:
                            user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                            sb.table('cron_run_logs').insert({
                                'run_id': run_id,
                                'job': job_name,
                                'user_id': user_uuid,
                                'processed': True,
                                'notifications_sent': int(sent),
                                'recipients': int(len(recipients))
                            }).execute()
                        except Exception as log_error:
                            job_result['errors'].append(f"Log error for user {uid}: {log_error}")
                            
                    except Exception as user_error:
                        job_result['errors'].append({"user_id": uid, "error": str(user_error)})
                        job_result['users_skipped'] += 1
                
                results['executed_jobs'].append(job_result)
                
            except Exception as job_error:
                results['errors'].append(f"Job {job_name} failed: {str(job_error)}")
        
        # Return quick response to prevent UptimeRobot timeout
        quick_response = {
            'status': 'success',
            'timestamp': results['timestamp'],
            'run_id': results['run_id'],
            'working_day': results['working_day'],
            'market_hours': results['market_hours'],
            'message': f'Processed {len(results["executed_jobs"])} jobs successfully',
            'executed_jobs': results['executed_jobs'],
            'skipped_jobs': results['skipped_jobs'],
            'errors': results['errors'],
            'response_time': 'optimized'
        }
        
        return jsonify(quick_response)
        
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "timestamp": datetime.now().isoformat()}), 500

@app.route('/cron/bse_announcements')
@log_errors
def cron_bse_announcements():
    """Dedicated endpoint for BSE announcements only - DISABLED on multiuser-bse-monitor

    This endpoint is DISABLED on multiuser-bse-monitor.onrender.com to prevent duplicates.
    BSE announcements are now handled exclusively by stockmonitor-aknr.onrender.com via external cron-job.org.
    """
    # Check if BSE announcements are explicitly disabled
    import os
    disable_bse = os.environ.get('DISABLE_BSE_ANNOUNCEMENTS', 'false').lower() == 'true'
    if disable_bse:
        return "BSE announcements endpoint is explicitly disabled via DISABLE_BSE_ANNOUNCEMENTS=true", 403

    # Check if this is the multiuser deployment and disable the endpoint
    app_url = os.environ.get('APP_URL', '') or os.environ.get('RENDER_EXTERNAL_URL', '')
    host = request.headers.get('Host', '')

    # Disable if this appears to be the multiuser deployment
    if 'multiuser-bse-monitor' in app_url or host.startswith('multiuser-bse-monitor'):
        return "BSE announcements endpoint is disabled on multiuser-bse-monitor deployment. Please use stockmonitor-aknr.onrender.com/cron/bse_announcements", 403

    # Original authentication check for stockmonitor deployment
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized", 403

    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        from datetime import datetime
        import uuid
        
        run_id = str(uuid.uuid4())
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []

        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'), 
                'user_name': r.get('user_name')
            })

        totals = {"users_processed": 0, "notifications_sent": 0, "users_skipped": 0}
        errors = []

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                totals["users_skipped"] += 1
                continue
            try:
                sent = db.send_bse_announcements_consolidated(sb, uid, scrips, recipients, hours_back=1)
                totals["users_processed"] += 1
                totals["notifications_sent"] += sent
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': 'bse_announcements',
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Log error for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})

        return jsonify({"ok": True, **totals, "errors": errors})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/cron/price_spike_alerts')
@log_errors
def cron_price_spike_alerts():
    """Dedicated endpoint for price spike alerts only"""
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized", 403

    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        from datetime import datetime
        import uuid
        
        # Check if market is open
        now_ist = db.ist_now()
        is_market_hours, market_open, market_close = db.ist_market_window(now_ist)
        is_working_day = now_ist.weekday() < 5
        
        if not (is_working_day and is_market_hours):
            return jsonify({
                "ok": True, 
                "message": "Market closed - price alerts skipped",
                "market_hours": is_market_hours,
                "working_day": is_working_day
            })
        
        run_id = str(uuid.uuid4())
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []

        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'), 
                'user_name': r.get('user_name')
            })

        totals = {"users_processed": 0, "notifications_sent": 0, "users_skipped": 0}
        errors = []

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                totals["users_skipped"] += 1
                continue
            try:
                sent = db.send_hourly_spike_alerts(sb, uid, scrips, recipients, price_threshold_pct=5.0, volume_threshold_pct=300.0)
                totals["users_processed"] += 1
                totals["notifications_sent"] += sent
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': 'price_spike_alerts',
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Log error for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})

        return jsonify({"ok": True, **totals, "errors": errors})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/cron/rss_news')
@log_errors
def cron_rss_news():
    """Ultra-lightweight RSS news processing"""
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized", 403

    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        from datetime import datetime
        import uuid
        
        run_id = str(uuid.uuid4())
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []

        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'), 
                'user_name': r.get('user_name')
            })

        totals = {"users_processed": 0, "notifications_sent": 0, "users_skipped": 0}
        errors = []

        # Check system memory before starting any RSS processing
        system_memory = _get_memory_usage_fast()
        if system_memory > 250:  # Very conservative limit
            print(f"🚨 MEMORY PROTECTION: System memory {system_memory}MB too high - skipping all RSS processing")
            return jsonify({"ok": True, "message": "Skipped due to high memory usage", "memory_mb": system_memory, **totals, "errors": errors})
        
        print(f"🌍 GLOBAL RSS: Starting optimized processing (memory: {system_memory}MB)")
        
        # Build all users data for global processing
        all_users_data = {}
        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if scrips and recipients:
                all_users_data[uid] = {
                    'scrips': scrips,
                    'recipients': recipients
                }
            else:
                totals["users_skipped"] += 1
        
        if all_users_data:
            try:
                # Use global optimization system from consolidated RSS file
                from consolidated_rss_news import process_rss_globally_optimized
                total_sent = process_rss_globally_optimized(sb, all_users_data)
                
                totals["users_processed"] = len(all_users_data)
                totals["notifications_sent"] = total_sent
                
                print(f"🌍 GLOBAL RSS: Completed - {total_sent} total messages sent to {len(all_users_data)} users")
                
            except Exception as e:
                errors.append(f"Global RSS processing error: {str(e)}")
                print(f"❌ GLOBAL RSS ERROR: {e}")
        
        # Log the runs for each user
        for uid in all_users_data.keys():
            try:
                user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                sb.table('cron_run_logs').insert({
                    'run_id': run_id,
                    'job': 'global_rss_news',
                    'user_id': user_uuid,
                    'processed': True,
                    'notifications_sent': 0,  # Global count, not per-user
                    'recipients': len(recs_by_user.get(uid, [])),
                }).execute()
            except Exception as e:
                errors.append(f"Log error for user {uid}: {e}")
        

        return jsonify({"ok": True, **totals, "errors": errors})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/cron/bulk_deals')
@log_errors
def cron_bulk_deals():
    """Dedicated endpoint for daily bulk/block deals monitoring

    Runs once daily to check previous trading day's bulk deals.
    Bulk deals are typically available the next morning, so this checks
    yesterday's data for any monitored stocks.
    """
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized", 403

    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        from datetime import datetime, date
        import uuid

        run_id = str(uuid.uuid4())
        job_name = 'bulk_deals_monitoring'
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []

        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'), 
                'user_name': r.get('user_name')
            })

        totals = {"users_processed": 0, "notifications_sent": 0, "users_skipped": 0}
        errors = []

        print(f"💼 BULK DEALS: Starting monitoring for {len(scrips_by_user)} users during market hours...")

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                totals["users_skipped"] += 1
                continue
            try:
                print(f"💼 BULK DEALS: Processing user {uid[:8]} with {len(scrips)} scrips...")
                
                # Import and use bulk deals monitoring
                from bulk_deals_monitor import send_bulk_deals_alerts
                sent = send_bulk_deals_alerts(sb, uid, scrips, recipients)
                
                totals["users_processed"] += 1
                totals["notifications_sent"] += sent
                
                print(f"💼 BULK DEALS: User {uid[:8]} - sent {sent} notifications")
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': 'bulk_deals_monitoring',
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Log error for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})
                print(f"❌ BULK DEALS ERROR for user {uid}: {e}")

        print(f"💼 BULK DEALS: Completed - {totals['users_processed']} users processed, {totals['notifications_sent']} notifications sent")

        return jsonify({
            "ok": True, 
            "message": f"Bulk deals monitoring completed during market hours",
            "current_time": now_ist.strftime('%Y-%m-%d %H:%M:%S'),
            "market_status": "OPEN",
            **totals, 
            "errors": errors
        })
    except Exception as e:
        print(f"💼 BULK DEALS: Fatal error - {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

def is_news_relevant_simple(title: str, company_name: str) -> bool:
    """Simple relevance check for news articles"""
    if not title or not company_name:
        return False
    
    title_lower = title.lower()
    company_lower = company_name.lower()
    
    # Extract company keywords (first word, remove common suffixes)
    company_words = company_lower.replace(' ltd', '').replace(' limited', '').replace(' inc', '').replace(' corp', '').split()
    
    # Check if any company word appears in title
    for word in company_words:
        if len(word) > 3 and word in title_lower:  # Only check meaningful words
            return True
    
    return False

def get_next_companies_to_process(sb, user_id: str, scrips: List[Dict], batch_size: int = 2) -> List[Dict]:
    """Get the next batch of companies to process using rotation tracking"""
    try:
        # Get last processed company index for this user
        result = sb.table('rss_processing_tracker').select('last_processed_index, updated_at').eq('user_id', user_id).execute()
        
        last_index = 0
        if result.data:
            last_index = result.data[0].get('last_processed_index', 0)
            
            # Check if we completed a full cycle recently (within last hour)
            from datetime import datetime, timedelta
            last_updated = result.data[0].get('updated_at')
            if last_updated:
                try:
                    last_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                    if datetime.now().timestamp() - last_time.timestamp() > 3600:  # 1 hour
                        last_index = 0  # Reset if it's been too long
                except:
                    last_index = 0
        
        # Calculate next batch
        start_index = (last_index) % len(scrips)
        end_index = min(start_index + batch_size, len(scrips))
        
        # Get the batch
        batch = scrips[start_index:end_index]
        
        # If we didn't get enough companies and haven't wrapped around, get from beginning
        if len(batch) < batch_size and start_index > 0:
            remaining = batch_size - len(batch)
            batch.extend(scrips[:remaining])
            next_index = remaining
        else:
            next_index = end_index
        
        # Update tracking
        try:
            if result.data:
                sb.table('rss_processing_tracker').update({
                    'last_processed_index': next_index,
                    'updated_at': datetime.now().isoformat()
                }).eq('user_id', user_id).execute()
            else:
                sb.table('rss_processing_tracker').insert({
                    'user_id': user_id,
                    'last_processed_index': next_index,
                    'updated_at': datetime.now().isoformat()
                }).execute()
        except Exception as e:
            print(f"Warning: Could not update RSS tracking: {e}")
        
        print(f"📰 RSS ROTATION: Processing companies {start_index}-{start_index+len(batch)-1} of {len(scrips)}")
        return batch
        
    except Exception as e:
        print(f"Warning: RSS tracking failed, using first {batch_size} companies: {e}")
        return scrips[:batch_size]

def process_rss_globally_optimized(sb, all_users_data: Dict) -> int:
    """
    GLOBALLY OPTIMIZED RSS PROCESSING
    Processes unique companies once and distributes to all interested users.
    Replaces per-user processing to eliminate duplicate API calls.
    """
    from collections import defaultdict
    
    total_messages = 0
    batch_size = 3
    
    try:
        print(f"🌍 GLOBAL RSS: Starting optimized processing for {len(all_users_data)} users")
        
        # Step 1: Build global unique company list
        all_companies = set()
        company_to_users = defaultdict(list)
        
        for user_id, user_data in all_users_data.items():
            user_companies = set()
            for scrip in user_data['scrips']:
                company_name = scrip.get('company_name')
                if company_name:
                    all_companies.add(company_name)
                    user_companies.add(company_name)
                    company_to_users[company_name].append(user_id)
            
            print(f"👤 User {user_id[:8]}: {len(user_companies)} companies")
        
        unique_companies = sorted(list(all_companies))
        print(f"🌍 Total unique companies across all users: {len(unique_companies)}")
        
        # Step 2: Get global rotation state
        try:
            result = sb.table('global_rss_rotation').select('last_company_index, updated_at').execute()
            
            global_index = 0
            if result.data:
                global_index = result.data[0].get('last_company_index', 0)
                last_updated = result.data[0].get('updated_at')
                
                # Reset if it's been too long (1 hour)
                if last_updated:
                    try:
                        from datetime import datetime
                        last_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                        if datetime.now().timestamp() - last_time.timestamp() > 3600:
                            global_index = 0
                            print("🔄 Reset global rotation due to timeout")
                    except:
                        global_index = 0
        except Exception as e:
            print(f"Warning: Could not get global rotation state: {e}")
            global_index = 0
        
        # Step 3: Calculate next batch
        start_index = global_index % len(unique_companies)
        end_index = min(start_index + batch_size, len(unique_companies))
        
        batch_companies = unique_companies[start_index:end_index]
        
        # Wrap around if needed
        if len(batch_companies) < batch_size and start_index > 0:
            remaining = batch_size - len(batch_companies)
            batch_companies.extend(unique_companies[:remaining])
            next_index = remaining
        else:
            next_index = end_index % len(unique_companies)
        
        print(f"🔄 GLOBAL ROTATION: Processing companies {start_index}-{start_index+len(batch_companies)-1} of {len(unique_companies)}")
        print(f"📊 COMPANIES IN BATCH: {', '.join(batch_companies)}")
        
        # Step 4: Update global rotation state
        try:
            current_time = datetime.now().isoformat()
            if result.data:
                sb.table('global_rss_rotation').update({
                    'last_company_index': next_index,
                    'total_companies': len(unique_companies),
                    'updated_at': current_time
                }).eq('id', result.data[0]['id']).execute()
            else:
                sb.table('global_rss_rotation').insert({
                    'last_company_index': next_index,
                    'total_companies': len(unique_companies),
                    'updated_at': current_time
                }).execute()
            print(f"✅ Updated global rotation: next_index={next_index}")
        except Exception as e:
            print(f"Warning: Could not update global rotation: {e}")
        
        # Step 5: Fetch news for each company ONCE
        company_news_cache = {}
        
        for company_name in batch_companies:
            print(f"📰 FETCHING: {company_name}")
            
            try:
                from consolidated_rss_news import fetch_google_news_rss, is_relevant_news
                
                # Fetch news once for this company
                raw_articles = fetch_google_news_rss(company_name)
                
                # Filter for relevance
                relevant_articles = []
                for article in raw_articles:
                    if is_relevant_news(article, company_name):
                        relevant_articles.append(article)
                
                company_news_cache[company_name] = relevant_articles
                interested_users = len(company_to_users[company_name])
                
                print(f"📰 {company_name}: {len(raw_articles)} raw → {len(relevant_articles)} relevant → {interested_users} users interested")
                
            except Exception as e:
                print(f"❌ Error fetching {company_name}: {e}")
                company_news_cache[company_name] = []
        
        # Step 6: Distribute cached news to interested users
        for company_name, articles in company_news_cache.items():
            if not articles:
                continue
            
            interested_user_ids = company_to_users[company_name]
            print(f"📤 DISTRIBUTING {company_name}: {len(articles)} articles to {len(interested_user_ids)} users")
            
            for user_id in interested_user_ids:
                user_data = all_users_data[user_id]
                recipients = user_data['recipients']
                
                try:
                    # Process for this specific user
                    user_messages = process_company_for_user_optimized(
                        sb, user_id, company_name, articles, recipients
                    )
                    total_messages += user_messages
                    
                    if user_messages > 0:
                        print(f"📤 {company_name} → User {user_id[:8]}: {user_messages} messages")
                    
                except Exception as e:
                    print(f"❌ Error processing {company_name} for user {user_id[:8]}: {e}")
        
        print(f"🌍 GLOBAL RSS COMPLETED: {total_messages} total messages sent")
        
        # Step 7: Cleanup old per-user tracking entries (if they exist)
        try:
            old_entries = sb.table('rss_processing_tracker').select('id, user_id').execute()
            if old_entries.data and len(old_entries.data) > len(all_users_data):
                print(f"🧹 Cleaning up {len(old_entries.data)} old tracking entries...")
                sb.table('rss_processing_tracker').delete().execute()
                print("🧹 Cleaned up old per-user tracking entries")
        except Exception as e:
            print(f"Note: Could not clean old tracking entries: {e}")
        
        return total_messages
        
    except Exception as e:
        print(f"❌ GLOBAL RSS ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 0

def process_company_for_user_optimized(sb, user_id: str, company_name: str, articles: List[Dict], recipients: List[Dict]) -> int:
    """Process cached articles for a specific user with duplicate checking"""
    try:
        from consolidated_rss_news import (
            generate_article_hash, is_duplicate_in_memory, is_duplicate_in_database,
            mark_sent_in_memory, record_sent_in_database, format_clean_rss_message
        )
        
        messages_sent = 0
        
        # Process each recipient separately
        for recipient in recipients:
            recipient_id = recipient['chat_id']
            user_name = recipient.get('user_name', 'User')
            
            # Filter articles for this specific recipient
            new_articles = []
            
            for article in articles:
                # Generate unique hash for this article + recipient combination
                article_hash = generate_article_hash(article, company_name, recipient_id)
                
                # Check memory cache (fastest)
                if is_duplicate_in_memory(article_hash):
                    continue
                
                # Check database for duplicates
                if is_duplicate_in_database(sb, article, company_name, user_id):
                    mark_sent_in_memory(article_hash)
                    continue
                
                # Article is new and relevant
                new_articles.append(article)
            
            if not new_articles:
                continue
            
            # Format and send message
            telegram_message = format_clean_rss_message(company_name, new_articles)
            
            try:
                from database import send_telegram_message_with_user_name
                if send_telegram_message_with_user_name(recipient_id, telegram_message, user_name):
                    messages_sent += 1
                    
                    # Mark articles as sent
                    for article in new_articles:
                        article_hash = generate_article_hash(article, company_name, recipient_id)
                        mark_sent_in_memory(article_hash)
                        record_sent_in_database(sb, article, company_name, user_id)
                    
            except Exception as e:
                print(f"❌ Error sending to {user_name}: {e}")
        
        return messages_sent
        
    except Exception as e:
        print(f"❌ Error in process_company_for_user_optimized: {e}")
        return 0

def lightweight_rss_news_processing(sb, user_id: str, scrips: List[Dict], recipients: List[Dict]) -> int:
    """
    DEPRECATED: Use global optimization instead.
    This function now bridges to the global system.
    """
    print(f"⚠️ DEPRECATED: lightweight_rss_news_processing called for user {user_id[:8]}")
    print(f"⚠️ This should use the global optimization system instead")
    
    # Fallback to old system for single user (not optimal)
    try:
        from consolidated_rss_news import process_consolidated_rss_news
        limited_scrips = scrips[:3]  # Process 3 companies max
        return process_consolidated_rss_news(sb, user_id, limited_scrips, recipients)
    except Exception as e:
        print(f"❌ Fallback RSS processing error: {e}")
        return 0


@app.route('/cron/daily_summary')
@log_errors
def cron_daily_summary():
    """Cron-compatible endpoint to send end-of-day price movement summary.
    Expects a secret key in query string (?key=...) to prevent abuse.
    Runs at 16:30 IST to send daily price movement notifications.

    This endpoint iterates over all users who have both monitored scrips and
    at least one Telegram recipient, and sends consolidated price movement summaries.
    """
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized", 403

    # Always use service client for cron
    sb = db.get_supabase_client(service_role=True)
    if not sb:
        return "Supabase not configured", 500

    try:
        # Allow overriding hours_back via query param (default: 1 hour)
        try:
            hours_back = int(request.args.get('hours_back', '1'))
        except Exception:
            hours_back = 1

        # Fetch all scrips and recipients once
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []

        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'), 
                'user_name': r.get('user_name')
            })

        totals = {"users_processed": 0, "notifications_sent": 0, "users_skipped": 0, "recipients": 0, "items": 0}
        errors = []

        import uuid
        run_id = str(uuid.uuid4())
        job_name = 'daily_summary'

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                totals["users_skipped"] += 1
                try:
                    # Ensure user_id is a valid UUID
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': False,
                        'notifications_sent': 0,
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    logging.error(f"Failed to log skipped cron run: {e}")
                continue
            try:
                # Send end-of-day price movement summary
                print(f"📊 Sending daily price summary for user {uid[:8]}...")
                sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                print(f"✅ Daily summary sent: {sent} messages for user {uid[:8]}")
                totals["users_processed"] += 1
                totals["notifications_sent"] += sent
                totals["recipients"] += len(recipients)
                try:
                    # Ensure user_id is a valid UUID
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    logging.error(f"Failed to log cron run: {e}")
                # We do not know exact items here, but we can log via BSE_VERBOSE in the function
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})

        return jsonify({"ok": True, **totals, "errors": errors})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/verify_phone_token', methods=['POST'])
def verify_phone_token():
    """Endpoint for verifying Firebase phone auth tokens."""
    return _process_firebase_token()

@app.route('/verify_google_token', methods=['POST'])
def verify_google_token():
    """Endpoint for verifying Firebase Google auth tokens."""
    return _process_firebase_token()

@app.route('/logout')
def logout():
    session.clear()
    flash("You have been successfully logged out.", "success")
    return redirect(url_for('login'))

# --- Main Application Routes (Protected) ---
@app.route('/health')
def health_check():
    """Optimized health check endpoint with connection pooling.
    Returns 200 OK with minimal processing to keep the app alive.
    """
    from datetime import datetime
    start_time = time.time()
    
    try:
        # Use pooled connection for quick DB check
        sb = _db_pool.get_connection(service_role=True)
        if sb:
            # Very lightweight query with timeout
            try:
                sb.table('profiles').select('id').limit(1).execute()
                db_status = 'connected'
            except:
                db_status = 'error'
            finally:
                _db_pool.return_connection(sb)
        else:
            db_status = 'disconnected'
    except Exception:
        db_status = 'error'
    
    response_time = round((time.time() - start_time) * 1000, 1)  # milliseconds
    
    # Add BSE deduplication stats
    try:
        from enhanced_bse_deduplication import get_enhanced_bse_deduplication
        dedup = get_enhanced_bse_deduplication()
        sb = dedup.get_supabase_client(service_role=True)
        if sb:
            bse_stats = dedup.get_deduplication_stats(sb)
            bse_dedup_info = {
                'total_announcements_sent': bse_stats.get('total_announcements_sent', 0),
                'last_24_hours': bse_stats.get('last_24_hours', 0),
                'unique_users_today': bse_stats.get('unique_users_today', 0),
                'table_name': bse_stats.get('table_name', 'seen_announcements'),
                'enhancement_type': bse_stats.get('enhancement_type', 'content_based_deduplication')
            }
        else:
            bse_dedup_info = {'status': 'database_error'}
    except Exception:
        bse_dedup_info = {'status': 'enhanced_dedup_error'}

    # Add multi-threshold alert stats
    try:
        alert_stats = get_alert_tracking_stats()
        alert_info = {
            'active_alerts_today': alert_stats.get('active_alerts_today', 0),
            'threshold_counts': alert_stats.get('threshold_counts', {}),
            'users_tracking': alert_stats.get('total_tracking_keys', 0),
            'thresholds_configured': alert_stats.get('thresholds_configured', [])
        }
    except Exception:
        alert_info = {'status': 'alert_tracker_error'}

    return {
        'status': 'ok',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'service': 'bse-monitor',
        'database': db_status,
        'response_ms': response_time,
        'memory_mb': _get_memory_usage_fast(),
        'db_pool_size': len(_db_pool.connections),
        'rss_objects': len(_rss_memory_tracker),
        'bse_deduplication': bse_dedup_info,
        'multi_threshold_alerts': alert_info
    }, 200

@app.route('/debug/cron_auth')
def debug_cron_auth():
    """Debug endpoint to check cron authentication"""
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    
    return {
        'provided_key': key,
        'expected_key': expected,
        'keys_match': key == expected,
        'expected_exists': expected is not None,
        'provided_exists': key is not None,
        'expected_length': len(expected) if expected else 0,
        'provided_length': len(key) if key else 0
    }

@app.route('/debug/user_setup')
@login_required
def debug_user_setup(sb):
    """Debug endpoint to check user's setup"""
    user_id = session.get('user_id')
    
    # Get user's monitored scrips
    monitored_scrips = db.get_user_scrips(sb, user_id)
    
    # Get user's recipients
    recipients = db.get_user_recipients(sb, user_id)
    
    # Get user's category preferences
    category_prefs = db.get_user_category_prefs(sb, user_id)
    
    return {
        'user_id': user_id,
        'monitored_scrips': monitored_scrips,
        'recipients': recipients,
        'category_preferences': category_prefs,
        'scrip_count': len(monitored_scrips),
        'recipient_count': len(recipients),
        'category_count': len(category_prefs)
    }

@app.route('/debug/cron_logs')
def debug_cron_logs():
    """Debug endpoint to check recent cron job runs"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        # Get recent cron runs (last 50, ordered by id desc since created_at doesn't exist)
        result = sb.table('cron_run_logs').select('*').order('id', desc=True).limit(50).execute()
        
        return {
            'success': True,
            'total_runs': len(result.data),
            'recent_runs': result.data
        }
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/test/evening_summary')
def test_evening_summary():
    """Test endpoint to manually trigger evening summary without secret key"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        # Force run evening summary for all users
        from datetime import datetime
        import uuid
        
        run_id = str(uuid.uuid4())
        job_name = 'evening_summary_test'
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id').execute().data or []
        
        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({'chat_id': r.get('chat_id')})

        users_processed = 0
        notifications_sent = 0
        users_skipped = 0
        errors = []

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                users_skipped += 1
                continue
            try:
                # Send price summary instead of announcements
                sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                users_processed += 1
                notifications_sent += sent
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Failed to log for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})
                users_skipped += 1

        return {
            'success': True,
            'run_id': run_id,
            'job': job_name,
            'timestamp': datetime.now().isoformat(),
            'totals': {
                'users_processed': users_processed,
                'users_skipped': users_skipped,
                'notifications_sent': notifications_sent,
                'errors': errors
            }
        }
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/test/bulk_deals')
def test_bulk_deals():
    """Test endpoint to manually trigger bulk deals monitoring without secret key"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        from datetime import datetime
        import uuid
        
        # Check market status for info
        now_ist = db.ist_now()
        is_market_hours, market_open, market_close = db.ist_market_window(now_ist)
        is_working_day = now_ist.weekday() < 5
        
        run_id = str(uuid.uuid4())
        job_name = 'bulk_deals_test'
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []
        
        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({
                'chat_id': r.get('chat_id'), 
                'user_name': r.get('user_name', 'User')
            })

        users_processed = 0
        notifications_sent = 0
        users_skipped = 0
        errors = []

        print(f"💼 BULK DEALS TEST: Starting for {len(scrips_by_user)} users...")

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                users_skipped += 1
                continue
            try:
                print(f"💼 BULK DEALS TEST: Processing user {uid[:8]} with {len(scrips)} scrips...")
                
                # Import and use bulk deals monitoring
                from bulk_deals_monitor import send_bulk_deals_alerts
                sent = send_bulk_deals_alerts(sb, uid, scrips, recipients)
                
                users_processed += 1
                notifications_sent += sent
                
                print(f"💼 BULK DEALS TEST: User {uid[:8]} - sent {sent} notifications")
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Failed to log for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})
                users_skipped += 1

        return {
            'success': True,
            'forced_test': True,
            'run_id': run_id,
            'job': job_name,
            'timestamp': datetime.now().isoformat(),
            'market_info': {
                'current_time': now_ist.strftime('%Y-%m-%d %H:%M:%S'),
                'is_market_hours': is_market_hours,
                'is_working_day': is_working_day,
                'market_open': market_open.strftime('%H:%M'),
                'market_close': market_close.strftime('%H:%M')
            },
            'totals': {
                'users_processed': users_processed,
                'users_skipped': users_skipped,
                'notifications_sent': notifications_sent,
                'errors': errors
            }
        }
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/monitor/cron_status')
def monitor_cron_status():
    """Monitoring dashboard for cron job status and recent runs"""
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        from datetime import datetime, timedelta
        
        # Get recent cron runs (last 24 hours)
        result = sb.table('cron_run_logs').select('*').order('id', desc=True).limit(100).execute()
        
        # Analyze the data
        runs_by_job = {}
        total_notifications = 0
        total_users = 0
        recent_errors = []
        
        for run in result.data:
            job = run.get('job', 'unknown')
            if job not in runs_by_job:
                runs_by_job[job] = {
                    'total_runs': 0,
                    'successful_runs': 0,
                    'total_notifications': 0,
                    'total_users': 0,
                    'last_run': None,
                    'recent_runs': []
                }
            
            runs_by_job[job]['total_runs'] += 1
            runs_by_job[job]['recent_runs'].append(run)
            
            if run.get('processed'):
                runs_by_job[job]['successful_runs'] += 1
                runs_by_job[job]['total_notifications'] += run.get('notifications_sent', 0)
                runs_by_job[job]['total_users'] += 1
            
            # Track the most recent run for each job
            if not runs_by_job[job]['last_run']:
                runs_by_job[job]['last_run'] = run
        
        # Calculate summary stats
        for job_data in runs_by_job.values():
            total_notifications += job_data['total_notifications']
            total_users += job_data['total_users']
            # Keep only last 10 runs for each job
            job_data['recent_runs'] = job_data['recent_runs'][:10]
        
        # Get current IST time and market status
        ist_now = db.ist_now()
        is_market_open, market_open_time, market_close_time = db.ist_market_window()
        
        return {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'ist_time': ist_now.isoformat(),
            'market_status': {
                'is_open': is_market_open,
                'open_time': market_open_time.isoformat() if market_open_time else None,
                'close_time': market_close_time.isoformat() if market_close_time else None
            },
            'summary': {
                'total_jobs': len(runs_by_job),
                'total_notifications_sent': total_notifications,
                'total_user_runs': total_users,
                'total_runs_analyzed': len(result.data)
            },
            'jobs': runs_by_job,
            'quick_links': {
                'test_evening_summary': '/test/evening_summary',
                'debug_cron_logs': '/debug/cron_logs',
                'debug_cron_auth': '/debug/cron_auth',
                'health_check': '/health'
            }
        }
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/force/evening_summary')
def force_evening_summary():
    """Force trigger evening summary bypassing all timing restrictions"""
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized - Use: /force/evening_summary?key=YOUR_CRON_SECRET_KEY", 403
    
    try:
        sb = db.get_supabase_client(service_role=True)
        if not sb:
            return {'error': 'Supabase not configured'}, 500
        
        from datetime import datetime
        import uuid
        
        run_id = str(uuid.uuid4())
        job_name = 'evening_summary_forced'
        
        # Get all users with scrips and recipients
        scrip_rows = sb.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb.table('telegram_recipients').select('user_id, chat_id').execute().data or []
        
        # Build maps by user
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            scrips_by_user.setdefault(uid, []).append({'bse_code': r.get('bse_code'), 'company_name': r.get('company_name')})

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if not uid:
                continue
            recs_by_user.setdefault(uid, []).append({'chat_id': r.get('chat_id')})

        users_processed = 0
        notifications_sent = 0
        users_skipped = 0
        errors = []

        print(f"FORCE EVENING SUMMARY: Processing {len(scrips_by_user)} users...")

        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid) or []
            if not scrips or not recipients:
                users_skipped += 1
                continue
            try:
                # FORCE send price summary - bypass all timing checks
                sent = db.send_script_messages_to_telegram(sb, uid, scrips, recipients)
                users_processed += 1
                notifications_sent += sent
                print(f"  User {uid}: sent {sent} notifications")
                
                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': job_name,
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': int(sent),
                        'recipients': int(len(recipients)),
                    }).execute()
                except Exception as e:
                    errors.append(f"Failed to log for user {uid}: {e}")
                    
            except Exception as e:
                errors.append({"user_id": uid, "error": str(e)})
                users_skipped += 1
                print(f"  ERROR User {uid}: {e}")

        result = {
            'success': True,
            'forced': True,
            'run_id': run_id,
            'job': job_name,
            'timestamp': datetime.now().isoformat(),
            'ist_time': db.ist_now().isoformat(),
            'totals': {
                'users_processed': users_processed,
                'users_skipped': users_skipped,
                'notifications_sent': notifications_sent,
                'errors': errors
            }
        }
        
        print(f"FORCE EVENING SUMMARY COMPLETE: {result}")
        return result
        
    except Exception as e:
        print(f"FORCE EVENING SUMMARY ERROR: {e}")
        return {'error': str(e)}, 500

def get_memory_usage():
    """Get current memory usage in MB"""
    try:
        import psutil
        import os
        process = psutil.Process(os.getpid())
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except Exception:
        return 'unknown'

@app.route('/')
@login_required
def dashboard(sb):
    """Main dashboard showing monitored scrips and recipients."""
    user_id = session.get('user_id')
    monitored_scrips = db.get_user_scrips(sb, user_id)
    telegram_recipients = db.get_user_recipients(sb, user_id)
    
    category_prefs = db.get_user_category_prefs(sb, user_id)
    return render_template('dashboard.html', 
                           monitored_scrips=monitored_scrips,
                           telegram_recipients=telegram_recipients,
                           category_prefs=category_prefs,
                           user_email=session.get('user_email', ''),
                           user_phone=session.get('user_phone', ''))

@app.route('/search')
@login_required
def search(sb):
    """Endpoint for fuzzy searching company names and BSE codes."""
    query = request.args.get('query', '')
    if not query or len(query) < 2:
        return jsonify({"matches": []})
    
    mask = (company_df['Company Name'].str.contains(query, case=False, na=False)) | \
           (company_df['BSE Code'].str.startswith(query))
           
    matches = company_df[mask].head(10)
    return jsonify({"matches": matches.to_dict('records')})

@app.route('/send_script_messages', methods=['POST'])
@login_required
def send_script_messages(sb):
    """Triggers sending Telegram messages for all monitored scrips."""
    user_id = session.get('user_id')
    try:
        monitored_scrips = db.get_user_scrips(sb, user_id)
        telegram_recipients = db.get_user_recipients(sb, user_id)
        
        if not monitored_scrips:
            flash('No scrips to monitor. Please add scrips first.', 'info')
        elif not telegram_recipients:
            flash('No Telegram recipients found. Please add a recipient first.', 'info')
        else:
            messages_sent = db.send_script_messages_to_telegram(sb, user_id, monitored_scrips, telegram_recipients)
            if messages_sent > 0:
                flash(f'Successfully sent {messages_sent} message(s)!', 'success')
            else:
                flash('No messages were sent. Check scrips and recipients.', 'info')
            
    except Exception as e:
        flash(f'Error sending messages: {str(e)}', 'error')
        print(f"Error in send_script_messages: {e}")
    
    return redirect(url_for('dashboard'))

@app.route('/send_bse_announcements', methods=['POST'])
@login_required
def send_bse_announcements(sb):
    """Send consolidated BSE announcements for monitored scrips to Telegram recipients."""
    user_id = session.get('user_id')
    try:
        monitored_scrips = db.get_user_scrips(sb, user_id)
        telegram_recipients = db.get_user_recipients(sb, user_id)
        hours_back = 24
        try:
            hours_back = int(request.form.get('hours_back', 24))
        except Exception:
            hours_back = 24

        if not monitored_scrips:
            flash('No scrips to monitor. Please add scrips first.', 'info')
        elif not telegram_recipients:
            flash('No Telegram recipients found. Please add a recipient first.', 'info')
        else:
            sent = db.send_bse_announcements_consolidated(sb, user_id, monitored_scrips, telegram_recipients, hours_back=hours_back)
            if sent > 0:
                flash(f'Sent announcements summary to {sent} recipient(s).', 'success')
            else:
                flash('No new announcements found in the selected period.', 'warning')
    except Exception as e:
        flash(f'Error sending BSE announcements: {str(e)}', 'error')
        print(f"Error in send_bse_announcements: {e}")

    return redirect(url_for('dashboard'))

# --- Data Management Routes (Protected) ---
@app.route('/add_scrip', methods=['POST'])
@login_required
def add_scrip(sb):
    user_id = session.get('user_id')
    bse_code = request.form.get('scrip_code')
    company_name = request.form.get('company_name', '').strip()

    if not bse_code:
        flash('Scrip code is required.', 'error')
        return redirect(url_for('dashboard'))

    if not company_name:
        match = company_df[company_df['BSE Code'] == bse_code]
        if not match.empty:
            company_name = str(match.iloc[0]['Company Name'])
        else:
            flash('Scrip code not found. Please check the BSE code.', 'error')
            return redirect(url_for('dashboard'))

    db.add_user_scrip(sb, user_id, bse_code, company_name)
    flash(f'Added {company_name} to your watchlist.', 'success')
    return redirect(url_for('dashboard'))

@app.route('/delete_scrip', methods=['POST'])
@login_required
def delete_scrip(sb):
    user_id = session.get('user_id')
    
    # Safely extract scrip_code - handle both string and list cases
    scrip_code = request.form.get('scrip_code')
    if isinstance(scrip_code, list):
        scrip_code = scrip_code[0] if scrip_code else None
    elif isinstance(scrip_code, dict):
        # Handle unexpected dict case
        scrip_code = str(scrip_code) if scrip_code else None
    
    if not scrip_code:
        flash('Invalid scrip code provided.', 'error')
        return redirect(url_for('dashboard'))
    
    try:
        db.delete_user_scrip(sb, user_id, str(scrip_code))
        flash(f'Scrip {scrip_code} removed from your watchlist.', 'success')
    except Exception as e:
        flash(f'Error removing scrip: {str(e)}', 'error')
        print(f"Error in delete_scrip: {e}")
    
    return redirect(url_for('dashboard'))

@app.route('/add_recipient', methods=['POST'])
@login_required
def add_recipient(sb):
    user_id = session.get('user_id')
    chat_id = request.form['chat_id']
    user_name = request.form.get('user_name', '').strip()
    
    if not user_name:
        flash('Recipient name is required.', 'error')
        return redirect(url_for('dashboard'))
    
    result = db.add_user_recipient(sb, user_id, chat_id, user_name)
    
    if result['success']:
        flash(result['message'], 'success')
    else:
        flash(result['message'], 'error')
    
    return redirect(url_for('dashboard'))

@app.route('/delete_recipient', methods=['POST'])
@login_required
def delete_recipient(sb):
    user_id = session.get('user_id')
    chat_id = request.form['chat_id']
    user_name = request.form.get('user_name')  # Optional for backwards compatibility
    
    db.delete_user_recipient(sb, user_id, chat_id, user_name)
    
    if user_name:
        flash(f'Recipient "{user_name}" ({chat_id}) removed.', 'success')
    else:
        flash(f'Recipient {chat_id} removed.', 'success')
    
    return redirect(url_for('dashboard'))

@app.route('/set_category_prefs', methods=['POST'])
@login_required
def set_category_prefs(sb):
    user_id = session.get('user_id')
    selected = request.form.getlist('categories')
    ok = db.set_user_category_prefs(sb, user_id, selected)
    if ok:
        flash('Category preferences saved.', 'success')
    else:
        flash('Failed to save preferences.', 'error')
    return redirect(url_for('dashboard'))

# --- Enhanced Sentiment Analysis Routes (Protected) ---
@app.route('/sentiment_analysis_mobile')
@login_required
def sentiment_analysis_mobile(sb):
    """Renders the enhanced mobile-friendly sentiment analysis dashboard."""
    user_id = session.get('user_id')
    monitored_scrips = db.get_user_scrips(sb, user_id)
    return render_template('sentiment_analysis_mobile.html', 
                         scrips=monitored_scrips,
                         user_email=session.get('user_email'))

@app.route('/sentiment_analysis_detailed')
@login_required  
def sentiment_analysis_detailed(sb):
    """Renders the detailed sentiment analysis results page."""
    return render_template('sentiment_analysis_detailed.html')

@app.route('/analyze_sentiment', methods=['POST'])
@login_required
def analyze_sentiment(sb):
    """API endpoint for comprehensive sentiment analysis (on-demand)."""
    try:
        data = request.get_json()
        stock_symbol = data.get('stock_symbol')
        company_name = data.get('company_name')
        
        if not stock_symbol or not company_name:
            return jsonify({'error': 'Stock symbol and company name required'}), 400
        
        # Use comprehensive sentiment analysis service
        from sentiment_analysis_service import perform_comprehensive_sentiment_analysis
        
        # Perform comprehensive analysis (API + Database)
        analysis_result = perform_comprehensive_sentiment_analysis(sb, stock_symbol, company_name)
        
        if analysis_result.get('success'):
            return jsonify({
                'success': True,
                'sentiment_data': analysis_result
            })
        else:
            return jsonify({
                'success': False,
                'error': analysis_result.get('error', 'Analysis failed')
            }), 500
            
    except Exception as e:
        print(f"Error in analyze_sentiment: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_sentiment_summary')
@login_required
def get_sentiment_summary(sb):
    """API endpoint for recent news monitoring status (not sentiment analysis)."""
    try:
        user_id = session.get('user_id')
        monitored_scrips = db.get_user_scrips(sb, user_id)
        
        # Get recent news monitoring status from database
        summary_data = []
        for scrip in monitored_scrips:
            try:
                # Get most recent news for this stock (not sentiment)
                result = sb.table('processed_news_articles')\
                    .select('*')\
                    .eq('stock_query', scrip['company_name'])\
                    .order('processed_at', desc=True)\
                    .limit(1)\
                    .execute()
                
                if result.data:
                    news_item = result.data[0]
                    summary_data.append({
                        'bse_code': scrip['bse_code'],
                        'company_name': scrip['company_name'],
                        'overall_sentiment': 'NEWS_AVAILABLE',
                        'sentiment_score': 0,
                        'confidence': 0,
                        'total_articles': 1,
                        'last_analysis': news_item['processed_at'],
                        'has_data': True
                    })
                else:
                    # No news data yet
                    summary_data.append({
                        'bse_code': scrip['bse_code'],
                        'company_name': scrip['company_name'],
                        'overall_sentiment': 'PENDING',
                        'sentiment_score': 0,
                        'confidence': 0,
                        'total_articles': 0,
                        'has_data': False
                    })
            except Exception as e:
                print(f"Error getting news status for {scrip.get('company_name', 'N/A')}: {e}")
                continue
        
        return jsonify({'success': True, 'summary_data': summary_data})
    except Exception as e:
        print(f"Error in get_sentiment_summary: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/toggle_sentiment_preference', methods=['POST'])
@login_required
def toggle_sentiment_preference(sb):
    """Toggle sentiment analysis preference for a specific stock."""
    try:
        user_id = session.get('user_id')
        data = request.get_json()
        stock_name = data.get('stock_name')
        enabled = data.get('enabled', True)
        
        if not stock_name:
            return jsonify({'error': 'Stock name required'}), 400
        
        # Check if preference already exists
        existing = sb.table('user_sentiment_preferences')\
            .select('*')\
            .eq('user_id', user_id)\
            .eq('stock_name', stock_name)\
            .execute()
        
        if existing.data:
            # Update existing preference
            sb.table('user_sentiment_preferences')\
                .update({'sentiment_enabled': enabled, 'updated_at': 'NOW()'})\
                .eq('user_id', user_id)\
                .eq('stock_name', stock_name)\
                .execute()
        else:
            # Create new preference
            sb.table('user_sentiment_preferences').insert({
                'user_id': user_id,
                'stock_name': stock_name,
                'sentiment_enabled': enabled
            }).execute()
        
        return jsonify({'success': True})
    except Exception as e:
        print(f"Error in toggle_sentiment_preference: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_sentiment_preferences')
@login_required
def get_sentiment_preferences(sb):
    """Get sentiment analysis preferences for all user's stocks."""
    try:
        user_id = session.get('user_id')
        monitored_scrips = db.get_user_scrips(sb, user_id)
        
        preferences = {}
        for scrip in monitored_scrips:
            stock_name = scrip['company_name']
            
            # Get preference from database
            result = sb.table('user_sentiment_preferences')\
                .select('sentiment_enabled, min_confidence_threshold')\
                .eq('user_id', user_id)\
                .eq('stock_name', stock_name)\
                .execute()
            
            if result.data:
                pref = result.data[0]
                preferences[stock_name] = {
                    'enabled': pref['sentiment_enabled'],
                    'min_confidence': pref['min_confidence_threshold']
                }
            else:
                # Default preferences
                preferences[stock_name] = {
                    'enabled': True,
                    'min_confidence': 40
                }
        
        return jsonify({'success': True, 'preferences': preferences})
    except Exception as e:
        print(f"Error in get_sentiment_preferences: {e}")
        return jsonify({'error': str(e)}), 500

# --- Health Check ---

# --- Enhanced Monitoring Endpoints ---
@app.route('/ping')
def ping():
    """Ultra-fast ping endpoint - NO database calls, minimal processing"""
    from datetime import datetime
    
    # Force quick garbage collection for RSS memory only when needed
    if len(_rss_memory_tracker) > 50:  # Only if RSS objects accumulating
        gc.collect()
    
    return jsonify({
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat() + 'Z',
        "server": "stockmonitor-aknr",
        "uptime_ms": int(time.time() * 1000) % 1000000,  # Rolling counter
        "memory_mb": _get_memory_usage_fast()
    }), 200

@app.route('/uptime')
def uptime():
    """UptimeRobot specific endpoint for keep-alive - NO LOGIN REQUIRED"""
    from datetime import datetime
    return jsonify({
        "status": "up",
        "timestamp": datetime.now().isoformat(),
        "server": "stockmonitor-aknr",
        "message": "Server is alive and preventing Render sleep",
        "purpose": "keep_alive"
    }), 200

@app.route('/alive')
def alive():
    """Simple alive check with timestamp - detects if server is hung"""
    import time
    from datetime import datetime
    
    return jsonify({
        "status": "alive",
        "timestamp": datetime.now().isoformat(),
        "unix_time": time.time(),
        "server": "stockmonitor-aknr"
    }), 200

@app.route('/health-detailed')
def health_detailed():
    """Comprehensive health check with timeout detection"""
    import time
    from datetime import datetime
    
    start_time = time.time()
    health_data = {
        "status": "checking",
        "timestamp": datetime.now().isoformat(),
        "checks": {},
        "server": "stockmonitor-aknr"
    }
    
    # Test 1: Database connection
    try:
        sb = db.get_supabase_client(service_role=True)
        if sb:
            response = sb.table('profiles').select('id').limit(1).execute()
            health_data["checks"]["database"] = "connected"
        else:
            health_data["checks"]["database"] = "failed"
            
    except Exception as e:
        health_data["checks"]["database"] = f"error: {str(e)[:100]}"
    
    # Test 2: Memory usage
    try:
        import psutil
        memory = psutil.virtual_memory()
        health_data["checks"]["memory_percent"] = memory.percent
        health_data["checks"]["memory_available_gb"] = round(memory.available / (1024**3), 2)
        
        if memory.percent > 90:
            health_data["checks"]["memory_status"] = "critical"
        elif memory.percent > 75:
            health_data["checks"]["memory_status"] = "warning"
        else:
            health_data["checks"]["memory_status"] = "normal"
            
    except ImportError:
        health_data["checks"]["memory"] = "psutil_not_available"
    except Exception as e:
        health_data["checks"]["memory"] = f"error: {str(e)}"
    
    # Test 3: Response time
    response_time = time.time() - start_time
    health_data["checks"]["response_time_seconds"] = round(response_time, 3)
    
    # Test 4: Last cron run
    try:
        sb = db.get_supabase_client(service_role=True)
        if sb:
            cron_response = sb.table('cron_run_logs').select('created_at').order(
                'created_at', desc=True
            ).limit(1).execute()
            
            if cron_response.data:
                last_cron = cron_response.data[0]['created_at']
                health_data["checks"]["last_cron_run"] = last_cron
            else:
                health_data["checks"]["last_cron_run"] = "no_records"
    except Exception as e:
        health_data["checks"]["last_cron_run"] = f"error: {str(e)[:50]}"
    
    # Overall status determination
    if response_time > 10:
        health_data["status"] = "slow_response"
    elif health_data["checks"].get("database") != "connected":
        health_data["status"] = "database_issue"
    elif health_data["checks"].get("memory_percent", 0) > 90:
        health_data["status"] = "memory_critical"
    elif health_data["checks"].get("memory_percent", 0) > 75:
        health_data["status"] = "memory_warning"
    else:
        health_data["status"] = "healthy"
    
    # Return appropriate HTTP status
    if health_data["status"] in ["healthy", "memory_warning"]:
        return jsonify(health_data), 200
    else:
        return jsonify(health_data), 503

@app.route('/memory-status')
def memory_status():
    """Detailed memory monitoring"""
    try:
        import psutil
        import gc
        import os
        from datetime import datetime
        
        # Force garbage collection
        gc.collect()
        
        # System memory
        memory = psutil.virtual_memory()
        
        # Process memory
        process = psutil.Process(os.getpid())
        process_memory = process.memory_info()
        
        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "system": {
                "total_gb": round(memory.total / (1024**3), 2),
                "available_gb": round(memory.available / (1024**3), 2),
                "used_percent": memory.percent,
                "status": "critical" if memory.percent > 90 else "warning" if memory.percent > 75 else "normal"
            },
            "process": {
                "memory_mb": round(process_memory.rss / (1024**2), 2),
                "memory_percent": round(process.memory_percent(), 2),
                "threads": process.num_threads(),
                "cpu_percent": process.cpu_percent()
            },
            "server": "stockmonitor-aknr"
        }), 200
        
    except Exception as e:
        from datetime import datetime
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

# --- Memory Optimization Endpoints ---
@app.route('/admin/memory-optimize')
def memory_optimize():
    """Force memory optimization"""
    key = request.args.get('key')
    expected = os.environ.get('CRON_SECRET_KEY')
    if not expected or key != expected:
        return "Unauthorized", 403
    
    before_mb = _get_memory_usage_fast()
    
    # Clear caches
    _get_memory_usage_fast.cache_clear()
    
    # Cleanup database connections
    _db_pool.cleanup_old_connections()
    
    # Force garbage collection
    gc.collect()
    
    # Clear RSS cache (less frequently)
    try:
        import random
        if random.randint(1, 10) == 1:  # Only 10% of the time
            from simple_rss_fix import cleanup_rss_cache
            cleanup_rss_cache()
    except:
        pass
    
    after_mb = _get_memory_usage_fast()
    
    return {
        'status': 'optimized',
        'before_mb': before_mb,
        'after_mb': after_mb,
        'freed_mb': round(before_mb - after_mb, 1),
        'rss_objects_tracked': len(_rss_memory_tracker),
        'db_connections': len(_db_pool.connections),
        'timestamp': datetime.now().isoformat()
    }

# Periodic Cleanup Function - RE-ENABLED with safety measures
def periodic_cleanup():
    """Run periodic cleanup every 30 minutes with proper error handling"""
    try:
        print("🧹 Starting periodic cleanup...")

        # Cleanup old database connections
        _db_pool.cleanup_old_connections()

        # Force garbage collection if memory high
        current_memory = _get_memory_usage_fast()
        if current_memory > 400:  # If over 400MB
            gc.collect()
            new_memory = _get_memory_usage_fast()
            print(f"🧹 Periodic cleanup: {current_memory}MB → {new_memory}MB")

        # Clear RSS cache (less frequently)
        try:
            import random
            if random.randint(1, 20) == 1:  # Only 5% of the time during periodic cleanup
                from simple_rss_fix import cleanup_rss_cache
                cleanup_rss_cache()
                print("🧹 RSS cache cleaned")
        except Exception as e:
            print(f"RSS cache cleanup error: {e}")

        print("✅ Periodic cleanup completed")
    except Exception as e:
        print(f"Periodic cleanup error: {e}")

    # Re-enabled with safer threading
    if not app.debug and not os.environ.get('DISABLE_AUTO_CLEANUP'):
        try:
            timer = threading.Timer(1800.0, periodic_cleanup)  # 30 minutes
            timer.daemon = True  # Set as daemon thread
            timer.start()
        except Exception as e:
            print(f"Failed to schedule periodic cleanup: {e}")

# Start periodic cleanup only in production
if not os.environ.get('FLASK_DEBUG') == '1' and not os.environ.get('DISABLE_AUTO_CLEANUP'):
    try:
        periodic_cleanup()
    except Exception as e:
        print(f"Failed to start periodic cleanup: {e}")

# --- Main Execution ---
if __name__ == '__main__':
    db.initialize_firebase()
    port = int(os.environ.get('PORT', os.environ.get('FLASK_RUN_PORT', 5000)))
    debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(host='0.0.0.0', port=port, debug=debug)




