from flask import Blueprint, render_template, request, redirect, url_for, session, flash, jsonify
from functools import wraps
import database as db

# Create a 'Blueprint' for the admin section. This helps organize routes.
admin_bp = Blueprint('admin', __name__, url_prefix='/admin')

# --- Admin Authentication Decorator ---
def admin_required(f):
    """A decorator to ensure a user is a logged-in admin."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Prefer full Supabase session if present
        access_token = session.get('access_token')
        if access_token and session.get('refresh_token'):
            sb = db.get_supabase_client()
            if not sb:
                flash("Backend not configured. Please set SUPABASE_URL and SUPABASE_KEY.", "error")
                return redirect(url_for('dashboard'))
            try:
                sb.auth.set_session(access_token, session.get('refresh_token'))
                user = sb.auth.get_user()
                if not user:
                    raise Exception("User not found")
                profile = sb.table('profiles').select('is_admin').eq('id', user.user.id).single().execute().data
                if not profile or not profile.get('is_admin'):
                    flash("You do not have permission to access this page.", "error")
                    return redirect(url_for('dashboard'))
                return f(sb, *args, **kwargs)
            except Exception as e:
                flash(f"Admin access error: {e}", "error")
                return redirect(url_for('dashboard'))

        # Fallback: use service client and app session identity (email/user_id)
        if not session.get('user_email'):
            return redirect(url_for('login'))

        sb_admin = db.get_supabase_client(service_role=True)
        if not sb_admin:
            flash("Admin backend not configured. Please set SUPABASE_URL and SUPABASE_SERVICE_KEY in your environment.", "error")
            return redirect(url_for('dashboard'))
        try:
            user_id = session.get('user_id')
            profile_query = sb_admin.table('profiles').select('id, is_admin')
            if user_id:
                profile_query = profile_query.eq('id', user_id)
            else:
                profile_query = profile_query.eq('email', session.get('user_email'))
            profile = profile_query.single().execute().data
            if not profile or not profile.get('is_admin'):
                flash("You do not have permission to access this page.", "error")
                return redirect(url_for('dashboard'))
        except Exception as e:
            flash(f"Admin access error: {e}", "error")
            return redirect(url_for('dashboard'))

        return f(sb_admin, *args, **kwargs)
    return decorated_function

# --- Admin Panel Routes ---
@admin_bp.route('/')
@admin_required
def dashboard(sb):
    """Main admin dashboard. Shows a list of all users."""
    all_users = db.admin_get_all_users()
    return render_template('admin_dashboard.html', users=all_users, selected_user=None)

@admin_bp.route('/cron_runs')
@admin_required
def cron_runs(sb):
    """Admin-only page: view last cron run summaries (counts per user)."""
    # Try a robust fetch that works even if ordering fails
    error_msg = None
    try:
        # Try to fetch cron run logs with proper error handling
        rows = []
        try:
            result = sb.table('cron_run_logs').select('*').order('id', desc=True).limit(500).execute()
            if result and hasattr(result, 'data'):
                potential_rows = result.data
                # Debug what we actually got
                if potential_rows is not None:
                    if isinstance(potential_rows, list):
                        rows = potential_rows
                    elif hasattr(potential_rows, '__iter__') and not isinstance(potential_rows, (str, bytes)):
                        # Try to convert to list if it's iterable but not a string
                        try:
                            rows = list(potential_rows)
                        except Exception:
                            rows = []
                    else:
                        rows = []
                else:
                    rows = []
        except Exception as e1:
            # Fallback without ordering if that fails
            try:
                result = sb.table('cron_run_logs').select('*').limit(500).execute()
                if result and hasattr(result, 'data') and result.data:
                    potential_rows = result.data
                    if isinstance(potential_rows, list):
                        rows = potential_rows
                    else:
                        rows = []
                else:
                    rows = []
            except Exception as e2:
                rows = []
                error_msg = f"Query errors: {e1}, {e2}"
        
        # Final safety check
        if not isinstance(rows, list):
            rows = []
            
    except Exception as e:
        rows = []
        error_msg = f"Outer query error: {e}"

    try:
        from collections import defaultdict
        grouped = defaultdict(list)
        runs = []
        
        # Handle empty rows gracefully
        if not rows:
            runs = []
        else:
            for r in rows:
                if r and r.get('run_id'):  # Ensure r is not None and has run_id
                    grouped[r.get('run_id')].append(r)
            
            # Convert to regular dict to avoid any issues with defaultdict.items()
            grouped_dict = dict(grouped)
            for run_id, items in grouped_dict.items():
                if not items:
                    continue
                # Get job info from first item (avoid complex sorting)
                items_sorted = sorted(items, key=lambda x: str(x.get('user_id') or ''))
                if items:
                    job = items[0].get('job', 'unknown')
                    run_at = items[0].get('id', 'N/A')  # Use id as identifier
                else:
                    job = 'unknown'
                    run_at = None
                    
                total_users = len({i.get('user_id') for i in items if i.get('user_id')})
                processed_users = sum(1 for i in items if i.get('processed'))
                skipped_users = sum(1 for i in items if not i.get('processed'))
                total_notifications = sum(int(i.get('notifications_sent') or 0) for i in items)
                total_recipients = sum(int(i.get('recipients') or 0) for i in items)
                runs.append({
                    'run_id': run_id,
                    'run_at': run_at,
                    'job': job,
                    'total_users': total_users,
                    'processed_users': processed_users,
                    'skipped_users': skipped_users,
                    'total_notifications': total_notifications,
                    'total_recipients': total_recipients,
                    'items': items_sorted[:50],
                })
            runs = sorted(runs, key=lambda x: str(x.get('run_at') or ''), reverse=True)[:10]
        if error_msg:
            flash(error_msg, 'warning')
        # Fetch current evening summary time from app_settings
        setting = None
        try:
            setting = sb.table('app_settings').select('value').eq('key','evening_summary_ist_hhmm').single().execute().data
        except Exception:
            setting = None
        evening_time = (setting or {}).get('value') if isinstance(setting, dict) else None
        return render_template('admin_cron_runs.html', runs=runs, evening_time=evening_time)
    except Exception as e:
        flash(f"Error processing cron runs: {e}", 'error')
        return render_template('admin_cron_runs.html', runs=[])

@admin_bp.route('/set_evening_time', methods=['POST'])
@admin_required
def set_evening_time(sb):
    t = request.form.get('evening_time', '').strip()
    import re
    if not re.fullmatch(r'^[0-2]\d:[0-5]\d$', t):
        flash('Invalid time format. Use HH:MM (24h).', 'error')
        return redirect(url_for('admin.cron_runs'))
    try:
        sb.table('app_settings').upsert({'key':'evening_summary_ist_hhmm','value':t}).execute()
        flash('Evening summary time updated.', 'success')
    except Exception as e:
        flash(f'Failed to update: {e}', 'error')
    return redirect(url_for('admin.cron_runs'))

@admin_bp.route('/trigger_cron', methods=['POST'])
@admin_required
def trigger_cron(sb):
    """Manually trigger cron jobs for testing"""
    import requests
    import os
    from datetime import datetime
    
    cron_type = request.form.get('cron_type')
    if not cron_type:
        flash('Invalid cron type.', 'error')
        return redirect(url_for('admin.cron_runs'))
    
    # Get the base URL and secret key
    base_url = request.url_root.rstrip('/')
    secret_key = os.environ.get('CRON_SECRET_KEY')
    
    if not secret_key:
        flash('CRON_SECRET_KEY not configured in environment variables.', 'error')
        return redirect(url_for('admin.cron_runs'))
    
    # Map cron types to endpoints
    endpoint_map = {
        'price_spike_alerts': '/cron/hourly_spike_alerts',  # Price spike monitoring
        'evening_summary': '/cron/evening_summary',         # Evening summary
        'bse_announcements': '/cron/bse_announcements'      # BSE announcements 24/7
    }
    
    if cron_type not in endpoint_map:
        flash('Unknown cron type.', 'error')
        return redirect(url_for('admin.cron_runs'))
    
    endpoint = endpoint_map[cron_type]
    
    # Build URL with proper parameters
    if cron_type == 'evening_summary':
        url = f"{base_url}{endpoint}?key={secret_key}&force=true"
    else:
        url = f"{base_url}{endpoint}?key={secret_key}"
    
    try:
        flash(f'Triggering {cron_type.replace("_", " ").title()}... This may take a few minutes.', 'info')
        
        # Make the request with a longer timeout
        response = requests.get(url, timeout=300)  # 5 minute timeout
        
        if response.status_code == 200:
            try:
                data = response.json()
                totals = data.get('totals', {})
                
                flash(f'✅ {cron_type.replace("_", " ").title()} completed successfully!', 'success')
                flash(f'📊 Results: {totals.get("users_processed", 0)} users processed, '
                      f'{totals.get("notifications_sent", 0)} notifications sent, '
                      f'{totals.get("users_skipped", 0)} users skipped.', 'info')
                
            except Exception:
                flash(f'✅ {cron_type.replace("_", " ").title()} completed (non-JSON response).', 'success')
                
        elif response.status_code == 403:
            flash('❌ Authentication failed. Check CRON_SECRET_KEY.', 'error')
            
        else:
            flash(f'❌ Request failed with status {response.status_code}: {response.text[:200]}', 'error')
            
    except requests.exceptions.Timeout:
        flash(f'⏰ {cron_type.replace("_", " ").title()} is taking longer than expected. Check logs for completion.', 'warning')
        
    except requests.exceptions.ConnectionError:
        flash('🔌 Connection error. Check if the application is running.', 'error')
        
    except Exception as e:
        flash(f'💥 Error triggering {cron_type}: {str(e)}', 'error')
    
    return redirect(url_for('admin.cron_runs'))

@admin_bp.route('/user/<user_id>')
@admin_required
def view_user(sb, user_id):
    """Shows the scrips and recipients for a specific user."""
    all_users = db.admin_get_all_users()
    selected_user_data = db.admin_get_user_details(user_id)
    
    return render_template('admin_dashboard.html', 
                           users=all_users, 
                           selected_user=selected_user_data)

@admin_bp.route('/add_scrip', methods=['POST'])
@admin_required
def add_scrip(sb):
    user_id = request.form['user_id']
    bse_code = request.form['scrip_code']
    company_name = request.form['company_name']
    db.admin_add_scrip_for_user(user_id, bse_code, company_name)
    return redirect(url_for('admin.view_user', user_id=user_id))

@admin_bp.route('/delete_scrip', methods=['POST'])
@admin_required
def delete_scrip(sb):
    user_id = request.form['user_id']
    bse_code = request.form['scrip_code']
    db.admin_delete_scrip_for_user(user_id, bse_code)
    return redirect(url_for('admin.view_user', user_id=user_id))

@admin_bp.route('/add_recipient', methods=['POST'])
@admin_required
def add_recipient(sb):
    user_id = request.form['user_id']
    chat_id = request.form['chat_id']
    user_name = request.form.get('user_name', '').strip()
    
    if not user_name:
        flash('Recipient name is required.', 'error')
    else:
        db.admin_add_recipient_for_user(user_id, chat_id, user_name)
        flash(f'Added recipient "{user_name}" with Chat ID {chat_id}.', 'success')
    
    return redirect(url_for('admin.view_user', user_id=user_id))

@admin_bp.route('/delete_recipient', methods=['POST'])
@admin_required
def delete_recipient(sb):
    user_id = request.form['user_id']
    chat_id = request.form['chat_id']
    user_name = request.form.get('user_name')  # Optional for backwards compatibility
    
    db.admin_delete_recipient_for_user(user_id, chat_id, user_name)
    
    if user_name:
        flash(f'Deleted recipient "{user_name}" ({chat_id}).', 'success')
    else:
        flash(f'Deleted recipient {chat_id}.', 'success')
    
    return redirect(url_for('admin.view_user', user_id=user_id))

@admin_bp.route('/purge', methods=['POST'])
@admin_required
def purge_data(sb):
    """Purge all data except for the current admin user, guarded by a secret."""
    secret = request.form.get('secret', '')
    if secret != 'vadodara':
        flash('Invalid secret for purge operation.', 'error')
        return redirect(url_for('admin.dashboard'))

    current_user_id = session.get('user_id')
    if not current_user_id:
        # Try to resolve from email via profiles
        try:
            email = session.get('user_email')
            if email:
                profile = sb.table('profiles').select('id').eq('email', email).single().execute().data
                current_user_id = profile and profile.get('id')
        except Exception:
            current_user_id = None

    if not current_user_id:
        flash('Could not determine current admin user id.', 'error')
        return redirect(url_for('admin.dashboard'))

    try:
        # Keep only current admin's rows in core tables
        sb.table('seen_announcements').delete().neq('user_id', current_user_id).execute()
        sb.table('monitored_scrips').delete().neq('user_id', current_user_id).execute()
        sb.table('telegram_recipients').delete().neq('user_id', current_user_id).execute()
        flash('Purge complete. Kept only your data.', 'success')
    except Exception as e:
        flash(f'Purge failed: {e}', 'error')

    return redirect(url_for('admin.dashboard'))
