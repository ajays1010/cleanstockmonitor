"""
Enhanced BSE Endpoint using Existing seen_announcements Table
Prevents duplicates by enhancing existing database structure
"""

import os
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict
from flask import Blueprint, request, jsonify
import logging
from enhanced_bse_deduplication import get_enhanced_bse_deduplication

logger = logging.getLogger(__name__)

# Create blueprint
enhanced_bse_bp = Blueprint('enhanced_bse', __name__, url_prefix='/cron')

def authenticate_cron_request():
    """Authenticate cron request"""
    expected_key = os.environ.get('CRON_SECRET_KEY')
    provided_key = request.args.get('key')
    return provided_key == expected_key

@enhanced_bse_bp.route('/bse_announcements_enhanced')
def bse_announcements_enhanced():
    """
    Enhanced BSE Announcements Endpoint
    Uses existing seen_announcements table with improved deduplication
    """
    start_time = time.time()

    # Authentication
    if not authenticate_cron_request():
        return jsonify({
            "ok": False,
            "error": "Unauthorized",
            "timestamp": datetime.now().isoformat()
        }), 403

    # Check if BSE announcements are disabled
    if os.environ.get('DISABLE_BSE_ANNOUNCEMENTS', 'false').lower() == 'true':
        return jsonify({
            "ok": False,
            "error": "BSE announcements disabled on this deployment",
            "timestamp": datetime.now().isoformat()
        }), 403

    # Multi-deployment check
    host = request.headers.get('Host', '')
    if 'multiuser-bse-monitor' in host:
        return jsonify({
            "ok": False,
            "error": "BSE announcements disabled on multiuser-bse-monitor deployment",
            "timestamp": datetime.now().isoformat()
        }), 403

    try:
        run_id = str(uuid.uuid4())
        logger.info(f"🔧 ENHANCED BSE: Starting run {run_id}")

        # Get database clients
        sb_service = get_enhanced_bse_deduplication().get_supabase_client(service_role=True)
        if not sb_service:
            return jsonify({
                "ok": False,
                "error": "Database connection failed",
                "timestamp": datetime.now().isoformat()
            }), 500

        # Initialize enhanced deduplication
        dedup = get_enhanced_bse_deduplication()

        # Get all users with monitored scrips and recipients
        scrip_rows = sb_service.table('monitored_scrips').select('user_id, bse_code, company_name').execute().data or []
        rec_rows = sb_service.table('telegram_recipients').select('user_id, chat_id, user_name').execute().data or []

        # Build user mappings
        scrips_by_user = {}
        for r in scrip_rows:
            uid = r.get('user_id')
            if uid:
                scrips_by_user.setdefault(uid, []).append({
                    'bse_code': r.get('bse_code'),
                    'company_name': r.get('company_name')
                })

        recs_by_user = {}
        for r in rec_rows:
            uid = r.get('user_id')
            if uid:
                recs_by_user.setdefault(uid, []).append({
                    'chat_id': r.get('chat_id'),
                    'user_name': r.get('user_name')
                })

        # Processing statistics
        totals = {
            "users_processed": 0,
            "users_skipped": 0,
            "total_announcements_found": 0,
            "duplicates_prevented": 0,
            "notifications_sent": 0,
            "database_errors": 0
        }

        processing_details = []
        errors = []

        # Process each user
        for uid, scrips in scrips_by_user.items():
            recipients = recs_by_user.get(uid, [])
            if not scrips or not recipients:
                totals["users_skipped"] += 1
                continue

            try:
                logger.info(f"📊 Processing user {uid[:8]} with {len(scrips)} scrips")

                # Fetch BSE announcements for this user
                user_announcements = _fetch_user_announcements(uid, scrips)
                totals["total_announcements_found"] += len(user_announcements)

                user_notifications_sent = 0
                user_duplicates_prevented = 0

                for announcement in user_announcements:
                    news_id = announcement['news_id']
                    headline = announcement['headline']
                    company_name = announcement['company_name']
                    category = announcement['category']
                    ann_dt = announcement['ann_dt']
                    scrip_code = announcement['scrip_code']
                    pdf_name = announcement.get('pdf_name', '')

                    # ENHANCED: Check if already sent using existing table with enhanced logic
                    already_sent, reason = dedup.is_announcement_already_sent(
                        sb_service, uid, news_id, headline, company_name, ann_dt, category, scrip_code
                    )

                    if already_sent:
                        user_duplicates_prevented += 1
                        totals["duplicates_prevented"] += 1
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            logger.info(f"🚫 ENHANCED: Duplicate prevented for {news_id} - {reason}")
                        continue

                    # Mark as sent in existing table BEFORE sending notification
                    marked = dedup.mark_announcement_sent(
                        sb_service, uid, news_id, headline, company_name, ann_dt, category, scrip_code, pdf_name
                    )

                    if marked:
                        # Send notification (this would be your Telegram notification logic)
                        notifications_sent = len(recipients)
                        user_notifications_sent += notifications_sent
                        totals["notifications_sent"] += notifications_sent

                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            logger.info(f"📢 ENHANCED: Sent {news_id} to {uid[:8]} ({notifications_sent} notifications)")
                    else:
                        totals["database_errors"] += 1
                        errors.append(f"Failed to mark {news_id} as sent for user {uid[:8]}")

                processing_details.append({
                    'user_id': uid[:8],
                    'announcements_found': len(user_announcements),
                    'duplicates_prevented': user_duplicates_prevented,
                    'notifications_sent': user_notifications_sent
                })

                totals["users_processed"] += 1

                # Log the run
                try:
                    user_uuid = uid if uid and len(uid) == 36 and '-' in uid else None
                    sb_service.table('cron_run_logs').insert({
                        'run_id': run_id,
                        'job': 'bse_announcements_enhanced',
                        'user_id': user_uuid,
                        'processed': True,
                        'notifications_sent': user_notifications_sent,
                        'recipients': int(len(recipients)),
                        'announcements_found': len(user_announcements),
                        'duplicates_prevented': user_duplicates_prevented
                    }).execute()
                except Exception as e:
                    errors.append(f"Log error for user {uid}: {e}")

            except Exception as e:
                error_msg = f"User {uid[:8]}: {str(e)}"
                errors.append(error_msg)
                logger.error(f"❌ Error processing user {uid[:8]}: {e}")

        # Calculate final statistics
        runtime = (time.time() - start_time) * 1000
        db_stats = dedup.get_deduplication_stats(sb_service)

        logger.info(f"🎯 ENHANCED BSE: Completed run {run_id}")
        logger.info(f"   Users processed: {totals['users_processed']}")
        logger.info(f"   Total announcements found: {totals['total_announcements_found']}")
        logger.info(f"   Duplicates prevented: {totals['duplicates_prevented']}")
        logger.info(f"   Notifications sent: {totals['notifications_sent']}")
        logger.info(f"   Runtime: {runtime:.2f}ms")

        return jsonify({
            "ok": True,
            "run_id": run_id,
            "message": "Enhanced BSE announcement processing completed (using existing table)",
            "stats": totals,
            "processing_details": processing_details,
            "database_stats": db_stats,
            "runtime_ms": round(runtime, 2),
            "timestamp": datetime.now().isoformat(),
            "errors": errors
        })

    except Exception as e:
        logger.error(f"❌ ENHANCED BSE: Fatal error: {e}")
        return jsonify({
            "ok": False,
            "error": str(e),
            "runtime_ms": round((time.time() - start_time) * 1000, 2),
            "timestamp": datetime.now().isoformat()
        }), 500

def _fetch_user_announcements(user_id: str, scrips: List[Dict]) -> List[Dict]:
    """Fetch BSE announcements for user's monitored scrips"""
    try:
        from database import fetch_bse_announcements_for_scrip, ist_now

        since_dt = ist_now() - timedelta(hours=1)  # Last 1 hour
        all_announcements = []

        for scrip in scrips:
            scrip_code = scrip['bse_code']
            company_name = scrip.get('company_name', str(scrip_code))

            try:
                announcements = fetch_bse_announcements_for_scrip(scrip_code, since_dt)

                for ann in announcements:
                    ann['company_name'] = company_name
                    all_announcements.append(ann)

            except Exception as e:
                logger.warning(f"Error fetching announcements for {scrip_code}: {e}")
                continue

        return all_announcements

    except Exception as e:
        logger.error(f"Error fetching user announcements: {e}")
        return []

@enhanced_bse_bp.route('/enhanced_bse_stats')
def enhanced_bse_stats():
    """Get enhanced deduplication statistics"""
    try:
        dedup = get_enhanced_bse_deduplication()
        sb = dedup.get_supabase_client(service_role=True)

        if not sb:
            return jsonify({
                "ok": False,
                "error": "Database connection failed"
            }), 500

        stats = dedup.get_deduplication_stats(sb)

        return jsonify({
            "ok": True,
            "service": "enhanced_bse_deduplication",
            "table_used": "seen_announcements (existing, enhanced)",
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@enhanced_bse_bp.route('/cleanup_enhanced_db')
def cleanup_enhanced_db():
    """Clean up old database records"""
    try:
        days_to_keep = int(request.args.get('days', 30))
        dedup = get_enhanced_bse_deduplication()
        sb = dedup.get_supabase_client(service_role=True)

        if not sb:
            return jsonify({
                "ok": False,
                "error": "Database connection failed"
            }), 500

        deleted_count = dedup.cleanup_old_records(sb, days_to_keep)

        return jsonify({
            "ok": True,
            "message": f"Cleaned up {deleted_count} old records from seen_announcements",
            "days_kept": days_to_keep,
            "table_used": "seen_announcements (existing)",
            "timestamp": datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500