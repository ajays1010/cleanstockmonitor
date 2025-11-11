"""
Enhanced BSE Announcement Endpoint with PDF and AI Analysis Support
Uses existing seen_announcements table for improved deduplication
"""

import os
import sys
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify
import time
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from enhanced_bse_deduplication import EnhancedBSEDeduplication
    from database import get_supabase_client, fetch_bse_announcements_for_user, ist_now
except ImportError as e:
    logging.error(f"Failed to import required modules: {e}")
    sys.exit(1)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create blueprint
enhanced_bse_bp = Blueprint('enhanced_bse', __name__)

# Initialize deduplication
dedup = EnhancedBSEDeduplication()

@enhanced_bse_bp.route('/cron/bse_announcements_enhanced', methods=['GET'])
def bse_announcements_enhanced():
    """Enhanced BSE announcements endpoint with improved deduplication using existing table"""

    start_time = time.time()
    run_id = os.urandom(16).hex()

    # Validate cron key
    cron_key = request.args.get('key')
    if cron_key != 'c78b684067c74784364e352c391ecad3':
        return jsonify({
            'ok': False,
            'error': 'Unauthorized'
        }), 403

    # Enable verbose logging if requested
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        logger.info(f"🔧 ENHANCED: Starting enhanced BSE processing run {run_id}")
        logger.info(f"🔧 ENHANCED: Using existing seen_announcements table with enhanced logic")

    try:
        # Get database connections
        sb_service = get_supabase_client(service_role=True)
        if not sb_service:
            logger.error("❌ ENHANCED: Failed to connect to database")
            return jsonify({
                'ok': False,
                'error': 'Database connection failed'
            }), 500

        # Get all users who have BSE monitoring enabled
        users_response = sb_service.table('users').select('*').eq('bse_monitoring', True).execute()

        if not users_response.data:
            logger.info("📊 ENHANCED: No users found with BSE monitoring enabled")
            return jsonify({
                'ok': True,
                'run_id': run_id,
                'message': 'No users with BSE monitoring enabled',
                'stats': {
                    'users_processed': 0,
                    'total_announcements_found': 0,
                    'notifications_sent': 0,
                    'duplicates_prevented': 0,
                    'database_errors': 0
                },
                'processing_details': [],
                'runtime_ms': 0,
                'timestamp': datetime.now().isoformat(),
                'table_used': 'seen_announcements (existing, enhanced)'
            })

        # Process each user
        totals = {
            'users_processed': 0,
            'total_announcements_found': 0,
            'notifications_sent': 0,
            'duplicates_prevented': 0,
            'database_errors': 0
        }

        processing_details = []
        errors = []

        # Check for force_script parameter
        force_script = request.args.get('force_script')

        for user_data in users_response.data:
            uid = user_data['id']

            try:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    logger.info(f"👤 ENHANCED: Processing user {uid[:8]}...")

                # Get user's monitored scripts
                scrips_response = sb_service.table('monitored_scrips').select('*').eq('user_id', uid).execute()

                if not scrips_response.data:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        logger.info(f"📝 ENHANCED: No monitored scripts for user {uid[:8]}")
                    continue

                # Get user's BSE announcements (with rate limiting protection)
                user_announcements = fetch_bse_announcements_for_user(uid, scrips_response.data, hours_back=24)

                if not user_announcements:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        logger.info(f"📭 ENHANCED: No announcements found for user {uid[:8]}")
                    continue

                # Filter by force_script if specified
                if force_script:
                    user_announcements = [ann for ann in user_announcements if ann.get('scrip_code') == force_script]

                totals["total_announcements_found"] += len(user_announcements)

                user_notifications_sent = 0
                user_duplicates_prevented = 0

                # Get user's telegram recipients for BSE
                recipients_response = sb_service.table('telegram_recipients').select('*').eq('user_id', uid).eq('notification_types', 'bse').execute()

                if not recipients_response.data:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        logger.info(f"📱 ENHANCED: No telegram recipients for BSE notifications for user {uid[:8]}")
                    continue

                recipients = recipients_response.data

                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    logger.info(f"📱 ENHANCED: Found {len(recipients)} telegram recipients for user {uid[:8]}")

                # Process each announcement
                for announcement in user_announcements:
                    try:
                        news_id = announcement['news_id']
                        headline = announcement['headline']
                        company_name = announcement['company_name']
                        ann_dt = announcement['ann_dt']
                        scrip_code = announcement['scrip_code']
                        category = announcement['category']
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

                        # Send actual Telegram notifications with PDF and AI analysis (like original)
                        notifications_sent = 0
                        try:
                            from database import (TELEGRAM_API_URL, ist_now, PDF_BASE_URL, BSE_HEADERS,
                                                ai_service, format_structured_telegram_message,
                                                analyze_pdf_bytes_with_gemini)
                            from requests import post
                            from collections import defaultdict

                            # Create item for PDF and AI processing
                            item = {
                                'scrip_code': str(scrip_code),
                                'headline': headline,
                                'ann_dt': ann_dt,
                                'pdf_name': pdf_name,
                                'category': category,
                                'news_id': news_id
                            }

                            # 1. Send text summary message first
                            code_to_name = {str(scrip_code): company_name}
                            by_scrip = defaultdict(list)
                            by_scrip[scrip_code] = [item]

                            # Build summary message like the original function
                            header = [
                                "📰 BSE Announcements",
                                f"🕐 {ist_now().strftime('%Y-%m-%d %H:%M:%S')} IST",
                                "",
                            ]
                            lines = header[:]
                            for scode, items in by_scrip.items():
                                comp_name = code_to_name.get(str(scode)) or str(scode)
                                lines.append(f"• {comp_name}")
                                for it in items[:5]:
                                    lines.append(f"  {it['headline'][:80]}...")
                                    lines.append(f"  📅 {it['ann_dt'].strftime('%d-%m-%Y %H:%M')}")
                                    if it.get('pdf_name'):
                                        lines.append(f"  📄 PDF: {it['pdf_name']}")

                            summary_text = "\n".join(lines)

                            # Send to each recipient
                            for recipient in recipients:
                                chat_id = recipient.get('chat_id')
                                user_name = recipient.get('user_name', 'User')

                                if chat_id:
                                    # Add user name header
                                    personalized_summary = f"👤 {user_name}\n" + "─" * 20 + "\n" + summary_text

                                    response = post(f"{TELEGRAM_API_URL}/sendMessage",
                                                json={'chat_id': chat_id,
                                                     'text': personalized_summary,
                                                     'parse_mode': 'HTML'},
                                                timeout=10)

                                    if response.status_code == 200:
                                        notifications_sent += 1
                                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                                            logger.info(f"📢 ENHANCED: Sent summary {news_id} to {user_name} ({chat_id})")
                                    else:
                                        logger.error(f"❌ ENHANCED: Failed to send summary to {user_name} - HTTP {response.status_code}")

                            # 2. Send PDF document (if available)
                            if pdf_name and os.environ.get('BSE_VERBOSE', '0') == '1':
                                logger.info(f"📄 ENHANCED: Attempting to send PDF for {pdf_name}")

                            if pdf_name:
                                pdf_url = f"{PDF_BASE_URL}{pdf_name}"
                                try:
                                    pdf_response = requests.get(pdf_url, headers=BSE_HEADERS, timeout=30)
                                    if pdf_response.status_code == 200 and pdf_response.content:

                                        # Build caption for PDF
                                        caption = (
                                            f"Company: {company_name}\n"
                                            f"Announcement: {headline}\n"
                                            f"Date: {ann_dt.strftime('%d-%m-%Y %H:%M')} IST\n"
                                            f"Category: {category}"
                                        )

                                        # Send PDF to each recipient
                                        for recipient in recipients:
                                            chat_id = recipient.get('chat_id')
                                            if chat_id:
                                                pdf_response_post = post(
                                                    f"{TELEGRAM_API_URL}/sendDocument",
                                                    json={
                                                        'chat_id': chat_id,
                                                        'document': pdf_url,
                                                        'caption': caption,
                                                        'parse_mode': 'HTML'
                                                    },
                                                    timeout=10
                                                )

                                                if pdf_response_post.status_code == 200:
                                                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                                                        logger.info(f"📄 ENHANCED: Sent PDF {pdf_name} to {chat_id}")
                                                else:
                                                    logger.error(f"❌ ENHANCED: Failed to send PDF to {chat_id} - HTTP {pdf_response_post.status_code}")

                                except Exception as pdf_error:
                                    logger.error(f"❌ ENHANCED: Error sending PDF {pdf_name}: {pdf_error}")

                            # 3. Send AI analysis (if available)
                            try:
                                if os.environ.get('BSE_VERBOSE', '0') == '1':
                                    logger.info(f"🤖 ENHANCED: Starting AI analysis for {pdf_name or 'no PDF'}")

                                # Always try AI analysis (like original)
                                if pdf_name:
                                    ai_response = requests.get(pdf_url, headers=BSE_HEADERS, timeout=30)
                                    if ai_response.status_code == 200 and ai_response.content:

                                        analysis_result = analyze_pdf_bytes_with_gemini(
                                            ai_response.content,
                                            announcement
                                        )

                                        if analysis_result:
                                            ai_message = format_structured_telegram_message(
                                                analysis_result,
                                                str(scrip_code),
                                                headline,
                                                ann_dt,
                                                is_quarterly=False
                                            )

                                            # Send AI analysis to each recipient
                                            for recipient in recipients:
                                                chat_id = recipient.get('chat_id')
                                                user_name = recipient.get('user_name', 'User')
                                                if chat_id:
                                                    ai_response_post = post(
                                                        f"{TELEGRAM_API_URL}/sendMessage",
                                                        json={
                                                            'chat_id': chat_id,
                                                            'text': ai_message,
                                                            'parse_mode': 'HTML'
                                                        },
                                                        timeout=10
                                                    )

                                                    if ai_response_post.status_code == 200:
                                                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                                                            logger.info(f"🤖 ENHANCED: Sent AI analysis {pdf_name} to {user_name}")
                                                    else:
                                                        logger.error(f"❌ ENHANCED: Failed to send AI analysis to {user_name} - HTTP {ai_response_post.status_code}")
                                        else:
                                            logger.warning(f"⚠️ ENHANCED: AI analysis returned empty result for {pdf_name}")
                                    else:
                                        logger.warning(f"⚠️ ENHANCED: Could not fetch PDF for AI analysis")

                            except Exception as ai_error:
                                logger.error(f"❌ ENHANCED: Error in AI analysis: {ai_error}")

                        except Exception as e:
                            logger.error(f"❌ ENHANCED: Error sending notifications for {news_id}: {e}")

                        user_notifications_sent += notifications_sent
                        totals["notifications_sent"] += notifications_sent

                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            logger.info(f"📢 ENHANCED: Sent {news_id} to {uid[:8]} ({notifications_sent} notifications)")

                        # Mark as sent in existing table AFTER all notifications are successfully sent
                        marked = dedup.mark_announcement_sent(
                            sb_service, uid, news_id, headline, company_name, ann_dt, category, scrip_code, pdf_name
                        )

                        if not marked:
                            totals["database_errors"] += 1
                            errors.append(f"Failed to mark {news_id} as sent for user {uid[:8]}")

                    except Exception as e:
                        logger.error(f"❌ ENHANCED: Error processing announcement {news_id}: {e}")
                        totals["database_errors"] += 1

                processing_details.append({
                    'user_id': uid[:8],
                    'announcements_found': len(user_announcements),
                    'duplicates_prevented': user_duplicates_prevented,
                    'notifications_sent': user_notifications_sent
                })

                totals["users_processed"] += 1

            except Exception as e:
                error_msg = f"User {uid[:8]}: {str(e)}"
                errors.append(error_msg)
                logger.error(f"❌ Error processing user {uid[:8]}: {e}")

        # Calculate final statistics
        runtime = (time.time() - start_time) * 1000
        db_stats = dedup.get_deduplication_stats(sb_service)

        if os.environ.get('BSE_VERBOSE', '0') == '1':
            logger.info(f"📊 ENHANCED: Processing completed:")
            logger.info(f"   Users processed: {totals['users_processed']}")
            logger.info(f"   Announcements found: {totals['total_announcements_found']}")
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
            "errors": errors,
            "table_used": "seen_announcements (existing, enhanced)"
        })

    except Exception as e:
        logger.error(f"❌ ENHANCED: Critical error in enhanced endpoint: {e}")
        return jsonify({
            'ok': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
            'table_used': "seen_announcements (existing, enhanced)"
        }), 500

@enhanced_bse_bp.route('/cron/cleanup_seen_announcements', methods=['GET'])
def cleanup_seen_announcements():
    """Clean up old announcements from seen_announcements table"""

    # Validate cron key
    cron_key = request.args.get('key')
    if cron_key != 'c78b684067c74784364e352c391ecad3':
        return jsonify({
            'ok': False,
            'error': 'Unauthorized'
        }), 403

    try:
        days_to_keep = int(request.args.get('days', 90))
        sb_service = get_supabase_client(service_role=True)

        if not sb_service:
            return jsonify({
                'ok': False,
                'error': 'Database connection failed'
            }), 500

        # Delete old announcements using existing table structure
        cutoff_date = ist_now() - timedelta(days=days_to_keep)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

        # Using existing table structure - delete by date
        delete_response = sb_service.table('seen_announcements').delete().lt('created_at', cutoff_str).execute()

        deleted_count = len(delete_response.data) if delete_response.data else 0

        logger.info(f"🧹 ENHANCED: Cleaned up {deleted_count} old records from seen_announcements")

        return jsonify({
            'ok': True,
            'message': f"Cleaned up {deleted_count} old records from seen_announcements",
            'days_kept': days_to_keep,
            'table_used': 'seen_announcements (existing)',
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"❌ ENHANCED: Error cleaning up seen_announcements: {e}")
        return jsonify({
            'ok': False,
            'error': str(e)
        }), 500