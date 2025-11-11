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
                                    lines.append(f"  - {it['ann_dt'].strftime('%d-%m %H:%M')} — {it['headline']}")
                                lines.append("")

                            summary_text = "\n".join(lines).strip()

                            # Send summary to each recipient
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
                                                    f"{TEGRAM_API_URL}/sendDocument",
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
                                            pdf_name,
                                            str(scrip_code)
                                        )

                                        if analysis_result and os.environ.get('ENABLE_AI_ANALYSIS', 'true').lower() == 'true':
                                            ai_message = format_structured_telegram_message(
                                                analysis_result,
                                                str(scrip_code),
                                                headline,
                                                ann_dt,
                                                category == 'financials'
                                            )

                                            # Send AI analysis to each recipient
                                            for recipient in recipients:
                                                chat_id = recipient.get('chat_id')
                                                user_name = recipient.get('user_name', 'User')

                                                if chat_id:
                                                    personalized_ai_message = f"👤 {user_name}\n" + "─" * 20 + "\n" + ai_message

                                                    ai_response_post = post(
                                                        f"{TELEGRAM_API_URL}/sendMessage",
                                                        json={
                                                            'chat_id': chat_id,
                                                            'text': personalized_ai_message,
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
            else:
                totals["database_errors"] += 1
                errors.append(f"Failed to send any notifications for {news_id} to user {uid[:8]}")

            processing_details.append({
                'user_id': uid[:8],
                'announcements_found': len(user_announcements),
                'duplicates_prevented': user_duplicates_prevented,
                'notifications_sent': user_notifications_sent
            })

                totals["users_processed"] += 1

            # Skip logging to cron_run_logs to prevent database schema errors
            # The main response processing_details contains all necessary information

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