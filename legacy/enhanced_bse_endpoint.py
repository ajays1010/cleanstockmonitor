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
    from database import get_supabase_client, fetch_bse_announcements_for_scrip, ist_now
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

        # Get unique users from monitored_scrips table (no users table exists)
        scrips_response = sb_service.table('monitored_scrips').select('user_id', count='exact').execute()

        if not scrips_response.data:
            logger.info("📊 ENHANCED: No users found with monitored scrips")
            return jsonify({
                'ok': True,
                'run_id': run_id,
                'message': 'No users with monitored scrips',
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

        # Get unique user IDs
        unique_user_ids = list(set([item['user_id'] for item in scrips_response.data]))

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

        for uid in unique_user_ids:

            try:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    logger.info(f"👤 ENHANCED: Processing user {uid[:8]}...")

                # Get user's monitored scripts
                scrips_response = sb_service.table('monitored_scrips').select('*').eq('user_id', uid).execute()

                if not scrips_response.data:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        logger.info(f"📝 ENHANCED: No monitored scripts for user {uid[:8]}")
                    continue

                # Get user's BSE announcements by fetching for each script (only last 1 hour by default)
                hours_back = int(request.args.get('hours_back', os.environ.get('BSE_CRON_HOURS_BACK', '1')))
                since_dt = ist_now() - timedelta(hours=hours_back)

                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    logger.info(f"⏰ ENHANCED: Fetching announcements from last {hours_back} hours (since {since_dt.strftime('%Y-%m-%d %H:%M')})")

                user_announcements = []

                for scrip_record in scrips_response.data:
                    scrip_code = scrip_record.get('bse_code')
                    company_name = scrip_record.get('company_name', f"Script {scrip_code}")

                    if force_script and scrip_code != force_script:
                        continue

                    try:
                        scrip_announcements = fetch_bse_announcements_for_scrip(scrip_code, since_dt)
                        for announcement in scrip_announcements:
                            announcement['scrip_code'] = scrip_code
                            announcement['company_name'] = company_name
                            announcement['user_id'] = uid
                        user_announcements.extend(scrip_announcements)
                    except Exception as scrip_error:
                        logger.warning(f"⚠️ ENHANCED: Error fetching announcements for scrip {scrip_code}: {scrip_error}")

                if not user_announcements:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        logger.info(f"📭 ENHANCED: No announcements found for user {uid[:8]}")
                    continue

                # SMART GROUPING: Group similar announcements to avoid duplicate notifications for same event
                groups = dedup.group_announcements(user_announcements)
                original_count = len(user_announcements)
                grouped_announcements = []

                for signature, group_anns in groups.items():
                    best_ann = dedup.select_best_announcement_from_group(group_anns)
                    if best_ann:
                        grouped_announcements.append(best_ann)

                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        if len(group_anns) > 1:
                            logger.info(f"🔗 ENHANCED: Grouped {len(group_anns)} announcements for {signature}")
                            logger.info(f"🔗 ENHANCED: Selected best: {best_ann.get('headline', '')[:60]}...")
                            for skipped_ann in group_anns:
                                if skipped_ann['news_id'] != best_ann['news_id']:
                                    logger.info(f"🔗 ENHANCED: Skipped duplicate: {skipped_ann.get('headline', '')[:60]}...")

                user_announcements = grouped_announcements

                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    if original_count > len(user_announcements):
                        logger.info(f"🎯 ENHANCED: Smart grouping reduced {original_count} announcements to {len(user_announcements)} for user {uid[:8]}")

                totals["total_announcements_found"] += len(user_announcements)

                user_notifications_sent = 0
                user_duplicates_prevented = 0

                # Get user's telegram recipients for BSE (no notification_types column exists)
                recipients_response = sb_service.table('telegram_recipients').select('*').eq('user_id', uid).execute()

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

                        # Send actual Telegram notifications with PDF and AI analysis
                        notifications_sent = 0
                        try:
                            from database import (TELEGRAM_API_URL, PDF_BASE_URL, BSE_HEADERS)
                            from ai_service import analyze_pdf_bytes_with_gemini, format_structured_telegram_message
                            from requests import post
                            import requests

                            # 1. Send text message
                            text_message = (
                                f"📰 BSE Announcement\n"
                                f"🏢 {company_name} ({scrip_code})\n"
                                f"📅 {ann_dt.strftime('%d-%m-%Y %H:%M')} IST\n"
                                f"📋 {headline}\n"
                                f"📁 Category: {category}"
                            )

                            if pdf_name:
                                text_message += f"\n📄 PDF Available: {pdf_name}"

                            # Send to each recipient
                            for recipient in recipients:
                                chat_id = recipient.get('chat_id')
                                user_name = recipient.get('user_name', 'User')

                                if chat_id:
                                    personalized_message = f"👤 {user_name}\n" + "─" * 20 + "\n" + text_message

                                    response = post(f"{TELEGRAM_API_URL}/sendMessage",
                                                json={'chat_id': chat_id,
                                                     'text': personalized_message,
                                                     'parse_mode': 'HTML'},
                                                timeout=10)

                                    if response.status_code == 200:
                                        notifications_sent += 1
                                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                                            logger.info(f"📢 ENHANCED: Sent text {news_id} to {user_name} ({chat_id})")
                                    else:
                                        logger.error(f"❌ ENHANCED: Failed to send text to {user_name} - HTTP {response.status_code}")

                            # 2. Send PDF document (if available)
                            if pdf_name:
                                if os.environ.get('BSE_VERBOSE', '0') == '1':
                                    logger.info(f"📄 ENHANCED: Attempting to send PDF for {pdf_name}")

                                pdf_url = f"{PDF_BASE_URL}{pdf_name}"
                                try:
                                    pdf_response = requests.get(pdf_url, headers=BSE_HEADERS, timeout=30)
                                    if pdf_response.status_code == 200 and pdf_response.content:

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

                            # 3. Send AI analysis (if PDF available and passes smart filter)
                            if pdf_name and os.environ.get('ENABLE_AI_ANALYSIS', 'false').lower() == 'true':
                                try:
                                    from ai_service import should_run_ai_analysis, is_quarterly_results_document

                                    # Smart filtering: Check if this announcement deserves AI analysis
                                    should_analyze = should_run_ai_analysis(headline, category)
                                    is_quarterly = is_quarterly_results_document(headline, category)

                                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                                        analysis_reason = "SMART FILTER: " + (
                                            "Financial Results" if "financial" in headline.lower() or "result" in headline.lower() or category == 'financials' else
                                            "Business Development" if any(term in headline.lower() for term in ["order", "contract", "bid", "win", "bagged", "secured"]) else
                                            "Strategic Announcement" if any(term in headline.lower() for term in ["merger", "acquisition", "partnership", "expansion"]) else
                                            "Other"
                                        )
                                        logger.info(f"🧠 ENHANCED: AI Analysis Filter - {pdf_name}")
                                        logger.info(f"🧠 ENHANCED: Headline: '{headline}'")
                                        logger.info(f"🧠 ENHANCED: Category: '{category}'")
                                        logger.info(f"🧠 ENHANCED: Should Analyze: {should_analyze} - {analysis_reason}")

                                    if should_analyze:
                                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                                            logger.info(f"🤖 ENHANCED: Starting AI analysis for {pdf_name}")

                                        # Get PDF for AI analysis
                                        pdf_response = requests.get(pdf_url, headers=BSE_HEADERS, timeout=30)
                                        if pdf_response.status_code == 200 and pdf_response.content:

                                            analysis_result = analyze_pdf_bytes_with_gemini(
                                                pdf_response.content,
                                                pdf_name,
                                                str(scrip_code)
                                            )

                                            if analysis_result:
                                                ai_message = format_structured_telegram_message(
                                                    analysis_result,
                                                    str(scrip_code),
                                                    headline,
                                                    ann_dt,
                                                    is_quarterly=is_quarterly
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
                                                if os.environ.get('BSE_VERBOSE', '0') == '1':
                                                    logger.info(f"🤖 ENHANCED: AI analysis returned no results for {pdf_name}")
                                        else:
                                            logger.error(f"❌ ENHANCED: Failed to fetch PDF for AI analysis - HTTP {pdf_response.status_code}")
                                    else:
                                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                                            logger.info(f"🚫 ENHANCED: Skipped AI analysis for {pdf_name} - Not financial or business development")

                                except Exception as ai_error:
                                    logger.error(f"❌ ENHANCED: Error in AI analysis filtering: {ai_error}")

                        except Exception as e:
                            logger.error(f"❌ ENHANCED: Error sending notifications for {news_id}: {e}")

                        user_notifications_sent += notifications_sent
                        totals["notifications_sent"] += notifications_sent

                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            logger.info(f"📢 ENHANCED: Sent {news_id} to {uid[:8]} ({notifications_sent} notifications)")

                        # Mark as sent in existing table AFTER all notifications are successfully sent
                        # Convert datetime object to ISO string for database storage
                        ann_dt_str = ann_dt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(ann_dt, 'strftime') else str(ann_dt)
                        marked = dedup.mark_announcement_sent(
                            sb_service, uid, news_id, headline, company_name, ann_dt_str, category, scrip_code, pdf_name
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