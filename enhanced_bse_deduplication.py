"""
Enhanced BSE Deduplication using Existing seen_announcements Table
Improves the existing database-first approach without creating new tables
"""

import os
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class EnhancedBSEDeduplication:
    """
    Enhanced BSE deduplication using existing seen_announcements table
    Adds stronger content-based deduplication to existing system
    """

    def __init__(self):
        self._verbose = os.environ.get('BSE_VERBOSE', '0') == '1'

        if self._verbose:
            logger.info("🔧 ENHANCED BSE DEDUPLICATION: Using existing seen_announcements table")

    def get_supabase_client(self, service_role: bool = False):
        """Get Supabase client"""
        try:
            from database import get_supabase_client
            return get_supabase_client(service_role=service_role)
        except Exception as e:
            logger.error(f"ENHANCED BSE: Failed to get Supabase client: {e}")
            return None

    def _generate_content_hash(self, headline: str, company_name: str, ann_dt: str, category: str) -> str:
        """Generate content hash for deduplication"""
        content_parts = [
            str(headline or '').strip().lower(),
            str(company_name or '').strip().lower(),
            str(ann_dt or '').strip(),
            str(category or '').strip().lower()
        ]
        content = '|'.join(content_parts)
        content = ' '.join(content.split())  # Normalize whitespace
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def is_result_notification_in_cooling_period(self, sb, user_id: str, scrip_code: str,
                                               headline: str, category: str) -> Tuple[bool, str]:
        """
        Check if result notifications should be skipped due to recent notification

        Prevents notification fatigue by implementing cooling periods:
        - 3 hours cooling period for any result notifications for same script
        - 1 week cooling period for financial results specifically
        """
        try:
            from datetime import datetime, timedelta

            # Check if this is a result-related announcement
            h = headline.lower()
            result_indicators = [
                "financial results", "quarter ended", "half year ended", "year ended",
                "quarterly results", "unaudited results", "audited results",
                "board meeting", "profit", "loss", "revenue", "dividend"
            ]

            is_result_related = any(indicator in h for indicator in result_indicators)

            if not is_result_related:
                return False, "not_result_related"

            # Determine cooling period
            is_financial_result = any(term in h for term in ["financial results", "quarter ended", "half year ended"])

            if is_financial_result:
                # 1 week cooling for financial results
                cooling_hours = 24 * 7  # 7 days
                cooling_type = "financial_results_week"
            else:
                # 3 hours cooling for other result-related announcements
                cooling_hours = 3
                cooling_type = "result_3_hours"

            # Check for recent notifications in cooling period
            cooling_since = (datetime.now() - timedelta(hours=cooling_hours)).isoformat()

            # Query recent notifications for this script and user
            recent_response = sb.table('seen_announcements').select('*')\
                .eq('user_id', user_id)\
                .eq('scrip_code', str(scrip_code))\
                .gte('created_at', cooling_since)\
                .execute()

            if recent_response.data and len(recent_response.data) > 0:
                # Check if any recent notifications were result-related
                for record in recent_response.data:
                    recent_headline = record.get('headline', '').lower()
                    recent_is_result = any(indicator in recent_headline for indicator in result_indicators)

                    if recent_is_result:
                        created_at = record.get('created_at', '')
                        if self._verbose:
                            logger.info(f"❄️ ENHANCED: Result notification in cooling period for {scrip_code}")
                            logger.info(f"❄️ ENHANCED: Recent result sent at {created_at}, cooling type: {cooling_type}")
                        return True, f"cooling_period_{cooling_type}"

            return False, "outside_cooling_period"

        except Exception as e:
            logger.error(f"ENHANCED BSE: Error checking cooling period: {e}")
            # If cooling check fails, allow sending
            return False, "cooling_check_failed"

    def is_announcement_already_sent(self, sb, user_id: str, news_id: str, headline: str,
                                   company_name: str, ann_dt: str, category: str,
                                   scrip_code: str) -> Tuple[bool, str]:
        """
        Enhanced duplicate checking using existing seen_announcements table

        Returns:
            (already_sent, reason)
        """
        # Convert datetime to string for consistent comparison
        ann_dt_str = ann_dt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(ann_dt, 'strftime') else str(ann_dt)

        try:
            if self._verbose:
                logger.info(f"🔍 ENHANCED BSE: Checking {news_id} for user {user_id[:8]}")

            # NEW: Check cooling period for result notifications
            is_cooling, cooling_reason = self.is_result_notification_in_cooling_period(
                sb, user_id, scrip_code, headline, category
            )

            if is_cooling:
                return True, f"cooling_period_{cooling_reason}"

            # 1. Use existing exact match check (already implemented)
            from database import db_seen_announcement_exists
            if db_seen_announcement_exists(sb, user_id, news_id):
                return True, "exact_match_existing_table"

            # 2. Enhanced content-based check (NEW - uses existing table)
            content_hash = self._generate_content_hash(headline, company_name, ann_dt, category)

            # Look for similar content in last 24 hours using headline pattern matching
            # (since we can't add new columns, we use pattern matching on existing fields)
            twenty_four_hours_ago = (datetime.now() - timedelta(hours=24)).isoformat()

            # Search for similar headlines (contains matching)
            similar_headline = f"%{headline.strip()[:50]}%"  # First 50 chars
            response = sb.table('seen_announcements').select('*')\
                .eq('user_id', user_id)\
                .eq('scrip_code', str(scrip_code))\
                .like('headline', similar_headline)\
                .gte('created_at', twenty_four_hours_ago)\
                .execute()

            if response.data and len(response.data) > 0:
                # Additional validation: check if it's really the same announcement
                for record in response.data:
                    existing_headline = record.get('headline', '').strip()
                    existing_ann_dt = record.get('ann_date', '').strip()
                    existing_category = record.get('category', '').strip()

                    # Compare key fields
                    if (existing_headline.lower() == headline.lower() and
                        existing_ann_dt == ann_dt_str and
                        existing_category.lower() == category.lower()):
                        if self._verbose:
                            logger.info(f"🚫 ENHANCED BSE: Content match found for {news_id}")
                        return True, f"content_match_{record.get('created_at', 'unknown')}"

            # 3. Check for same news_id sent to ANY user in last 6 hours (global deduplication)
            six_hours_ago = (datetime.now() - timedelta(hours=6)).isoformat()
            global_response = sb.table('seen_announcements').select('*')\
                .eq('news_id', news_id)\
                .gte('created_at', six_hours_ago)\
                .execute()

            if global_response.data and len(global_response.data) > 0:
                other_users = [f"{r.get('user_id', '')[:8]}" for r in global_response.data]
                if self._verbose:
                    logger.info(f"🚫 ENHANCED BSE: News {news_id} already sent to users {other_users}")
                return True, f"sent_to_other_users_{len(other_users)}"

            # 4. Check if very recent similar announcement exists (race condition prevention)
            five_minutes_ago = (datetime.now() - timedelta(minutes=5)).isoformat()
            recent_response = sb.table('seen_announcements').select('*')\
                .eq('user_id', user_id)\
                .eq('scrip_code', str(scrip_code))\
                .gte('created_at', five_minutes_ago)\
                .execute()

            if recent_response.data and len(recent_response.data) > 2:  # If more than 2 recent announcements
                if self._verbose:
                    logger.info(f"🚫 ENHANCED BSE: Too many recent announcements for user {user_id[:8]}")
                return True, "rate_limiting_protection"

            # No duplicate found
            if self._verbose:
                logger.info(f"✅ ENHANCED BSE: No duplicate found for {news_id} to {user_id[:8]} - safe to send")
            return False, "no_duplicate_found"

        except Exception as e:
            logger.error(f"ENHANCED BSE: Error checking duplicates: {e}")
            # If database check fails, allow sending (better than missing announcements)
            return False, "database_check_failed"

    def mark_announcement_sent(self, sb, user_id: str, news_id: str, headline: str,
                             company_name: str, ann_dt: str, category: str,
                             scrip_code: str, pdf_name: str = "") -> bool:
        """
        Mark announcement as sent using existing db_save_seen_announcement function
        """
        try:
            # Convert datetime to string for database storage
            ann_dt_str = ann_dt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(ann_dt, 'strftime') else str(ann_dt)

            # Use existing database function
            from database import db_save_seen_announcement

            caption = f"Company: {company_name}, Category: {category}"
            db_save_seen_announcement(
                sb, user_id, news_id, str(scrip_code),
                headline, pdf_name, ann_dt_str, caption, category
            )

            if self._verbose:
                logger.info(f"ENHANCED BSE: Marked {news_id} sent to {user_id[:8]} in existing table")
            return True

        except Exception as e:
            logger.error(f"ENHANCED BSE: Error marking announcement sent: {e}")
            return False

    def generate_content_signature(self, headline: str, scrip_code: str, ann_dt) -> str:
        """
        Generate a content signature to group similar announcements.

        Returns a signature that groups announcements about the same event:
        - Financial results for the same period
        - Board meetings for the same date
        - Contract wins for similar projects
        """
        if not headline:
            return f"empty_{scrip_code}"

        h = headline.lower()

        # 1. FINANCIAL RESULTS - Group by financial period
        financial_periods = [
            "quarter ended", "half year ended", "year ended", "quarterly", "annual",
            "financial results", "unaudited", "audited", "standalone", "consolidated"
        ]

        if any(period in h for period in financial_periods):
            # Extract date patterns from financial announcements
            import re

            # Look for date patterns like "30th september 2025", "31-march-2025", "30.09.2025"
            date_patterns = [
                r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4})',
                r'(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})',
                r'(\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2})'
            ]

            extracted_dates = []
            for pattern in date_patterns:
                matches = re.findall(pattern, h)
                extracted_dates.extend(matches)

            # Create signature based on extracted dates
            if extracted_dates:
                # Normalize dates and sort
                normalized_dates = []
                for date_str in extracted_dates:
                    # Remove ordinal suffixes and normalize separators
                    clean_date = re.sub(r'(st|nd|rd|th)', '', date_str.lower())
                    clean_date = re.sub(r'[./]', '-', clean_date)
                    normalized_dates.append(clean_date)

                dates_sig = '_'.join(sorted(set(normalized_dates)))
                return f"financial_{scrip_code}_{dates_sig}"
            else:
                return f"financial_{scrip_code}_unknown_period"

        # 2. BOARD MEETINGS - Group by meeting date
        if "board meeting" in h:
            # Extract meeting date
            import re

            meeting_date_patterns = [
                r'(\d{1,2}(?:st|nd|rd|th)?\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4})',
                r'(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{4})'
            ]

            for pattern in meeting_date_patterns:
                matches = re.findall(pattern, h)
                if matches:
                    # Use first meeting date found
                    meeting_date = matches[0].lower()
                    meeting_date = re.sub(r'(st|nd|rd|th)', '', meeting_date)
                    meeting_date = re.sub(r'[./]', '-', meeting_date)
                    return f"board_meeting_{scrip_code}_{meeting_date}"

            return f"board_meeting_{scrip_code}_unknown_date"

        # 3. CONTRACTS/WINS - Group by contract type and approximate value/time
        contract_terms = ["order", "contract", "bagged", "secured", "won", "received"]
        if any(term in h for term in contract_terms):
            # Extract key contract identifiers
            contract_patterns = [
                r'(rs\.?\s*\d+(?:\,\d{3})*(?:\.\d+)?\s*(?:crore|lakh|million|billion))',
                r'(worth\s+rs\.?\s*\d+(?:\,\d{3})*(?:\.\d+)?)',
                r'(\d+(?:\,\d{3})*(?:\.\d+)?\s*(?:crore|lakh|million|billion))',
            ]

            contract_value = "unknown_value"
            for pattern in contract_patterns:
                matches = re.findall(pattern, h)
                if matches:
                    contract_value = re.sub(r'[^\w\d]', '_', matches[0].lower())
                    break

            return f"contract_{scrip_code}_{contract_value}"

        # 4. DEFAULT - Use headline hash for other announcements
        import hashlib
        headline_hash = hashlib.md5(headline.encode()).hexdigest()[:8]
        return f"other_{scrip_code}_{headline_hash}"

    def group_announcements(self, announcements: list) -> dict:
        """
        Group announcements by content signature to identify duplicates.

        Returns:
            dict: {signature: [announcements_in_group]}
        """
        groups = {}

        for ann in announcements:
            headline = ann.get('headline', '')
            scrip_code = ann.get('scrip_code', '')
            ann_dt = ann.get('ann_dt')

            signature = self.generate_content_signature(headline, scrip_code, ann_dt)

            if signature not in groups:
                groups[signature] = []
            groups[signature].append(ann)

        return groups

    def select_best_announcement_from_group(self, group: list) -> dict:
        """
        Select the best announcement from a group of similar announcements.

        Priority:
        1. Announcement with PDF (most comprehensive)
        2. Earlier announcement (usually the most complete)
        3. Longer headline (more details)
        """
        if not group:
            return None

        if len(group) == 1:
            return group[0]

        # Sort by priority
        def announcement_priority(ann):
            has_pdf = 1 if ann.get('pdf_name') else 0
            headline_length = len(ann.get('headline', ''))
            # Earlier announcements get priority (inverse timestamp)
            try:
                ann_dt = ann.get('ann_dt')
                if hasattr(ann_dt, 'timestamp'):
                    earlier = -ann_dt.timestamp()
                else:
                    earlier = 0  # Fallback
            except:
                earlier = 0

            return (has_pdf, earlier, headline_length)

        # Sort by priority and return the best one
        sorted_group = sorted(group, key=announcement_priority, reverse=True)
        return sorted_group[0]

    def get_deduplication_stats(self, sb) -> Dict:
        """Get statistics from existing seen_announcements table"""
        try:
            # Get total records - use safer query approach
            total_response = sb.table('seen_announcements').select('user_id').execute()
            total_count = len(total_response.data) if total_response.data else 0

            # Get records from last 24 hours
            yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
            recent_response = sb.table('seen_announcements').select('user_id').gte('created_at', yesterday).execute()
            recent_count = len(recent_response.data) if recent_response.data else 0

            # Get unique users today
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            users_response = sb.table('seen_announcements').select('user_id').gte('created_at', today).execute()
            unique_users = len(set([r.get('user_id') for r in users_response.data])) if users_response.data else 0

            return {
                'total_announcements_sent': total_count,
                'last_24_hours': recent_count,
                'unique_users_today': unique_users,
                'table_name': 'seen_announcements (enhanced)',
                'enhancement_type': 'content_based_deduplication'
            }

        except Exception as e:
            logger.error(f"ENHANCED BSE: Error getting stats: {e}")
            return {
                'total_announcements_sent': 0,
                'last_24_hours': 0,
                'unique_users_today': 0,
                'error': str(e)
            }

    def cleanup_old_records(self, sb, days_to_keep: int = 30):
        """Clean up old records using existing table"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()

            # Delete records older than cutoff date
            response = sb.table('seen_announcements').delete().lt('created_at', cutoff_date).execute()

            deleted_count = len(response.data) if response.data else 0
            if self._verbose and deleted_count > 0:
                logger.info(f"🧹 ENHANCED BSE: Cleaned up {deleted_count} old records from seen_announcements")

            return deleted_count

        except Exception as e:
            logger.error(f"ENHANCED BSE: Error cleaning up old records: {e}")
            return 0

# Singleton instance
_enhanced_bse_deduplication = None

def get_enhanced_bse_deduplication():
    """Get global enhanced BSE deduplication instance"""
    global _enhanced_bse_deduplication
    if _enhanced_bse_deduplication is None:
        _enhanced_bse_deduplication = EnhancedBSEDeduplication()
    return _enhanced_bse_deduplication