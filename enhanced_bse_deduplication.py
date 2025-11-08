"""
Enhanced BSE Deduplication using Existing seen_announcements Table
Improves the existing database-first approach without creating new tables
"""

import os
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
from modular_config import get_config

logger = logging.getLogger(__name__)

class EnhancedBSEDeduplication:
    """
    Enhanced BSE deduplication using existing seen_announcements table
    Adds stronger content-based deduplication to existing system
    """

    def __init__(self):
        self.config = get_config()
        self._verbose = self.config.BSE_VERBOSE

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

    def is_announcement_already_sent(self, sb, user_id: str, news_id: str, headline: str,
                                   company_name: str, ann_dt: str, category: str,
                                   scrip_code: str) -> Tuple[bool, str]:
        """
        Enhanced duplicate checking using existing seen_announcements table

        Returns:
            (already_sent, reason)
        """
        try:
            if self._verbose:
                logger.info(f"🔍 ENHANCED BSE: Checking {news_id} for user {user_id[:8]}")

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
                        existing_ann_dt == ann_dt and
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
            # Use existing database function
            from database import db_save_seen_announcement

            caption = f"Company: {company_name}, Category: {category}"
            db_save_seen_announcement(
                sb, user_id, news_id, str(scrip_code),
                headline, pdf_name, ann_dt, caption, category
            )

            if self._verbose:
                logger.info(f"✅ ENHANCED BSE: Marked {news_id} sent to {user_id[:8]} in existing table")
            return True

        except Exception as e:
            logger.error(f"ENHANCED BSE: Error marking announcement sent: {e}")
            return False

    def get_deduplication_stats(self, sb) -> Dict:
        """Get statistics from existing seen_announcements table"""
        try:
            # Get total records
            total_response = sb.table('seen_announcements').select('id', count='exact').execute()
            total_count = getattr(total_response, 'count', 0) or 0

            # Get records from last 24 hours
            yesterday = (datetime.now() - timedelta(hours=24)).isoformat()
            recent_response = sb.table('seen_announcements').select('id', count='exact').gte('created_at', yesterday).execute()
            recent_count = getattr(recent_response, 'count', 0) or 0

            # Get unique users today
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            users_response = sb.table('seen_announcements').select('user_id', count='exact').gte('created_at', today).execute()
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