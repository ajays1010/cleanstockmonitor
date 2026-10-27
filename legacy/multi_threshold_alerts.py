#!/usr/bin/env python3
"""
Multi-Threshold Price Spike Alert System
Sends notifications at progressive thresholds: 5%, 10%, 15%, 20%
Each threshold triggers only once per direction per day
"""

import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class MultiThresholdAlertTracker:
    """
    Tracks price spike alerts across multiple thresholds
    Each threshold (5%, 10%, 15%, 20%) triggers once per direction per day
    """

    def __init__(self):
        # Threshold levels in percentage
        self.thresholds = [5.0, 10.0, 15.0, 20.0]

        # Memory-based tracking for performance
        self._daily_tracker: Dict[str, Set[str]] = {}  # user_id_stock_date -> set of sent thresholds
        self._last_cleanup = datetime.now()
        self._cleanup_interval = 3600  # 1 hour

        # Verbose logging
        self._verbose = os.environ.get('ALERT_VERBOSE', '0') == '1'

        if self._verbose:
            print(f"🔔 Multi-Threshold Alert Tracker: Initialized with thresholds {self.thresholds}%")

    def _get_tracking_key(self, user_id: str, bse_code: str, alert_date: str) -> str:
        """Generate tracking key for user-stock-date combination"""
        return f"{user_id}_{bse_code}_{alert_date}"

    def _cleanup_old_entries(self):
        """Clean up entries older than today to prevent memory growth"""
        now = datetime.now()
        if now - self._last_cleanup < timedelta(seconds=self._cleanup_interval):
            return

        self._last_cleanup = now
        today = date.today().isoformat()

        # Remove entries older than today
        keys_to_remove = []
        for key in self._daily_tracker.keys():
            parts = key.split('_')
            if len(parts) >= 3:
                key_date = parts[-2] + '_' + parts[-1]  # Handle dates with hyphens
                if key_date != today:
                    keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._daily_tracker[key]

        if self._verbose and keys_to_remove:
            print(f"🧹 Alert Tracker: Cleaned {len(keys_to_remove)} old entries")

    def _get_threshold_level(self, price_change_pct: float) -> Optional[float]:
        """Get the highest threshold crossed by the price change"""
        abs_change = abs(price_change_pct)

        # Find the highest threshold crossed
        for threshold in sorted(self.thresholds, reverse=True):
            if abs_change >= threshold:
                return threshold

        return None

    def _get_alert_type(self, price_change_pct: float, threshold: float) -> str:
        """Generate alert type for tracking"""
        direction = "up" if price_change_pct > 0 else "down"
        return f"price_{direction}_{int(threshold)}pct"

    def should_send_alert(self, user_client, user_id: str, bse_code: str,
                         price_change_pct: float, check_db: bool = True) -> Tuple[bool, Optional[float], str]:
        """
        Check if alert should be sent for this price change

        Returns:
            (should_send, threshold_crossed, alert_type)
        """
        current_date = date.today().isoformat()

        # Clean up old entries
        self._cleanup_old_entries()

        # Get threshold level
        threshold = self._get_threshold_level(price_change_pct)
        if threshold is None:
            return False, None, "no_threshold"

        # Generate alert type
        alert_type = self._get_alert_type(price_change_pct, threshold)

        # Check memory tracker first (faster)
        tracking_key = self._get_tracking_key(user_id, bse_code, current_date)
        if tracking_key in self._daily_tracker:
            sent_thresholds = self._daily_tracker[tracking_key]
            if alert_type in sent_thresholds:
                if self._verbose:
                    print(f"🔔 ALERT DUPLICATE: {alert_type} already sent for {user_id[:8]}-{bse_code}")
                return False, threshold, f"already_sent_{alert_type}"

        # Check database if requested (fallback/synchronization)
        if check_db:
            try:
                if self._has_sent_db_alert(user_client, user_id, bse_code, alert_type, current_date):
                    if self._verbose:
                        print(f"🔔 ALERT DUPLICATE (DB): {alert_type} found in database for {user_id[:8]}-{bse_code}")
                    # Add to memory tracker for consistency
                    self._mark_alert_sent_memory(user_id, bse_code, current_date, alert_type)
                    return False, threshold, f"db_duplicate_{alert_type}"
            except Exception as e:
                logger.warning(f"Database check failed for alert tracking: {e}")

        # Alert should be sent
        if self._verbose:
            direction = "↑" if price_change_pct > 0 else "↓"
            print(f"🚨 ALERT TRIGGER: {bse_code} {direction} {abs(price_change_pct):.1f}% (threshold: {threshold}%) for {user_id[:8]}")

        return True, threshold, alert_type

    def _has_sent_db_alert(self, user_client, user_id: str, bse_code: str,
                          alert_type: str, alert_date: str) -> bool:
        """Check if alert was sent today (database check)"""
        try:
            resp = (
                user_client.table('daily_alerts_sent')
                .select('user_id', count='exact')
                .eq('user_id', user_id)
                .eq('bse_code', str(bse_code))
                .eq('alert_date', alert_date)
                .eq('alert_type', alert_type)
                .execute()
            )
            return (getattr(resp, 'count', 0) or 0) > 0
        except Exception:
            return False

    def mark_alert_sent(self, user_client, user_id: str, bse_code: str,
                       price_change_pct: float, threshold: float):
        """Mark alert as sent in both memory and database"""
        current_date = date.today().isoformat()
        alert_type = self._get_alert_type(price_change_pct, threshold)

        # Mark in memory
        self._mark_alert_sent_memory(user_id, bse_code, current_date, alert_type)

        # Mark in database
        self._mark_alert_sent_db(user_client, user_id, bse_code, alert_type, current_date)

        if self._verbose:
            direction = "↑" if price_change_pct > 0 else "↓"
            print(f"✅ ALERT RECORDED: {bse_code} {direction} {threshold}% for {user_id[:8]}")

    def _mark_alert_sent_memory(self, user_id: str, bse_code: str,
                               alert_date: str, alert_type: str):
        """Mark alert as sent in memory tracker"""
        tracking_key = self._get_tracking_key(user_id, bse_code, alert_date)

        if tracking_key not in self._daily_tracker:
            self._daily_tracker[tracking_key] = set()

        self._daily_tracker[tracking_key].add(alert_type)

    def _mark_alert_sent_db(self, user_client, user_id: str, bse_code: str,
                           alert_type: str, alert_date: str):
        """Mark alert as sent in database"""
        try:
            user_client.table('daily_alerts_sent').insert({
                'user_id': user_id,
                'bse_code': str(bse_code),
                'alert_date': alert_date,
                'alert_type': alert_type,
            }).execute()
        except Exception as e:
            logger.warning(f"Failed to record alert in database: {e}")

    def get_tracking_stats(self) -> Dict:
        """Get current tracking statistics"""
        current_date = date.today().isoformat()

        # Count active tracking entries
        active_entries = 0
        threshold_counts = {threshold: 0 for threshold in self.thresholds}

        for key, sent_thresholds in self._daily_tracker.items():
            parts = key.split('_')
            if len(parts) >= 3:
                key_date = parts[-2] + '_' + parts[-1]
                if key_date == current_date:
                    active_entries += len(sent_thresholds)

                    # Count thresholds
                    for alert_type in sent_thresholds:
                        if "price_" in alert_type:
                            try:
                                threshold_pct = int(alert_type.split('_')[-1].replace('pct', ''))
                                if threshold_pct in threshold_counts:
                                    threshold_counts[threshold_pct] += 1
                            except:
                                pass

        return {
            'active_alerts_today': active_entries,
            'threshold_counts': threshold_counts,
            'total_tracking_keys': len(self._daily_tracker),
            'thresholds_configured': self.thresholds,
            'last_cleanup': self._last_cleanup.isoformat()
        }

    def clear_user_tracking(self, user_id: str):
        """Clear tracking for specific user (useful for testing)"""
        keys_to_remove = []
        for key in self._daily_tracker.keys():
            if key.startswith(f"{user_id}_"):
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._daily_tracker[key]

        if self._verbose:
            print(f"🗑️ Alert Tracker: Cleared {len(keys_to_remove)} entries for user {user_id[:8]}")

    def get_today_alerts_for_user(self, user_id: str, bse_code: str = None) -> List[str]:
        """Get alerts already sent today for a user (and optionally stock)"""
        current_date = date.today().isoformat()
        sent_alerts = []

        for key, sent_thresholds in self._daily_tracker.items():
            parts = key.split('_')
            if len(parts) >= 3 and parts[0] == user_id:
                key_date = parts[-2] + '_' + parts[-1]
                if key_date == current_date:
                    if bse_code is None or parts[1] == bse_code:
                        sent_alerts.extend(sent_thresholds)

        return list(set(sent_alerts))  # Remove duplicates

# Global tracker instance
_tracker_instance = None

def get_alert_tracker():
    """Get the global alert tracker instance"""
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = MultiThresholdAlertTracker()
    return _tracker_instance

def should_send_price_alert(user_client, user_id: str, bse_code: str,
                           price_change_pct: float) -> Tuple[bool, Optional[float], str]:
    """
    Convenience function to check if price alert should be sent

    Returns:
        (should_send, threshold_crossed, alert_type)
    """
    tracker = get_alert_tracker()
    return tracker.should_send_alert(user_client, user_id, bse_code, price_change_pct)

def mark_price_alert_sent(user_client, user_id: str, bse_code: str,
                         price_change_pct: float, threshold: float):
    """Convenience function to mark price alert as sent"""
    tracker = get_alert_tracker()
    tracker.mark_alert_sent(user_client, user_id, bse_code, price_change_pct, threshold)

def get_alert_tracking_stats() -> Dict:
    """Get alert tracking statistics"""
    tracker = get_alert_tracker()
    return tracker.get_tracking_stats()

# Flask app integration
def add_alert_endpoints(app):
    """Add multi-threshold alert endpoints to Flask app"""

    @app.route('/debug/alert_stats')
    def alert_stats():
        """Get multi-threshold alert statistics"""
        try:
            stats = get_alert_tracking_stats()
            return {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'stats': stats
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }, 500

    @app.route('/admin/alert_cleanup', methods=['POST'])
    def alert_cleanup():
        """Force cleanup alert tracking"""
        try:
            tracker = get_alert_tracker()
            tracker.force_cleanup()
            return {
                'status': 'success',
                'message': 'Alert tracking cleaned up',
                'stats': tracker.get_tracking_stats()
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }, 500

    @app.route('/debug/user_alerts/<user_id>')
    def user_alerts(user_id):
        """Get today's alerts for a user"""
        try:
            bse_code = request.args.get('bse_code')
            alerts = get_alert_tracker().get_today_alerts_for_user(user_id, bse_code)
            return {
                'status': 'success',
                'user_id': user_id,
                'bse_code': bse_code,
                'alerts_sent_today': alerts,
                'count': len(alerts)
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }, 500