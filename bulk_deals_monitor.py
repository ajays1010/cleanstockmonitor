#!/usr/bin/env python3
"""
Bulk/Block Deals Monitor - Production Module

This module provides bulk and block deals monitoring functionality for integration
with the BSE monitoring application. It fetches deals from NSE and BSE websites
and filters them based on user's monitored stocks.

Features:
- Scrapes NSE and BSE bulk/block deals
- Filters deals by user's monitored stocks  
- Integrates with existing Telegram notification system
- Stores seen deals to prevent duplicates
- Provides formatted messages for Telegram
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import json
import time
import os
from typing import List, Dict, Optional
import re
import logging

class BulkBlockDealsMonitor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def fetch_nse_deals(self) -> List[Dict]:
        """
        Fetch bulk and block deals from NSE
        Returns list of deal dictionaries
        """
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print("🔍 Fetching NSE Bulk/Block Deals...")
        
        try:
            # NSE requires session establishment
            main_url = "https://www.nseindia.com"
            self.session.get(main_url, timeout=30)
            time.sleep(1)
            
            today = datetime.now().strftime("%d-%m-%Y")
            api_endpoints = [
                f"https://www.nseindia.com/api/historical/bulk-deals?date={today}",
                f"https://www.nseindia.com/api/historical/block-deals?date={today}",
            ]
            
            deals = []
            for api_url in api_endpoints:
                try:
                    response = self.session.get(api_url, timeout=30)
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            if isinstance(data, dict) and 'data' in data:
                                for deal in data['data']:
                                    deals.append({
                                        'source': 'NSE',
                                        'deal_type': 'Bulk' if 'bulk' in api_url.lower() else 'Block',
                                        'security_name': deal.get('symbol', ''),
                                        'company_name': deal.get('symbol', ''),
                                        'script_code': '',  # NSE doesn't provide BSE codes
                                        'client_name': deal.get('clientName', ''),
                                        'buy_sell': deal.get('buySell', ''),
                                        'quantity': self.parse_number(str(deal.get('quantity', 0))),
                                        'price': self.parse_number(str(deal.get('tradePrice', 0))),
                                        'deal_value': 0,
                                        'deal_date': deal.get('date', today),
                                        'exchange': 'NSE'
                                    })
                                break
                        except json.JSONDecodeError:
                            continue
                except Exception:
                    continue
            
            # Calculate deal values
            for deal in deals:
                if deal['quantity'] and deal['price']:
                    deal['deal_value'] = deal['quantity'] * deal['price']
            
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"✅ NSE: Found {len(deals)} deals")
            
            return deals
            
        except Exception as e:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"❌ NSE Error: {e}")
            return []
    
    def fetch_bse_deals(self, deal_type: str = 'bulk') -> List[Dict]:
        """
        Fetch bulk or block deals from BSE
        deal_type: 'bulk' or 'block'
        """
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"🔍 Fetching BSE {deal_type.title()} Deals...")
        
        try:
            if deal_type.lower() == 'block':
                url = "https://www.bseindia.com/markets/equity/EQReports/block_deals.aspx"
            else:
                url = "https://www.bseindia.com/markets/equity/EQReports/bulk_deals.aspx"
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                deals = []
                
                # Find tables with data
                tables = soup.find_all('table')
                table = None
                
                for t in tables:
                    rows = t.find_all('tr')
                    if len(rows) > 3:  # Should have header + data
                        table = t
                        break
                
                if table:
                    rows = table.find_all('tr')
                    
                    for i, row in enumerate(rows):
                        if i == 0:  # Skip header
                            continue
                            
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 5:
                            try:
                                # BSE format: Deal Date, Security Code, Security Name, Client Name, Deal Type, Quantity, [Trade Price]
                                deal_date = cells[0].get_text(strip=True)
                                security_code = cells[1].get_text(strip=True)
                                security_name = cells[2].get_text(strip=True)
                                client_name = cells[3].get_text(strip=True)
                                buy_sell = cells[4].get_text(strip=True)
                                quantity = self.parse_number(cells[5].get_text(strip=True)) if len(cells) > 5 else 0
                                price = self.parse_number(cells[6].get_text(strip=True)) if len(cells) > 6 else 0
                                
                                if not security_name or security_name.lower() in ['no records', 'total', '']:
                                    continue
                                
                                deal = {
                                    'source': 'BSE',
                                    'deal_type': deal_type.title(),
                                    'security_name': security_name,
                                    'company_name': security_name,
                                    'script_code': security_code,
                                    'client_name': client_name,
                                    'buy_sell': buy_sell,
                                    'quantity': quantity,
                                    'price': price,
                                    'deal_value': quantity * price if quantity and price else 0,
                                    'deal_date': deal_date,
                                    'exchange': 'BSE'
                                }
                                
                                deals.append(deal)
                                
                            except Exception as e:
                                continue
                
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"✅ BSE {deal_type.title()}: Found {len(deals)} deals")
                
                return deals
            else:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"❌ BSE {deal_type.title()}: HTTP {response.status_code}")
                return []
                
        except Exception as e:
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"❌ BSE {deal_type.title()} Error: {e}")
            return []
    
    def parse_number(self, text: str) -> float:
        """Parse number from text, handling commas and other formatting"""
        try:
            cleaned = re.sub(r'[^\d.]', '', text.replace(',', ''))
            return float(cleaned) if cleaned else 0.0
        except:
            return 0.0
    
    def filter_deals_by_monitored_stocks(self, deals: List[Dict], monitored_stocks: List[Dict]) -> List[Dict]:
        """
        Filter deals based on user's monitored stocks
        monitored_stocks: List of {'bse_code': str, 'company_name': str}
        """
        if not monitored_stocks:
            return deals
        
        # Create lookup sets
        bse_codes = {str(stock.get('bse_code', '')).strip() for stock in monitored_stocks}
        company_names = {str(stock.get('company_name', '')).strip().upper() for stock in monitored_stocks}
        
        filtered_deals = []
        
        for deal in deals:
            # Match by BSE script code (most reliable)
            script_code = deal.get('script_code', '')
            if script_code and script_code in bse_codes:
                filtered_deals.append(deal)
                continue
            
            # Match by company/security name
            security_name = deal.get('security_name', '').strip().upper()
            company_name = deal.get('company_name', '').strip().upper()
            
            for monitored_name in company_names:
                if (monitored_name in security_name or security_name in monitored_name or
                    monitored_name in company_name or company_name in monitored_name):
                    filtered_deals.append(deal)
                    break
        
        return filtered_deals
    
    def fetch_all_deals(self) -> List[Dict]:
        """Fetch deals from all sources"""
        all_deals = []
        
        # Fetch NSE deals
        nse_deals = self.fetch_nse_deals()
        all_deals.extend(nse_deals)
        time.sleep(2)  # Rate limiting
        
        # Fetch BSE deals
        bse_bulk_deals = self.fetch_bse_deals('bulk')
        all_deals.extend(bse_bulk_deals)
        time.sleep(2)
        
        bse_block_deals = self.fetch_bse_deals('block')
        all_deals.extend(bse_block_deals)
        
        return all_deals
    
    def format_deals_for_telegram(self, deals: List[Dict]) -> str:
        """Format deals for Telegram notification"""
        if not deals:
            return ""
        
        lines = []
        lines.append("💼 Bulk/Block Deals Alert")
        lines.append(f"🕐 {datetime.now().strftime('%d-%m-%Y %H:%M')} IST")
        lines.append("")
        
        for deal in deals:
            deal_emoji = "🔵" if deal['deal_type'] == 'Block' else "🟡"
            buy_sell_emoji = "🟢" if deal.get('buy_sell', '').upper().startswith('B') else "🔴"
            
            lines.append(f"{deal_emoji} {deal['deal_type']} Deal ({deal['source']})")
            lines.append(f"📈 {deal['security_name']}")
            
            if deal.get('script_code'):
                lines.append(f"🔢 Code: {deal['script_code']}")
            
            lines.append(f"👤 Client: {deal['client_name']}")
            lines.append(f"{buy_sell_emoji} {deal.get('buy_sell', 'N/A')}")
            lines.append(f"📦 Quantity: {deal['quantity']:,.0f}")
            
            if deal['price'] > 0:
                lines.append(f"💰 Price: ₹{deal['price']:.2f}")
                lines.append(f"💸 Value: ₹{deal['deal_value']:,.0f}")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def create_deal_id(self, deal: Dict) -> str:
        """Create unique ID for a deal to prevent duplicates"""
        return f"{deal['source']}_{deal.get('script_code', deal['security_name'])}_{deal['client_name']}_{deal['quantity']}_{deal['deal_date']}"

def send_bulk_deals_alerts(user_client, user_id: str, monitored_scrips: List[Dict],
                          telegram_recipients: List[Dict]) -> int:
    """
    Enhanced bulk/block deals alerts for monitored stocks.
    Checks previous trading day's bulk deals and sends notifications.
    Integrates with existing BSE monitoring system.

    Returns: Number of messages sent
    """
    messages_sent = 0

    try:
        # Try to use the enhanced bulk deal fetcher first
        try:
            from bulk_deal_fetcher import BulkDealFetcher, format_bulk_deal_message

            fetcher = BulkDealFetcher()

            # Get previous trading day (bulk deals are available for previous day)
            previous_trading_day = fetcher.get_previous_trading_day()

            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"Bulk Deals: Checking deals for {previous_trading_day} (previous trading day)")

            # Fetch all bulk deals for previous trading day
            all_bulk_deals = fetcher.fetch_all_bulk_deals(previous_trading_day)

            if not all_bulk_deals:
                print(f"Bulk Deals: No deals found for {previous_trading_day}")
                return 0

            # Filter deals for monitored stocks only
            monitored_deals = fetcher.filter_monitored_stocks(all_bulk_deals, monitored_scrips)

            if not monitored_deals:
                print(f"Bulk Deals: No deals found for monitored stocks on {previous_trading_day}")
                return 0

            # Check if we already sent bulk deal alerts for this date
            if _has_sent_bulk_alerts_today(user_client, user_id, previous_trading_day):
                print(f"Bulk Deals: Alerts already sent for {previous_trading_day}")
                return 0

            # Format message
            message = format_bulk_deal_message(monitored_deals)

            if not message:
                print("Bulk Deals: Failed to format message")
                return 0

            # Add header with trading date
            from datetime import datetime
            header = f"📈 Bulk Deals Alert - {previous_trading_day.strftime('%d %b %Y')}\n"
            full_message = header + message

            # Send to all recipients
            for recipient in telegram_recipients:
                try:
                    user_name = recipient.get('user_name', 'User')
                    send_telegram_message_with_user_name(recipient['chat_id'], full_message, user_name)
                    messages_sent += 1
                    print(f"Bulk Deals: Sent alert to {user_name}")
                except Exception as e:
                    print(f"Bulk Deals: Failed to send to {recipient.get('user_name', 'Unknown')}: {e}")

            # Record that we sent alerts for this date
            _record_bulk_alert_sent(user_client, user_id, previous_trading_day, len(monitored_deals))

            print(f"Bulk Deals: Sent {messages_sent} alerts for {len(monitored_deals)} deals on {previous_trading_day}")
            return messages_sent

        except ImportError:
            # Fallback to original implementation if enhanced system not available
            print("Bulk Deals: Enhanced system not available, using fallback")
            monitor = BulkBlockDealsMonitor()

            # Fetch all deals
            all_deals = monitor.fetch_all_deals()

            if not all_deals:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"Bulk Deals: No deals found for any stocks")
                return 0

            # Filter by user's monitored stocks
            filtered_deals = monitor.filter_deals_by_monitored_stocks(all_deals, monitored_scrips)

            if not filtered_deals:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"Bulk Deals: No deals found for user's monitored stocks")
                return 0

            # Check for new deals (not seen before)
            new_deals = []
            for deal in filtered_deals:
                deal_id = monitor.create_deal_id(deal)

                # Check if deal already seen
                if not db_seen_deal_exists(user_client, user_id, deal_id):
                    new_deals.append(deal)
                    # Mark as seen
                    db_save_seen_deal(user_client, user_id, deal_id, deal)

            if not new_deals:
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"Bulk Deals: No new deals (all already processed)")
                return 0

            # Format message
            message_text = monitor.format_deals_for_telegram(new_deals)
        
        if not message_text:
            return 0
        
        # Send to all recipients
        for recipient in telegram_recipients:
            try:
                chat_id = recipient['chat_id']
                
                # Use existing Telegram API setup
                TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
                TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
                
                response = requests.post(
                    f"{TELEGRAM_API_URL}/sendMessage",
                    json={
                        'chat_id': chat_id,
                        'text': message_text,
                        'parse_mode': 'HTML'
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get('ok'):
                        messages_sent += 1
                        if os.environ.get('BSE_VERBOSE', '0') == '1':
                            print(f"✅ Bulk deals alert sent to {chat_id}")
                    else:
                        print(f"❌ Telegram API error: {result.get('description', 'Unknown')}")
                else:
                    print(f"❌ HTTP error {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Error sending to {recipient.get('chat_id', 'unknown')}: {e}")
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"Bulk Deals: Sent {messages_sent} alerts for {len(new_deals)} new deals")
        
        return messages_sent
        
    except Exception as e:
        print(f"❌ Bulk deals monitoring error: {e}")
        return 0

def db_seen_deal_exists(user_client, user_id: str, deal_id: str) -> bool:
    """Check if deal already processed for this user"""
    try:
        resp = (
            user_client
            .table('seen_deals')
            .select('id')
            .eq('user_id', user_id)
            .eq('deal_id', deal_id)
            .limit(1)
            .execute()
        )
        return len(resp.data) > 0
    except Exception:
        # If table doesn't exist or other error, assume not seen
        return False

def db_save_seen_deal(user_client, user_id: str, deal_id: str, deal: Dict):
    """Save seen deal to prevent duplicate notifications"""
    try:
        user_client.table('seen_deals').insert({
            'user_id': user_id,
            'deal_id': deal_id,
            'source': deal.get('source', ''),
            'deal_type': deal.get('deal_type', ''),
            'security_name': deal.get('security_name', ''),
            'script_code': deal.get('script_code', ''),
            'client_name': deal.get('client_name', ''),
            'buy_sell': deal.get('buy_sell', ''),
            'quantity': float(deal.get('quantity', 0)),
            'price': float(deal.get('price', 0)),
            'deal_value': float(deal.get('deal_value', 0)),
            'deal_date': deal.get('deal_date', ''),
            'processed_at': datetime.now().isoformat()
        }).execute()
    except Exception as e:
        # Log error but don't fail the whole process
        print(f"Warning: Could not save seen deal: {e}")

# SQL schema for seen_deals table (add to database setup)
SEEN_DEALS_SQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_deals (
    id SERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    deal_id VARCHAR(255) NOT NULL,
    source VARCHAR(10) NOT NULL,
    deal_type VARCHAR(10) NOT NULL,
    security_name VARCHAR(255),
    script_code VARCHAR(20),
    client_name TEXT,
    buy_sell VARCHAR(10),
    quantity NUMERIC(15,2),
    price NUMERIC(15,2),
    deal_value NUMERIC(20,2),
    deal_date VARCHAR(20),
    processed_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, deal_id)
);

CREATE INDEX IF NOT EXISTS idx_seen_deals_user_deal ON seen_deals(user_id, deal_id);
CREATE INDEX IF NOT EXISTS idx_seen_deals_date ON seen_deals(processed_at);
"""