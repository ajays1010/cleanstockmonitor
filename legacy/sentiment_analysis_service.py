#!/usr/bin/env python3
"""
On-Demand Sentiment Analysis Service
Performs comprehensive sentiment analysis when user requests it

Features:
1. Fetches real-time news from NewsData.io API
2. Retrieves stored news from database
3. Combines both sources for comprehensive analysis
4. Performs detailed sentiment analysis with confidence scoring
5. Returns rich data for detailed analysis page
"""

import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from textblob import TextBlob
import requests
import time

# Import RSS news fetcher with retry logic
RSS_AVAILABLE = False
RSSNewsFetcher = None

try:
    from rss_news_fetcher import RSSNewsFetcher
    RSS_AVAILABLE = True
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print("✅ RSS news fetcher successfully imported and available")
except ImportError as e:
    RSS_AVAILABLE = False
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"❌ RSS news fetcher import failed: {e}")
except Exception as e:
    RSS_AVAILABLE = False
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"❌ RSS news fetcher error: {e}")

# Import AI news deduplicator
try:
    from ai_news_deduplicator import ai_deduplicate_news_articles
    AI_DEDUPLICATION_AVAILABLE = True
except ImportError:
    AI_DEDUPLICATION_AVAILABLE = False
    print("AI news deduplicator not available")

class ComprehensiveSentimentAnalyzer:
    """Enhanced sentiment analyzer for financial news"""
    
    def __init__(self):
        # Financial sentiment keywords
        self.positive_keywords = {
            'surge', 'rally', 'gain', 'profit', 'growth', 'bullish', 'upbeat', 'optimistic',
            'expansion', 'strong', 'robust', 'excellent', 'outstanding', 'record', 'milestone',
            'breakthrough', 'success', 'achievement', 'boost', 'rise', 'increase', 'jump',
            'soar', 'climb', 'advance', 'improvement', 'upgrade', 'buy', 'recommend', 'outperform',
            'beat', 'exceed', 'outshine', 'positive', 'confidence', 'momentum', 'winners'
        }
        
        self.negative_keywords = {
            'fall', 'drop', 'decline', 'loss', 'bearish', 'pessimistic', 'concern', 'worry',
            'downgrade', 'sell', 'avoid', 'risk', 'crash', 'plunge', 'tumble', 'slide',
            'weak', 'poor', 'disappointing', 'trouble', 'crisis', 'problem', 'issue',
            'challenge', 'threat', 'warning', 'alert', 'caution', 'volatile', 'uncertainty',
            'underperform', 'negative', 'losses', 'miss', 'below', 'concerns', 'pressure'
        }
        
        # Weight multipliers for different text components
        self.title_weight = 2.5
        self.description_weight = 1.5
        self.keyword_weight = 1.2
    
    def analyze_article_sentiment(self, article: Dict) -> Dict:
        """
        Comprehensive sentiment analysis for a single article
        Returns detailed sentiment breakdown with confidence scoring
        """
        title = article.get('title', '') or ''
        description = article.get('description', '') or ''
        content = article.get('content', '') or ''
        keywords = article.get('keywords', []) or []
        
        # Safety check for empty content
        if not title and not description and not content:
            return {
                'article_id': article.get('article_id', ''),
                'title': 'No title available',
                'sentiment_score': 0.0,
                'sentiment_label': 'NEUTRAL',
                'sentiment_emoji': '📊',
                'confidence': 0,
                'analysis_source': 'error',
                'error': 'No text content available'
            }
        
        try:
            # Combine text for analysis (prioritize title and description)
            main_text = f"{title} {description}".strip()
            full_text = f"{main_text} {content}".strip() if content else main_text
            keywords_text = ' '.join(keywords) if keywords else ''
            
            # TextBlob sentiment analysis on different components
            title_blob = TextBlob(title) if title else None
            desc_blob = TextBlob(description) if description else None
            full_blob = TextBlob(full_text) if full_text else TextBlob("neutral")
            
            # Calculate weighted sentiment scores
            title_sentiment = title_blob.sentiment.polarity * self.title_weight if title_blob else 0
            desc_sentiment = desc_blob.sentiment.polarity * self.description_weight if description else 0
            
            # Keyword-based sentiment enhancement
            keyword_sentiment = self._analyze_financial_keywords(full_text)
            
            # Combined weighted score
            active_weights = 0
            if title: active_weights += self.title_weight
            if description: active_weights += self.description_weight
            if keyword_sentiment != 0: active_weights += self.keyword_weight
            
            total_weight = max(active_weights, 1.0)
            weighted_sentiment = (
                title_sentiment + 
                desc_sentiment + 
                (keyword_sentiment * self.keyword_weight)
            ) / total_weight
            
            # Subjectivity and confidence calculation
            subjectivity = full_blob.sentiment.subjectivity
            confidence_base = abs(weighted_sentiment) * 100
            confidence_boost = (1 - subjectivity) * 20  # Lower subjectivity = higher confidence
            confidence = min(100, int(confidence_base + confidence_boost))
            
        except Exception as e:
            # Fallback for any TextBlob errors
            weighted_sentiment = 0.0
            confidence = 0
            subjectivity = 0.0
        
        # Classify sentiment with enhanced thresholds
        if weighted_sentiment > 0.15:
            sentiment_label = 'POSITIVE'
            emoji = '📈'
        elif weighted_sentiment < -0.15:
            sentiment_label = 'NEGATIVE'  
            emoji = '📉'
        else:
            sentiment_label = 'NEUTRAL'
            emoji = '📊'
        
        # Find matching keywords for transparency
        positive_keywords_found = self._find_keywords(full_text, self.positive_keywords)
        negative_keywords_found = self._find_keywords(full_text, self.negative_keywords)
        
        return {
            'article_id': article.get('article_id', article.get('id', '')),
            'title': title or 'No title',
            'description': description or 'No description available',
            'source': article.get('source_name', article.get('source', 'Unknown')),
            'pub_date': article.get('pubDate', article.get('published_at', '')),
            'url': article.get('link', article.get('url', '')),
            'sentiment_score': round(weighted_sentiment, 3),
            'sentiment_label': sentiment_label,
            'sentiment_emoji': emoji,
            'confidence': confidence,
            'subjectivity': round(subjectivity, 3),
            'positive_keywords_found': positive_keywords_found,
            'negative_keywords_found': negative_keywords_found,
            'analysis_timestamp': datetime.now().isoformat(),
            'analysis_source': 'comprehensive'
        }
    
    def _analyze_financial_keywords(self, text: str) -> float:
        """Enhanced financial keyword analysis"""
        if not text:
            return 0.0
            
        text_lower = text.lower()
        
        positive_count = sum(1 for word in self.positive_keywords if word in text_lower)
        negative_count = sum(1 for word in self.negative_keywords if word in text_lower)
        
        if positive_count == 0 and negative_count == 0:
            return 0.0
        
        total_sentiment_words = positive_count + negative_count
        sentiment_score = (positive_count - negative_count) / total_sentiment_words
        
        # Boost score based on keyword density
        keyword_density = total_sentiment_words / len(text_lower.split())
        density_boost = min(keyword_density * 2, 0.5)  # Max boost of 0.5
        
        return sentiment_score * (1 + density_boost)
    
    def _find_keywords(self, text: str, keyword_set: set) -> List[str]:
        """Find matching keywords in text"""
        if not text:
            return []
        
        text_lower = text.lower()
        found_keywords = [word for word in keyword_set if word in text_lower]
        return found_keywords[:10]  # Limit to top 10 for display

class NewsDataAPIClient:
    """API client for real-time news fetching"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsdata.io/api/1/news"
        self.headers = {'X-ACCESS-KEY': api_key}
    
    def fetch_stock_news(self, stock_query: str, size: int = 10) -> Dict:
        """Fetch real-time news for sentiment analysis"""
        params = {
            'q': stock_query,
            'language': 'en',
            'country': 'in',
            'category': 'business',
            'size': min(size, 10)
        }
        
        try:
            response = requests.get(
                self.base_url, 
                params=params,
                headers=self.headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'success': True,
                    'articles': data.get('results', []),
                    'total_results': data.get('totalResults', 0),
                    'source': 'newsdata_api'
                }
            else:
                return {
                    'success': False,
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'source': 'newsdata_api'
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'source': 'newsdata_api'
            }

def get_stored_news(user_client, stock_name: str, days_back: int = 7) -> List[Dict]:
    """
    Retrieve stored news articles from database for sentiment analysis
    Uses optimized search terms to find more articles
    """
    try:
        # Calculate date range
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # Get optimized search queries to try multiple variations
        search_queries = get_optimized_search_query(stock_name)
        
        all_articles = []
        
        # Try each search query
        for search_query in search_queries:
            result = user_client.table('processed_news_articles')\
                .select('*')\
                .eq('stock_query', search_query)\
                .gte('processed_at', cutoff_date.isoformat())\
                .order('processed_at', desc=True)\
                .limit(10)\
                .execute()
            
            # Convert database records to article format
            for record in result.data:
                all_articles.append({
                    'article_id': record.get('article_id', ''),
                    'title': record.get('title', ''),
                    'description': f"Stored news from {record.get('source_name', 'Unknown')}",
                    'link': record.get('url', ''),
                    'url': record.get('url', ''),
                    'source_name': record.get('source_name', 'Stored News'),
                    'source': record.get('source_name', 'Stored News'),
                    'pubDate': record.get('pub_date', ''),
                    'published_at': record.get('pub_date', ''),
                    'source_type': 'database',
                    'found_with_query': search_query
                })
        
        # Remove duplicates based on article_id
        unique_articles = []
        seen_ids = set()
        
        for article in all_articles:
            article_id = article.get('article_id', '')
            if article_id and article_id not in seen_ids:
                seen_ids.add(article_id)
                unique_articles.append(article)
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"Database search found {len(unique_articles)} unique articles for {stock_name}")
        
        return unique_articles
        
    except Exception as e:
        print(f"Error retrieving stored news: {e}")
        return []

def get_optimized_search_query(company_name: str) -> List[str]:
    """
    Generate optimized search queries for better news discovery
    Returns multiple search terms to try in order of priority
    """
    search_queries = []
    
    # Clean company name (remove common suffixes)
    clean_name = company_name.replace(' Ltd', '').replace(' Limited', '').replace(' Pvt', '').replace(' Private', '')
    clean_name = clean_name.replace(' Corporation', '').replace(' Corp', '').replace(' Inc', '')
    
    # Special cases for common companies (high priority)
    name_lower = company_name.lower()
    if 'ola electric' in name_lower:
        search_queries = ['OLA Electric', 'Ola Electric', 'OLA EV', 'Ola Electric Mobility']
    elif 'reliance' in name_lower and 'industries' in name_lower:
        search_queries = ['Reliance Industries', 'RIL', 'Reliance']
    elif 'tcs' in name_lower or 'tata consultancy' in name_lower:
        search_queries = ['TCS', 'Tata Consultancy Services', 'Tata Consultancy']
    elif 'infosys' in name_lower:
        search_queries = ['Infosys', 'INFY']
    elif 'hdfc' in name_lower and 'bank' in name_lower:
        search_queries = ['HDFC Bank', 'HDFC']
    elif 'icici' in name_lower and 'bank' in name_lower:
        search_queries = ['ICICI Bank', 'ICICI']
    elif 'sbi' in name_lower or 'state bank' in name_lower:
        search_queries = ['SBI', 'State Bank of India', 'State Bank']
    elif 'bajaj' in name_lower and 'auto' in name_lower:
        search_queries = ['Bajaj Auto', 'Bajaj']
    elif 'maruti' in name_lower:
        search_queries = ['Maruti Suzuki', 'Maruti', 'MSIL']
    elif 'adani' in name_lower:
        # Extract specific Adani company
        if 'enterprises' in name_lower:
            search_queries = ['Adani Enterprises', 'Adani']
        elif 'ports' in name_lower:
            search_queries = ['Adani Ports', 'Adani']
        elif 'power' in name_lower:
            search_queries = ['Adani Power', 'Adani']
        else:
            search_queries = ['Adani', clean_name]
    else:
        # Generic approach for other companies
        # Add the clean name first
        search_queries.append(clean_name)
        
        # If clean name is different from original, add original too
        if clean_name != company_name:
            search_queries.append(company_name)
        
        # Try to extract brand name (first word if multiple words)
        words = clean_name.split()
        if len(words) > 1:
            brand_name = words[0]
            if len(brand_name) > 3:  # Avoid short words like "The", "Ltd"
                search_queries.append(brand_name)
    
    # Ensure we always have at least the original name
    if company_name not in search_queries:
        search_queries.append(company_name)
    
    # Remove duplicates while preserving order
    unique_queries = []
    for query in search_queries:
        if query and query not in unique_queries:
            unique_queries.append(query)
    
    return unique_queries[:4]  # Limit to top 4 search terms to avoid too many API calls

def check_rss_availability() -> bool:
    """
    Runtime check for RSS availability
    Re-attempts import if initially failed
    """
    global RSS_AVAILABLE, RSSNewsFetcher
    
    if RSS_AVAILABLE and RSSNewsFetcher:
        return True
    
    # Re-attempt import
    try:
        from rss_news_fetcher import RSSNewsFetcher as RSSClass
        RSSNewsFetcher = RSSClass
        RSS_AVAILABLE = True
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print("✅ RSS news fetcher re-imported successfully")
        return True
    except Exception as e:
        RSS_AVAILABLE = False
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"❌ RSS news fetcher re-import failed: {e}")
        return False

def perform_comprehensive_sentiment_analysis(user_client, stock_symbol: str, company_name: str) -> Dict:
    """
    Main function for comprehensive sentiment analysis
    Prioritizes RSS feeds (real-time) over API (12+ hours old)
    
    Returns:
        Complete sentiment analysis report with detailed breakdown
    """
    
    # Initialize analyzer
    analyzer = ComprehensiveSentimentAnalyzer()
    
    # Get NewsData.io API key
    api_key = os.environ.get('NEWSDATA_API_KEY')
    
    all_articles = []
    data_sources = []
    debug_info = []
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"Starting comprehensive analysis for {company_name} ({stock_symbol})")
        print("🎯 Priority: RSS (real-time) → API (12h old) → Database")
    
    # 1. PRIORITY: Fetch real-time news from RSS feeds (most important for fresh news)
    rss_available = check_rss_availability()
    
    if rss_available:
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"🔄 Fetching RSS news for {company_name}...")
        
        try:
            # Create RSS fetcher instance
            rss_fetcher = RSSNewsFetcher()
            
            # Fetch comprehensive RSS news
            rss_result = rss_fetcher.fetch_comprehensive_rss_news(company_name)
            
            if rss_result.get('success'):
                rss_articles = rss_result.get('articles', [])
                if rss_articles:
                    # Mark articles as from RSS
                    for article in rss_articles:
                        article['source_type'] = 'rss'
                    all_articles.extend(rss_articles)
                    data_sources.extend(rss_result.get('data_sources', []))
                    debug_info.append(f"RSS: Found {len(rss_articles)} real-time articles from {len(rss_result.get('data_sources', []))} sources")
                    
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"✅ RSS: Found {len(rss_articles)} real-time articles")
                else:
                    debug_info.append("RSS: No articles found")
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"⚠️ RSS: No articles found")
            else:
                debug_info.append(f"RSS: Failed - {rss_result.get('error', 'unknown')[:100]}")
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"❌ RSS: Failed - {rss_result.get('error', 'unknown')[:100]}")
        except Exception as e:
            debug_info.append(f"RSS: Exception - {str(e)[:100]}")
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"❌ RSS: Exception - {str(e)[:100]}")
    else:
        debug_info.append("RSS: Not available (import failed)")
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print("❌ RSS: Not available (import failed)")
    
    # 2. SECONDARY: Fetch news from NewsData.io API (12+ hours old, but reliable backup)
    if api_key:
        api_client = NewsDataAPIClient(api_key)
        
        # Get optimized search queries
        search_queries = get_optimized_search_query(company_name)
        debug_info.append(f"Generated search queries: {search_queries}")
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"📡 Fetching API news (backup, 12h+ old) - Search queries: {search_queries}")
        
        api_success = False
        for i, search_query in enumerate(search_queries):
            if os.environ.get('BSE_VERBOSE', '0') == '1':
                print(f"Trying API search query {i+1}/{len(search_queries)}: '{search_query}'")
                
            api_result = api_client.fetch_stock_news(search_query, size=10)
            debug_info.append(f"API query '{search_query}': {api_result.get('success')} - {len(api_result.get('articles', []))} articles")
            
            if api_result.get('success'):
                api_articles = api_result.get('articles', [])
                if api_articles:  # Found articles with this search term
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"✅ Found {len(api_articles)} API articles with '{search_query}' (12h+ old)")
                    
                    # Mark articles as from API
                    for article in api_articles:
                        article['source_type'] = 'api'
                        article['search_query_used'] = search_query
                    all_articles.extend(api_articles)
                    data_sources.append(f"NewsData.io API ({len(api_articles)} articles, 12h+ old, via '{search_query}')")
                    api_success = True
                    break  # Stop searching once we find articles
                else:
                    if os.environ.get('BSE_VERBOSE', '0') == '1':
                        print(f"⚠️ No API articles found with '{search_query}'")
            else:
                error_msg = api_result.get('error', 'Unknown error')
                if os.environ.get('BSE_VERBOSE', '0') == '1':
                    print(f"❌ API search failed for '{search_query}': {error_msg}")
                debug_info.append(f"API error for '{search_query}': {error_msg}")
        
        # If no articles found with any search term
        if not api_success:
            data_sources.append(f"NewsData.io API (no articles found for any search term: {', '.join(search_queries)})")
    else:
        debug_info.append("No NewsData.io API key configured")
        data_sources.append("NewsData.io API (not configured)")
    
    # 3. TERTIARY: Get stored news from database (historical backup)
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"💾 Fetching stored news from database (historical backup)...")
    
    stored_articles = get_stored_news(user_client, company_name, days_back=7)
    all_articles.extend(stored_articles)
    data_sources.append(f"Database ({len(stored_articles)} historical articles)")
    debug_info.append(f"Database search found {len(stored_articles)} historical articles")
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"Total articles before deduplication: {len(all_articles)}")
    
    # AI-powered intelligent deduplication
    if AI_DEDUPLICATION_AVAILABLE and len(all_articles) >= 3:
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"Using AI deduplication for {len(all_articles)} articles...")
        
        dedup_result = ai_deduplicate_news_articles(all_articles)
        unique_articles = dedup_result.get('deduplicated_articles', [])
        dedup_stats = dedup_result.get('stats', {})
        duplicate_clusters = dedup_result.get('duplicate_clusters', [])
        
        debug_info.append(f"AI Deduplication: {dedup_stats.get('original_count', 0)} → {dedup_stats.get('deduplicated_count', 0)} articles")
        debug_info.append(f"Deduplication method: {dedup_stats.get('method', 'unknown')}")
        debug_info.append(f"Duplicates removed: {dedup_stats.get('duplicates_removed', 0)}")
        
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"✅ AI Deduplication: {dedup_stats.get('original_count', 0)} → {dedup_stats.get('deduplicated_count', 0)} articles")
            if duplicate_clusters:
                print(f"📊 Found {len(duplicate_clusters)} duplicate clusters")
                for i, cluster in enumerate(duplicate_clusters, 1):
                    print(f"   {i}. {cluster.get('reason', 'Unknown')} (confidence: {cluster.get('confidence', 0)})")
    else:
        # Simple title-based deduplication fallback
        if os.environ.get('BSE_VERBOSE', '0') == '1':
            print(f"Using simple deduplication for {len(all_articles)} articles...")
        
        unique_articles = []
        seen_titles = set()
        
        for article in all_articles:
            title = article.get('title', '').lower().strip()
            title_key = title[:50] if title else str(len(unique_articles))  # Use first 50 chars as key
            
            if title_key not in seen_titles and title:
                seen_titles.add(title_key)
                article['is_clustered'] = False
                article['duplicate_count'] = 0
                unique_articles.append(article)
        
        dedup_stats = {
            'original_count': len(all_articles),
            'deduplicated_count': len(unique_articles),
            'duplicates_removed': len(all_articles) - len(unique_articles),
            'method': 'simple_title_matching'
        }
        duplicate_clusters = []
        
        debug_info.append(f"Simple Deduplication: {dedup_stats['original_count']} → {dedup_stats['deduplicated_count']} articles")
    
    if os.environ.get('BSE_VERBOSE', '0') == '1':
        print(f"Total unique articles: {len(unique_articles)}")
    
    if not unique_articles:
        return {
            'success': False,
            'stock_name': company_name,
            'stock_symbol': stock_symbol,
            'error': 'No news articles found for analysis',
            'data_sources': data_sources,
            'debug_info': debug_info,
            'search_queries_tried': get_optimized_search_query(company_name),
            'rss_available': RSS_AVAILABLE,
            'ai_deduplication_available': AI_DEDUPLICATION_AVAILABLE,
            'deduplication_stats': dedup_stats if 'dedup_stats' in locals() else {},
            'timestamp': datetime.now().isoformat()
        }
    
    # 4. Perform sentiment analysis on all articles
    analyzed_articles = []
    sentiment_scores = []
    
    for article in unique_articles:
        analysis = analyzer.analyze_article_sentiment(article)
        analyzed_articles.append(analysis)
        if analysis.get('sentiment_score') is not None:
            sentiment_scores.append(analysis['sentiment_score'])
    
    if not sentiment_scores:
        return {
            'success': False,
            'error': 'Could not analyze sentiment for any articles',
            'articles_found': len(unique_articles),
            'debug_info': debug_info
        }
    
    # 5. Calculate aggregate sentiment metrics
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
    
    # Count sentiment distribution
    sentiment_counts = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0}
    confidence_scores = []
    
    for analysis in analyzed_articles:
        label = analysis.get('sentiment_label', 'NEUTRAL')
        sentiment_counts[label] = sentiment_counts.get(label, 0) + 1
        
        conf = analysis.get('confidence', 0)
        if conf > 0:
            confidence_scores.append(conf)
    
    # Overall sentiment determination
    if avg_sentiment > 0.15:
        overall_sentiment = 'POSITIVE'
        overall_emoji = '📈'
    elif avg_sentiment < -0.15:
        overall_sentiment = 'NEGATIVE'
        overall_emoji = '📉'
    else:
        overall_sentiment = 'NEUTRAL'
        overall_emoji = '📊'
    
    # Calculate overall confidence
    avg_confidence = int(sum(confidence_scores) / len(confidence_scores)) if confidence_scores else 0
    
    # Sort articles by sentiment score magnitude for display
    analyzed_articles.sort(key=lambda x: abs(x.get('sentiment_score', 0)), reverse=True)
    
    # Count articles by source type
    source_counts = {
        'rss': len([a for a in unique_articles if a.get('source_type') == 'rss']),
        'api': len([a for a in unique_articles if a.get('source_type') == 'api']),
        'database': len([a for a in unique_articles if a.get('source_type') == 'database'])
    }
    
    return {
        'success': True,
        'stock_name': company_name,
        'stock_symbol': stock_symbol,
        'analysis_timestamp': datetime.now().isoformat(),
        
        # Overall sentiment
        'overall_sentiment': overall_sentiment,
        'overall_emoji': overall_emoji,
        'sentiment_score': round(avg_sentiment, 3),
        'confidence': avg_confidence,
        
        # Article statistics
        'total_articles': len(analyzed_articles),
        'positive_articles': sentiment_counts['POSITIVE'],
        'negative_articles': sentiment_counts['NEGATIVE'],
        'neutral_articles': sentiment_counts['NEUTRAL'],
        
        # Detailed data
        'articles': analyzed_articles,
        'data_sources': data_sources,
        
        # Deduplication information
        'deduplication_stats': dedup_stats if 'dedup_stats' in locals() else {},
        'duplicate_clusters': duplicate_clusters if 'duplicate_clusters' in locals() else [],
        'ai_deduplication_used': AI_DEDUPLICATION_AVAILABLE and len(all_articles) >= 3,
        
        # Analysis metadata
        'rss_articles_count': source_counts['rss'],
        'api_articles_count': source_counts['api'],
        'database_articles_count': source_counts['database'],
        'analysis_method': 'comprehensive_with_ai_dedup' if AI_DEDUPLICATION_AVAILABLE else 'comprehensive_with_rss',
        'debug_info': debug_info,
        'search_queries_tried': get_optimized_search_query(company_name),
        'rss_available': RSS_AVAILABLE,
        'ai_deduplication_available': AI_DEDUPLICATION_AVAILABLE
    }