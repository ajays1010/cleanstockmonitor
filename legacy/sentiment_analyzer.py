import os
# tweepy is lazily imported in setup_clients
import requests
try:
    from newsapi import NewsApiClient
except Exception:
    NewsApiClient = None

# Optional Moneycontrol API integration
moneycontrol_api = None
for name in ("moneycontrol", "moneycontrol_api", "moneycontrolapi"):
    if moneycontrol_api is None:
        try:
            moneycontrol_api = __import__(name)
        except Exception:
            pass
from textblob import TextBlob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import json
from bs4 import BeautifulSoup
import time
from typing import List, Dict, Tuple, Optional
# Lazy import plotly inside visualization methods to avoid hard dependency during non-visual tests
from config import (
    TWITTER_BEARER_TOKEN, 
    NEWS_API_KEY, 
    INDIAN_NEWS_SOURCES, 
    SENTIMENT_CONFIG
)

class StockSentimentAnalyzer:
    """
    Comprehensive stock sentiment analysis system with Indian news sources
    """
    
    def __init__(self):
        self.twitter_client = None
        self.news_client = None
        self.setup_clients()
        
    def setup_clients(self):
        """Initialize Twitter and News API clients"""
        try:
            if TWITTER_BEARER_TOKEN:
                try:
                    import tweepy
                    self.twitter_client = tweepy.Client(bearer_token=TWITTER_BEARER_TOKEN)
                    print("✅ Twitter client initialized")
                except Exception as e:
                    print(f"⚠️ Tweepy not available or failed to init: {e}")
                    self.twitter_client = None
            else:
                print("⚠️ Twitter Bearer Token not found")
                
            if NEWS_API_KEY and NewsApiClient:
                self.news_client = NewsApiClient(api_key=NEWS_API_KEY)
                print("✅ News API client initialized")
            elif NEWS_API_KEY and not NewsApiClient:
                print("⚠️ newsapi library not available")
            else:
                print("⚠️ News API key not found")
                
        except Exception as e:
            print(f"❌ Error setting up clients: {e}")
    
    def gather_twitter_data(self, stock_symbol: str, company_name: str, hours_back: int = 24) -> List[Dict]:
        """
        Gather tweets related to a stock symbol
        """
        if not self.twitter_client:
            print("❌ Twitter client not available")
            return []
            
        tweets_data = []
        try:
            # Search for tweets containing stock symbol or company name
            query = f"({stock_symbol} OR {company_name}) -is:retweet lang:en"
            
            # Get tweets from the last N hours
            # Twitter API requires end_time to be at least 10 seconds in the past
            end_time = datetime.now(timezone.utc) - timedelta(seconds=10)
            start_time = end_time - timedelta(hours=hours_back)
            
            tweets = self.twitter_client.search_recent_tweets(
                query=query,
                max_results=SENTIMENT_CONFIG['max_tweets'],
                tweet_fields=['created_at', 'public_metrics', 'author_id'],
                start_time=start_time,
                end_time=end_time
            )
            
            if tweets.data:
                for tweet in tweets.data:
                    tweets_data.append({
                        'id': tweet.id,
                        'text': tweet.text,
                        'created_at': tweet.created_at,
                        'retweet_count': tweet.public_metrics['retweet_count'],
                        'like_count': tweet.public_metrics['like_count'],
                        'reply_count': tweet.public_metrics['reply_count'],
                        'source': 'twitter'
                    })
                    
            print(f"📱 Gathered {len(tweets_data)} tweets for {stock_symbol}")
            
        except Exception as e:
            print(f"❌ Error gathering Twitter data: {e}")
            
        return tweets_data
    
    def gather_news_data(self, stock_symbol: str, company_name: str, hours_back: int = 24) -> List[Dict]:
        """
        Gather news articles from multiple sources including Indian financial news
        """
        news_data = []
        
        # Gather from News API (international sources)
        if self.news_client:
            news_data.extend(self._gather_newsapi_data(stock_symbol, company_name, hours_back))
        
        # Gather from Moneycontrol API if available
        news_data.extend(self._gather_moneycontrol_api_data(stock_symbol, company_name, hours_back))

        # Gather from Indian financial news sources (HTML scraping fallback)
        news_data.extend(self._gather_indian_news_data(stock_symbol, company_name, hours_back))
        
        return news_data
    
    def _gather_newsapi_data(self, stock_symbol: str, company_name: str, hours_back: int) -> List[Dict]:
        """Gather news from News API"""
        news_data = []
        try:
            query = f"{stock_symbol} OR {company_name}"
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(hours=hours_back)
            
            articles = self.news_client.get_everything(
                q=query,
                from_param=start_date.strftime('%Y-%m-%d'),
                to=end_date.strftime('%Y-%m-%d'),
                language='en',
                sort_by='relevancy',
                page_size=SENTIMENT_CONFIG['max_news_articles']
            )
            
            if articles['articles']:
                for article in articles['articles']:
                    news_data.append({
                        'id': article.get('url', ''),
                        'title': article.get('title', ''),
                        'description': article.get('description', ''),
                        'content': article.get('content', ''),
                        'published_at': article.get('publishedAt', ''),
                        'source': article.get('source', {}).get('name', ''),
                        'url': article.get('url', ''),
                        'source_type': 'news_api'
                    })
                    
            print(f"📰 Gathered {len(news_data)} articles from News API for {stock_symbol}")
            
        except Exception as e:
            print(f"❌ Error gathering News API data: {e}")
            
        return news_data
    
    def _gather_moneycontrol_api_data(self, stock_symbol: str, company_name: str, hours_back: int) -> List[Dict]:
        """Gather news from Moneycontrol via the moneycontrol-api package if available."""
        news_data = []
        if not moneycontrol_api:
            return news_data
        try:
            # Attempt to call a plausible news endpoint from the installed library
            # Since different forks exist, we try a few common patterns.
            items = []
            for attr in ("news", "get_news", "get_company_news", "company_news"):
                fn = getattr(moneycontrol_api, attr, None)
                if callable(fn):
                    try:
                        # Try with company_name first, then stock_symbol
                        items = fn(company_name) or []
                        if not items:
                            items = fn(stock_symbol) or []
                        break
                    except TypeError:
                        # Try keyword variants
                        try:
                            items = fn(query=company_name) or []
                            if not items:
                                items = fn(query=stock_symbol) or []
                            break
                        except Exception:
                            continue
                # Some modules expose a Client class
                client_cls = getattr(moneycontrol_api, "Client", None)
                if client_cls:
                    try:
                        client = client_cls()
                        if hasattr(client, "company_news"):
                            items = client.company_news(company_name) or []
                        elif hasattr(client, "get_news"):
                            items = client.get_news(company_name) or []
                        elif hasattr(client, "news"):
                            items = client.news(company_name) or []
                        if items:
                            break
                    except Exception:
                        pass

            # Normalize and filter items
            now = datetime.now(timezone.utc)
            since = now - timedelta(hours=hours_back)
            for it in items[:100]:  # limit to 100
                title = it.get("title") or it.get("headline") or ""
                desc = it.get("description") or it.get("summary") or it.get("content") or ""
                url = it.get("url") or it.get("link") or ""
                pub = it.get("published_at") or it.get("pubDate") or it.get("date") or it.get("time") or ""
                ts = self._parse_timestamp(pub)
                if ts < since:
                    continue
                if not title and not desc:
                    continue
                if company_name.lower() not in (title + " " + desc).lower() and stock_symbol.lower() not in (title + " " + desc).lower():
                    # Keep only relevant items
                    continue
                news_data.append({
                    'id': url or title,
                    'title': title,
                    'description': desc,
                    'content': desc,
                    'published_at': ts.isoformat().replace('+00:00','Z'),
                    'source': 'Moneycontrol',
                    'url': url,
                    'source_type': 'moneycontrol_api'
                })
        except Exception as e:
            print(f"⚠️ Moneycontrol API fetch failed: {e}")
        return news_data

    def _gather_indian_news_data(self, stock_symbol: str, company_name: str, hours_back: int) -> List[Dict]:
        """Gather news from Indian financial news sources"""
        news_data = []
        
        for source_key, source_config in INDIAN_NEWS_SOURCES.items():
            try:
                source_news = self._scrape_indian_news_source(
                    source_config, stock_symbol, company_name, hours_back
                )
                news_data.extend(source_news)
                print(f"📰 Gathered {len(source_news)} articles from {source_config['name']} for {stock_symbol}")
            except Exception as e:
                print(f"❌ Error gathering data from {source_config['name']}: {e}")
        
        return news_data
    
    def _scrape_indian_news_source(self, source_config: Dict, stock_symbol: str, company_name: str, hours_back: int) -> List[Dict]:
        """Scrape news from a specific Indian news source"""
        news_data = []
        
        try:
            # Create search query for the source
            search_query = f"{stock_symbol} {company_name}"
            
            # For demonstration, we'll use a simple approach
            # In production, you'd want to implement proper search functionality
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Try to get recent news from the main page
            response = requests.get(source_config['base_url'], headers=headers, timeout=5)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find articles using the configured selectors
            articles = soup.select(source_config['selectors']['articles'])
            
            for article in articles[:10]:  # Limit to 10 articles per source
                try:
                    title_elem = article.select_one(source_config['selectors']['title'])
                    desc_elem = article.select_one(source_config['selectors']['description'])
                    link_elem = article.select_one(source_config['selectors']['link'])
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        description = desc_elem.get_text(strip=True) if desc_elem else ""
                        link = link_elem.get('href', '')
                        
                        # Make relative URLs absolute
                        if link.startswith('/'):
                            link = source_config['base_url'] + link
                        
                        # Check if the article is related to our search terms
                        if any(term.lower() in title.lower() or term.lower() in description.lower() 
                               for term in [stock_symbol, company_name]):
                            
                            news_data.append({
                                'id': link,
                                'title': title,
                                'description': description,
                                'content': description,
                                'published_at': datetime.now(timezone.utc).isoformat().replace('+00:00','Z'),  # Approximate in UTC
                                'source': source_config['name'],
                                'url': link,
                                'source_type': 'indian_news'
                            })
                            
                except Exception as e:
                    print(f"⚠️ Error parsing article from {source_config['name']}: {e}")
                    continue
            
        except Exception as e:
            print(f"❌ Error scraping {source_config['name']}: {e}")
        
        return news_data
    
    def analyze_sentiment(self, text: str) -> Tuple[float, str]:
        """
        Analyze sentiment of text using TextBlob
        Returns: (sentiment_score, sentiment_label)
        """
        try:
            # Clean and preprocess text
            cleaned_text = self.preprocess_text(text)
            
            # Analyze sentiment
            blob = TextBlob(cleaned_text)
            sentiment_score = blob.sentiment.polarity
            
            # Convert score to label
            if sentiment_score > 0.1:
                sentiment_label = "Positive"
            elif sentiment_score < -0.1:
                sentiment_label = "Negative"
            else:
                sentiment_label = "Neutral"
                
            return sentiment_score, sentiment_label
            
        except Exception as e:
            print(f"❌ Error analyzing sentiment: {e}")
            return 0.0, "Neutral"
    
    def preprocess_text(self, text: str) -> str:
        """
        Clean and preprocess text for sentiment analysis
        """
        if not text:
            return ""
            
        # Remove URLs, mentions, hashtags
        import re
        text = re.sub(r'http\S+|www\S+|@\w+|#\w+', '', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\.\,\!\?]', '', text)
        
        # Convert to lowercase
        text = text.lower().strip()
        
        return text
    
    def _parse_timestamp(self, ts) -> datetime:
        """Parse various timestamp formats into a timezone-aware UTC datetime"""
        now = datetime.now(timezone.utc)
        try:
            if isinstance(ts, datetime):
                return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            if not isinstance(ts, str):
                return now
            s = ts.strip()
            if not s:
                return now
            if s.endswith('Z'):
                s = s[:-1] + '+00:00'
            try:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                return now
        except Exception:
            return now

    def process_stock_sentiment(self, stock_symbol: str, company_name: str, hours_back: int = 24) -> Dict:
        """
        Main function to process sentiment analysis for a stock
        """
        print(f"🔍 Starting sentiment analysis for {stock_symbol} ({company_name})")
        
        # Gather data
        twitter_data = self.gather_twitter_data(stock_symbol, company_name, hours_back)
        news_data = self.gather_news_data(stock_symbol, company_name, hours_back)
        
        # Analyze sentiment for each data point
        all_sentiments = []
        
        # Process Twitter data
        for tweet in twitter_data:
            sentiment_score, sentiment_label = self.analyze_sentiment(tweet['text'])
            all_sentiments.append({
                'timestamp': self._parse_timestamp(tweet['created_at']).isoformat().replace('+00:00','Z'),
                'text': tweet['text'][:100] + "..." if len(tweet['text']) > 100 else tweet['text'],
                'sentiment_score': sentiment_score,
                'sentiment_label': sentiment_label,
                'source': 'twitter',
                'engagement': tweet['retweet_count'] + tweet['like_count'] + tweet['reply_count'],
                'raw_data': tweet
            })
        
        # Process news data
        for article in news_data:
            # Analyze both title and description
            title_sentiment, title_label = self.analyze_sentiment(article['title'])
            desc_sentiment, desc_label = self.analyze_sentiment(article['description'])
            
            # Use weighted average (title more important)
            combined_sentiment = (title_sentiment * 0.7) + (desc_sentiment * 0.3)
            combined_label = "Positive" if combined_sentiment > 0.1 else "Negative" if combined_sentiment < -0.1 else "Neutral"
            
            all_sentiments.append({
                'timestamp': self._parse_timestamp(article['published_at']),
                'text': article['title'],
                'sentiment_score': combined_sentiment,
                'sentiment_label': combined_label,
                'source': 'news',
                'engagement': 1,  # News articles get base engagement
                'raw_data': article
            })
        
        # Sort by timestamp
        all_sentiments.sort(key=lambda x: x['timestamp'])
        
        # Calculate aggregate metrics
        total_items = len(all_sentiments)
        if total_items > 0:
            avg_sentiment = sum(item['sentiment_score'] for item in all_sentiments) / total_items
            positive_count = sum(1 for item in all_sentiments if item['sentiment_label'] == 'Positive')
            negative_count = sum(1 for item in all_sentiments if item['sentiment_label'] == 'Negative')
            neutral_count = sum(1 for item in all_sentiments if item['sentiment_label'] == 'Neutral')
            
            sentiment_distribution = {
                'positive': positive_count / total_items * 100,
                'negative': negative_count / total_items * 100,
                'neutral': neutral_count / total_items * 100
            }
        else:
            avg_sentiment = 0.0
            sentiment_distribution = {'positive': 0, 'negative': 0, 'neutral': 0}
        
        result = {
            'stock_symbol': stock_symbol,
            'company_name': company_name,
            'analysis_timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00','Z'),
            'time_period_hours': hours_back,
            'total_data_points': total_items,
            'average_sentiment': avg_sentiment,
            'sentiment_distribution': sentiment_distribution,
            'sentiment_data': all_sentiments,
            'summary': {
                'overall_mood': "Bullish" if avg_sentiment > 0.1 else "Bearish" if avg_sentiment < -0.1 else "Neutral",
                'confidence': min(abs(avg_sentiment) * 100, 100),
                'data_quality': "High" if total_items > 20 else "Medium" if total_items > 10 else "Low"
            }
        }
        
        print(f"✅ Sentiment analysis completed for {stock_symbol}")
        print(f"   📊 Average Sentiment: {avg_sentiment:.3f}")
        print(f"   📈 Overall Mood: {result['summary']['overall_mood']}")
        print(f"   🎯 Confidence: {result['summary']['confidence']:.1f}%")
        
        return result
    
    def create_sentiment_heatmap(self, sentiment_data: List[Dict], stock_symbol: str):
        """
        Create a heatmap visualization of sentiment over time
        """
        try:
            import plotly.graph_objects as go
        except Exception:
            return {'error': 'plotly not installed', 'data_points': len(sentiment_data) if sentiment_data else 0}
        if not sentiment_data:
            return go.Figure()
        
        # Prepare data for heatmap
        df = pd.DataFrame(sentiment_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['day'] = df['timestamp'].dt.day_name()
        
        # Create pivot table for heatmap
        pivot_data = df.pivot_table(
            values='sentiment_score',
            index='day',
            columns='hour',
            aggfunc='mean',
            fill_value=0
        )
        
        # Reorder days
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        pivot_data = pivot_data.reindex(day_order)
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=pivot_data.values,
            x=pivot_data.columns,
            y=pivot_data.index,
            colorscale='RdYlGn',  # Red (negative) to Green (positive)
            zmid=0,  # Center at neutral sentiment
            colorbar=dict(
                title="Sentiment Score",
                tickmode='array',
                tickvals=[-1, -0.5, 0, 0.5, 1],
                ticktext=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
            )
        ))
        
        fig.update_layout(
            title=f"Sentiment Heatmap for {stock_symbol} (Last 24 Hours)",
            xaxis_title="Hour of Day",
            yaxis_title="Day of Week",
            height=500
        )
        
        return fig
    
    def create_sentiment_timeline(self, sentiment_data: List[Dict], stock_symbol: str):
        """
        Create a timeline visualization of sentiment over time
        """
        try:
            import plotly.graph_objects as go
        except Exception:
            return {'error': 'plotly not installed', 'data_points': len(sentiment_data) if sentiment_data else 0}
        if not sentiment_data:
            return go.Figure()
        
        df = pd.DataFrame(sentiment_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Create timeline plot
        fig = go.Figure()
        
        # Add sentiment line
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['sentiment_score'],
            mode='lines+markers',
            name='Sentiment Score',
            line=dict(color='blue', width=2),
            marker=dict(size=6)
        ))
        
        # Add zero line for reference
        fig.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Neutral")
        
        # Add sentiment zones
        fig.add_hrect(y0=0.1, y1=1, fillcolor="green", opacity=0.1, annotation_text="Positive Zone")
        fig.add_hrect(y0=-1, y1=-0.1, fillcolor="red", opacity=0.1, annotation_text="Negative Zone")
        
        fig.update_layout(
            title=f"Sentiment Timeline for {stock_symbol}",
            xaxis_title="Time",
            yaxis_title="Sentiment Score (-1 to +1)",
            height=400,
            showlegend=True
        )
        
        return fig

# --- Flask Integration Functions ---
def get_sentiment_analysis_for_stock(stock_symbol: str, company_name: str, hours_back: int = 24) -> Dict:
    """
    Flask-compatible function to get sentiment analysis
    """
    analyzer = StockSentimentAnalyzer()
    return analyzer.process_stock_sentiment(stock_symbol, company_name, hours_back)

def create_sentiment_visualizations(sentiment_data: Dict) -> Dict:
    """
    Create all visualizations for sentiment data
    """
    analyzer = StockSentimentAnalyzer()
    
    heatmap = analyzer.create_sentiment_heatmap(
        sentiment_data['sentiment_data'], 
        sentiment_data['stock_symbol']
    )
    
    timeline = analyzer.create_sentiment_timeline(
        sentiment_data['sentiment_data'], 
        sentiment_data['stock_symbol']
    )
    
    return {
        'heatmap': heatmap.to_json() if hasattr(heatmap, 'to_json') else json.dumps(heatmap),
        'timeline': timeline.to_json() if hasattr(timeline, 'to_json') else json.dumps(timeline),
        'summary': sentiment_data['summary']
    }
