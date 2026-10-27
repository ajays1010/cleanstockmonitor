# Configuration file for API keys and settings
import os

# Twitter API Configuration
TWITTER_BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN", "")

# News API Configuration
NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "")

# Supabase Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# Indian News Sources Configuration
INDIAN_NEWS_SOURCES = {
    'mint': {
        'name': 'Mint',
        'base_url': 'https://www.livemint.com',
        'search_url': 'https://www.livemint.com/search',
        'selectors': {
            'articles': 'div.listingPage',
            'title': 'h2.headline',
            'description': 'p.summary',
            'link': 'a[href*="/news/"]',
            'date': 'span.date'
        }
    },
    'moneycontrol': {
        'name': 'Moneycontrol',
        'base_url': 'https://www.moneycontrol.com',
        'search_url': 'https://www.moneycontrol.com/news/search',
        'selectors': {
            'articles': 'div.news_list',
            'title': 'h2.news_list_title',
            'description': 'p.news_list_summary',
            'link': 'a.news_list_title',
            'date': 'span.article_date'
        }
    }
}

# Sentiment Analysis Configuration
SENTIMENT_CONFIG = {
    'max_tweets': 100,
    'max_news_articles': 50,
    'time_window_hours': 24,
    'sentiment_thresholds': {
        'positive': 0.1,
        'negative': -0.1
    }
}
