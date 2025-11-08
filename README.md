# BSE Stock Monitor - Production Release

A comprehensive stock market monitoring and notification system for BSE (Bombay Stock Exchange) with multi-user Telegram support.

## Features

✅ **Multi-User Telegram Notifications** - Multiple users can share chat IDs with personalized names  
✅ **Real-time Stock Monitoring** - Live price alerts and BSE announcements  
✅ **AI-Powered Analysis** - Intelligent PDF processing and sentiment analysis  
✅ **Automated Cron Jobs** - Scheduled monitoring during market hours  
✅ **Admin Dashboard** - Complete user and system management  
✅ **Firebase Authentication** - Secure user login with Google/Phone  
✅ **Supabase Database** - Reliable cloud database backend  

## Quick Setup

### 1. Environment Configuration
```bash
cp .env.example .env
# Edit .env with your actual API keys and configuration
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run Application
```bash
python app.py
```

### 4. Deploy to Production
The application is configured for deployment on Render.com using the included `render.yaml` configuration.

## Essential Environment Variables

```env
FIREBASE_SERVICE_ACCOUNT_JSON={"type": "service_account", ...}
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=your-supabase-service-role-key
TELEGRAM_BOT_TOKEN=123456789:your-telegram-bot-token
CRON_SECRET_KEY=your-secret-cron-key
```

## Key Endpoints

- `/` - Main user dashboard
- `/admin/` - Admin panel
- `/cron/master` - Unified cron controller
- `/health` - Health check endpoint

## Multi-User Telegram Enhancement

This system now supports multiple Telegram users with the following capabilities:

- **Shared Chat IDs**: Multiple users can use the same Telegram chat with different names
- **Personalized Messages**: Each notification includes the user's specified name
- **Enhanced UI**: User-friendly forms for adding recipients with names

### Example Usage:
- **John Smith** → Chat ID: `123456789`  
- **Trading Team** → Chat ID: `123456789` (same chat, different name)  
- **Family Group** → Chat ID: `123456789` (same chat, different name)  

### Message Format:
```
👤 John Smith
────────────────────
📊 Stock Alert: RELIANCE crossed ₹2,500!
```

## Database Schema

The system uses PostgreSQL via Supabase with the following key tables:
- `profiles` - User accounts and authentication
- `monitored_scrips` - User's tracked stocks
- `telegram_recipients` - Multi-user telegram recipients with names
- `bse_announcements` - BSE announcement data
- `cron_run_logs` - System monitoring logs

## Production Deployment

This folder contains only production-ready files:
- No test scripts or development artifacts
- Clean environment configuration
- Optimized for cloud deployment
- Ready for GitHub → Render auto-deployment

## Security Features

- Firebase Admin SDK for secure authentication
- Cron endpoint protection with secret keys
- Environment variable configuration
- Service role separation for admin functions

## Support

For technical issues or feature requests, refer to the main project documentation or contact the development team.

---

**Version**: Production Release  
**Last Updated**: Multi-user Telegram Enhancement Complete  
**Deployment**: Render.com via GitHub Auto-Deploy