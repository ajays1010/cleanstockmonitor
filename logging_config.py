import logging
import logging.handlers
import os
import sys
from datetime import datetime
import traceback

class GitHubLogger:
    """Logger that can optionally push logs to GitHub for persistence"""
    
    def __init__(self):
        self.setup_logging()
    
    def setup_logging(self):
        """Configure logging with multiple handlers"""
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Configure root logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                # Console handler
                logging.StreamHandler(sys.stdout),
                # File handler (rotates daily)
                logging.handlers.TimedRotatingFileHandler(
                    'logs/app.log',
                    when='midnight',
                    interval=1,
                    backupCount=7,
                    encoding='utf-8'
                ),
                # Error file handler
                logging.FileHandler('logs/errors.log', encoding='utf-8')
            ]
        )
        
        # Set levels for different loggers
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    def log_app_start(self):
        """Log application startup with system info"""
        import platform
        logging.info("="*50)
        logging.info("BSE Monitor Application Starting")
        logging.info(f"Python: {platform.python_version()}")
        logging.info(f"Platform: {platform.platform()}")
        logging.info(f"Memory: {self.get_memory_usage()} MB")
        logging.info("="*50)
    
    def log_error(self, error, context=""):
        """Log detailed error information"""
        error_msg = f"ERROR {context}: {str(error)}"
        logging.error(error_msg)
        logging.error(f"Traceback: {traceback.format_exc()}")
        
        # Write critical errors to separate file
        try:
            with open('logs/critical.log', 'a', encoding='utf-8') as f:
                f.write(f"{datetime.utcnow().isoformat()} - {error_msg}\n")
                f.write(f"Traceback: {traceback.format_exc()}\n")
                f.write("-" * 80 + "\n")
        except Exception:
            pass
    
    def log_memory_usage(self):
        """Log current memory usage"""
        memory = self.get_memory_usage()
        logging.info(f"Memory usage: {memory} MB")
        return memory
    
    def get_memory_usage(self):
        """Get current memory usage in MB"""
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            return round(process.memory_info().rss / 1024 / 1024, 2)
        except Exception:
            return 'unknown'
    
    def log_cron_execution(self, endpoint, user_count, success_count, error_count):
        """Log cron job execution details"""
        logging.info(f"CRON {endpoint}: users={user_count}, success={success_count}, errors={error_count}")
    
    def push_logs_to_github(self):
        """Optional: Push logs to GitHub repository (requires GitHub token and repo setup)"""
        github_token = os.environ.get('GITHUB_LOG_TOKEN')
        github_repo = os.environ.get('GITHUB_LOG_REPO')  # format: username/repo
        
        if not github_token or not github_repo:
            return False
        
        try:
            import requests
            import base64
            from datetime import datetime
            
            # Read critical log file
            if not os.path.exists('logs/critical.log'):
                return False
            
            with open('logs/critical.log', 'r', encoding='utf-8') as f:
                content = f.read()
            
            if not content.strip():
                return False
            
            # Prepare for GitHub API
            encoded_content = base64.b64encode(content.encode()).decode()
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f'logs/critical_{timestamp}.log'
            
            url = f"https://api.github.com/repos/{github_repo}/contents/{filename}"
            headers = {
                'Authorization': f'token {github_token}',
                'Content-Type': 'application/json'
            }
            
            data = {
                'message': f'Critical logs from BSE Monitor - {timestamp}',
                'content': encoded_content
            }
            
            response = requests.put(url, json=data, headers=headers, timeout=30)
            
            if response.status_code in [200, 201]:
                # Clear the critical log after successful upload
                with open('logs/critical.log', 'w') as f:
                    f.write('')
                logging.info(f"Logs pushed to GitHub: {filename}")
                return True
            else:
                logging.error(f"Failed to push logs to GitHub: {response.status_code}")
                return False
                
        except Exception as e:
            logging.error(f"Error pushing logs to GitHub: {e}")
            return False

# Global logger instance
github_logger = GitHubLogger()