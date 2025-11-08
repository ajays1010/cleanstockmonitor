"""
AI PDF Analysis Service for BSE Monitor
Provides AI-powered analysis of PDF documents using Google Gemini API
"""

import os
import base64
import logging
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    if os.path.exists('.env'):
        load_dotenv()
except ImportError:
    # Manual loading if dotenv not available
    env_file = Path('.env')
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    value = value.strip('"\'')
                    os.environ[key.strip()] = value

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import required libraries with graceful fallbacks
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logger.warning("Google GenerativeAI not available. Install with: pip install google-generativeai")

try:
    import PyPDF2
    PDF_READER_AVAILABLE = True
except ImportError:
    PDF_READER_AVAILABLE = False
    logger.warning("PyPDF2 not available. Install with: pip install PyPDF2")

def analyze_pdf_bytes_with_gemini(pdf_bytes: bytes, pdf_name: str, scrip_code: str = None) -> Optional[Dict[str, Any]]:
    """
    Analyze PDF bytes using Google Gemini API for financial document analysis.
    
    Args:
        pdf_bytes: The PDF file content as bytes
        pdf_name: Name of the PDF file
        scrip_code: BSE/NSE scrip code for the company
        
    Returns:
        Dictionary containing analysis results or None if analysis fails
    """
    
    # Check if Gemini API is available
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        logger.warning("GEMINI_API_KEY not set. Skipping AI analysis.")
        return None
        
    if not GEMINI_AVAILABLE:
        logger.warning("Google GenerativeAI library not available. Skipping AI analysis.")
        return None
    
    try:
        # Configure Gemini API
        genai.configure(api_key=api_key)
        
        # Initialize the model with proper configuration
        model_name = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

        # Try different model initialization approaches
        try:
            # Standard model initialization
            model = genai.GenerativeModel(model_name)
        except Exception as model_error:
            # If standard fails, try with generation config
            logger.warning(f"Standard model init failed: {model_error}, trying with config")
            try:
                generation_config = {
                    "temperature": 0.3,
                    "top_p": 0.8,
                    "top_k": 40,
                    "max_output_tokens": 8192,
                }
                model = genai.GenerativeModel(
                    model_name,
                    generation_config=generation_config
                )
            except Exception as config_error:
                logger.error(f"Model initialization failed: {config_error}")
                # Fallback to a basic model
                model = genai.GenerativeModel("gemini-pro")
        
        # Save PDF to temporary file for Gemini API
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_file:
            tmp_file.write(pdf_bytes)
            tmp_file_path = tmp_file.name
        
        try:
            # Extract PDF text first (more reliable approach for newer Gemini APIs)
            pdf_text = extract_text_from_pdf(pdf_bytes)

            # Create the enhanced prompt for financial analysis with support for all announcement types
            analysis_prompt = f"""
You are a financial analyst expert specializing in Indian stock market and BSE/NSE listed companies.
Analyze this PDF document (filename: {pdf_name}) and provide a comprehensive financial analysis.

Company Information:
- Scrip Code: {scrip_code if scrip_code else 'Not provided'}

🎯 DOCUMENT TYPE DETECTION:
• First determine the document type based on content and title
• Types: quarterly_results, annual_report, board_meeting, dividend_announcement, rating_change, rights_issue, agm_notice, investor_presentation, other

🎯 CRITICAL FOR QUARTERLY RESULTS:
• FIND the section with heading 'UNAUDITED CONSOLIDATED FINANCIAL RESULT' (any page)
• Look for financial table with columns for current quarter and previous quarter
• Extract EXACT numbers for 'Total Income', 'Total Expenses', and 'Profit Before Tax' rows
• If 'Profit Before Tax' not shown, calculate it as: Total Income - Total Expenses
• Numbers should be in Crores (convert from thousands/millions if needed)
• Common headings: 'Revenue from Operations', 'Total Income', 'Total Expenses', 'PBT', 'Profit/(Loss) before tax'
• Look for patterns like 'Q1 FY25', 'Q4 FY24', '3 months ended', etc.
• Calculate growth percentages: ((Current-Previous)/Previous)*100
• If document is NOT quarterly results, set quarterly_financials to null

🎯 FOR NON-QUARTERLY ANNOUNCEMENTS:
• Board Meeting: Extract decisions, resolutions, key agenda items
• Dividend: Extract dividend amount, record date, payment date, yield
• Rating: Extract new rating, previous rating, rationale, outlook
• Rights Issue: Extract issue price, ratio, record date, premium/discount
• AGM: Extract key resolutions, voting results, important decisions
• Other: Extract key business impact, financial implications

Please analyze the document and provide a JSON response with the following structure:
{{
    "company_name": "Company name from document",
    "scrip_code": "BSE/NSE code if found in document",
    "document_type": "quarterly_results/board_meeting/dividend_announcement/rating_change/rights_issue/agm_notice/annual_report/investor_presentation/other",
    "announcement_title": "Title of the announcement/document",
    "current_stock_price": "Current stock price if mentioned",
    "price_change": "Price change information",
    "quarterly_financials": {{
        "current_quarter": {{
            "period": "Q1/Q2/Q3/Q4 FY24 format or specific quarter name",
            "total_income": "Extract exact figure in Crores from financial table",
            "total_expenses": "Extract exact figure in Crores from financial table",
            "profit_before_tax": "Profit Before Tax in Crores (Total Income - Total Expenses)"
        }},
        "previous_quarter": {{
            "period": "Previous quarter period name",
            "total_income": "Previous quarter income in Crores",
            "total_expenses": "Previous quarter expenses in Crores",
            "profit_before_tax": "Previous quarter Profit Before Tax in Crores"
        }},
        "growth_analysis": {{
            "income_growth_percent": "Calculate: ((Current-Previous)/Previous)*100",
            "expenses_growth_percent": "Calculate: ((Current-Previous)/Previous)*100",
            "pbt_growth_percent": "Calculate: ((Current-Previous)/Previous)*100 for PBT",
            "income_growth_yoy_percent": "Year-over-year income growth if available",
            "expenses_growth_yoy_percent": "Year-over-year expenses growth if available",
            "pbt_growth_yoy_percent": "Year-over-year PBT growth if available"
        }}
    }},
    "financial_summary": "Brief summary of financial impact/key financial metrics",
    "business_impact": "How this announcement affects business operations",
    "market_implications": "Expected impact on stock price and market perception",
    "risk_assessment": "Key risks and opportunities from this announcement",
    "key_financials": {{
        "revenue": "Revenue figures",
        "profit": "Profit/loss information",
        "eps": "Earnings per share",
        "debt": "Debt information",
        "cash_flow": "Cash flow data"
    }},
    "investment_recommendation": "BUY/SELL/HOLD with reasoning",
    "price_target": "Target price if any",
    "sentiment_analysis": "POSITIVE/NEGATIVE/NEUTRAL",
    "public_perception": "Expected public/market reaction",
    "general_perception": "General market assessment",
    "catalyst_impact": "Impact on stock price movement",
    "risk_reward": "Risk-reward assessment",
    "web_insights": "Additional market insights",
    "price_momentum": "Expected price momentum",
    "motive_and_meaning": "Management intentions and document significance",
    "gist": "Key takeaway for investors (1-2 sentences)",
    "tldr": "Brief summary of key points"
}}

Focus on:
1. Financial performance metrics (if applicable)
2. Business developments and strategic initiatives
3. Market impact and investor perception
4. Risk factors and opportunities
5. Regulatory compliance and corporate governance
6. Specific analysis based on announcement type

For quarterly results: Focus on growth trends, margins, and financial health
For board meetings: Focus on strategic decisions and their business impact
For dividends: Focus on yield, payout ratio, and sustainability
For ratings: Focus on credit quality and business outlook
For rights issues: Focus on dilution impact and use of proceeds
For AGMs: Focus on governance and strategic direction

Provide actionable insights for retail and institutional investors.
"""

            if pdf_text:
                logger.info(f"Extracted {len(pdf_text)} characters from PDF")
                # Create enhanced prompt with PDF content
                enhanced_prompt = f"""{analysis_prompt}

PDF Content to Analyze:
{pdf_text[:12000]}...  # Limit to first 12k characters to avoid token limits

Please provide the analysis based on this PDF content."""

                logger.info("Using text-based analysis approach")
                response = model.generate_content(enhanced_prompt)
            else:
                logger.warning("Could not extract text from PDF, using prompt-only analysis")
                # Fallback to prompt-only analysis
                response = model.generate_content(analysis_prompt)
            
            # No file cleanup needed for text-based approach
            
            if response and response.text:
                # Try to parse JSON response
                import json
                try:
                    # Clean the response text (remove markdown formatting if present)
                    response_text = response.text.strip()
                    if response_text.startswith("```json"):
                        response_text = response_text[7:]
                    if response_text.endswith("```"):
                        response_text = response_text[:-3]
                    
                    analysis_result = json.loads(response_text)
                    
                    # Add metadata
                    analysis_result['analysis_timestamp'] = str(pd.Timestamp.now())
                    analysis_result['model_used'] = model_name
                    analysis_result['pdf_filename'] = pdf_name
                    
                    logger.info(f"Successfully analyzed PDF: {pdf_name}")
                    return analysis_result
                    
                except json.JSONDecodeError:
                    # If JSON parsing fails, return structured response
                    logger.warning("Failed to parse JSON response, returning text analysis")
                    return {
                        "analysis_text": response.text,
                        "company_name": "Analysis Available",
                        "pdf_filename": pdf_name,
                        "analysis_timestamp": str(pd.Timestamp.now()),
                        "status": "text_analysis_only"
                    }
            
            logger.warning("No response received from Gemini API")
            return None
            
        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_file_path)
            except Exception:
                pass
        
    except Exception as e:
        # Log specific error types for better debugging
        error_msg = str(e)
        if "400" in error_msg:
            logger.error(f"AI analysis failed for {pdf_name}: Invalid PDF format or content - {error_msg}")
        elif "403" in error_msg:
            logger.error(f"AI analysis failed for {pdf_name}: API key invalid or permissions denied - {error_msg}")
        elif "429" in error_msg:
            logger.error(f"AI analysis failed for {pdf_name}: Rate limit exceeded - {error_msg}")
        elif "500" in error_msg:
            logger.error(f"AI analysis failed for {pdf_name}: Gemini API internal error - {error_msg}")
        else:
            logger.error(f"AI analysis failed for {pdf_name}: {error_msg}")
        
        return None


def format_analysis_for_display(analysis: Dict[str, Any]) -> str:
    """
    Format the AI analysis results for display in web interface.
    
    Args:
        analysis: Analysis results dictionary from analyze_pdf_bytes_with_gemini
        
    Returns:
        HTML formatted string for display
    """
    if not analysis:
        return "<p>No analysis available</p>"
    
    # If it's a text-only analysis
    if analysis.get('status') == 'text_analysis_only':
        return f"<div class='analysis-text'><pre>{analysis.get('analysis_text', 'No analysis text available')}</pre></div>"
    
    html = "<div class='ai-analysis-results'>"
    
    # Company Information Section
    html += "<div class='analysis-section'>"
    html += "<h3>🏢 Company Information</h3>"
    html += f"<p><strong>Company:</strong> {analysis.get('company_name', 'N/A')}</p>"
    html += f"<p><strong>Scrip Code:</strong> {analysis.get('scrip_code', 'N/A')}</p>"
    html += f"<p><strong>Document Type:</strong> {analysis.get('document_type', 'N/A')}</p>"
    html += "</div>"
    
    # Financial Summary Section
    key_financials = analysis.get('key_financials', {})
    if key_financials and isinstance(key_financials, dict):
        html += "<div class='analysis-section'>"
        html += "<h3>💰 Financial Summary</h3>"
        for key, value in key_financials.items():
            if value and value != 'N/A':
                html += f"<p><strong>{key.title()}:</strong> {value}</p>"
        html += "</div>"
    
    # Investment Analysis Section
    html += "<div class='analysis-section'>"
    html += "<h3>📈 Investment Analysis</h3>"
    html += f"<p><strong>Recommendation:</strong> <span class='recommendation'>{analysis.get('investment_recommendation', 'N/A')}</span></p>"
    html += f"<p><strong>Price Target:</strong> {analysis.get('price_target', 'N/A')}</p>"
    html += f"<p><strong>Analysis:</strong> {analysis.get('sentiment_analysis', 'N/A')}</p>"
    html += "</div>"
    
    # Market Impact Section
    html += "<div class='analysis-section'>"
    html += "<h3>🎯 Market Impact</h3>"
    html += f"<p><strong>Public Perception:</strong> {analysis.get('public_perception', 'N/A')}</p>"
    html += f"<p><strong>Catalyst Impact:</strong> {analysis.get('catalyst_impact', 'N/A')}</p>"
    html += f"<p><strong>Price Momentum:</strong> {analysis.get('price_momentum', 'N/A')}</p>"
    html += "</div>"
    
    # Summary Section
    tldr = analysis.get('tldr')
    if tldr and tldr != 'N/A':
        html += "<div class='analysis-section'>"
        html += "<h3>📝 Summary</h3>"
        html += f"<p>{tldr}</p>"
        html += "</div>"
    
    # Metadata
    html += "<div class='analysis-metadata'>"
    html += f"<small>Analysis generated on {analysis.get('analysis_timestamp', 'Unknown')} using {analysis.get('model_used', 'AI Model')}</small>"
    html += "</div>"
    
    html += "</div>"
    
    return html


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """
    Extract text content from PDF bytes for fallback analysis.
    
    Args:
        pdf_bytes: The PDF file content as bytes
        
    Returns:
        Extracted text content or empty string if extraction fails
    """
    if not PDF_READER_AVAILABLE:
        return ""
    
    try:
        import io
        pdf_file = io.BytesIO(pdf_bytes)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        text_content = ""
        for page in pdf_reader.pages:
            text_content += page.extract_text() + "\n"
        
        return text_content.strip()
        
    except Exception as e:
        logger.error(f"Failed to extract text from PDF: {str(e)}")
        return ""


def validate_pdf_content(pdf_bytes: bytes) -> bool:
    """
    Validate that the provided bytes represent a valid PDF file.
    
    Args:
        pdf_bytes: The PDF file content as bytes
        
    Returns:
        True if valid PDF, False otherwise
    """
    try:
        # Check PDF header
        if not pdf_bytes.startswith(b'%PDF-'):
            return False
        
        # Try to read with PyPDF2 if available
        if PDF_READER_AVAILABLE:
            import io
            pdf_file = io.BytesIO(pdf_bytes)
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            # Try to access the first page
            if len(pdf_reader.pages) > 0:
                pdf_reader.pages[0]
            
        return True
        
    except Exception:
        return False


# Import pandas for timestamp functionality
try:
    import pandas as pd
except ImportError:
    # Fallback for timestamp if pandas not available
    import datetime
    class pd:
        class Timestamp:
            @staticmethod
            def now():
                return datetime.datetime.now()


def format_structured_telegram_message(analysis: Dict[str, Any], scrip_code: str, announcement_title: str, ann_date_ist, is_quarterly: bool = False) -> str:
    """Format the Telegram message according to the requested structure"""
    from datetime import datetime
    
    try:
        # Extract data from AI analysis
        company_name = analysis.get("company_name", "N/A")
        ai_scrip_code = analysis.get("scrip_code", scrip_code)  # Use AI extracted or fallback to BSE code
        stock_price = analysis.get("current_stock_price", "N/A")
        price_change = analysis.get("price_change", "")
        ai_title = analysis.get("announcement_title", announcement_title)
        
        # Format date as DD/MM/YY HH:MM AM/PM
        if ann_date_ist:
            formatted_date = ann_date_ist.strftime("%d/%m/%y %I:%M %p")
        else:
            formatted_date = "N/A"
        
        # Build price display with Yahoo fallback if AI missed it
        price_display = stock_price
        if (not price_display) or price_display == "N/A":
            try:
                #live = get_stock_data_yahoo(scrip_code)
                live_price = None #live.get('current_price') if isinstance(live, dict) else None
                if live_price not in (None, 'N/A'):
                    price_display = str(live_price)
                    # If AI didn't provide change, use Yahoo's
                    if not price_change:
                        live_change = None #live.get('day_change_percent')
                        if live_change not in (None, 'N/A'):
                            price_change = f"{live_change}%"
            except Exception:
                pass
        
        # Check if this is a quarterly results document
        doc_type = analysis.get("document_type", "").lower()
        quarterly_data = analysis.get("quarterly_financials")
        
        # Base message structure
        message_parts = [
            f"🏢 {company_name} ({ai_scrip_code})",
            f"📄 {ai_title}",
            f"📅 {formatted_date}",
            f"💹 ₹{price_display} {price_change}" if price_display != "N/A" else "",
        ]
        
        # Add quarterly results section if this is a quarterly document AND has quarterly data
        if is_quarterly and (doc_type == "quarterly_results" or quarterly_data):
            message_parts.append("\n📈 QUARTERLY RESULTS ANALYSIS:")
            
            current_q = quarterly_data.get("current_quarter", {})
            previous_q = quarterly_data.get("previous_quarter", {})
            growth = quarterly_data.get("growth_analysis", {})
            
            if current_q and previous_q:
                # Current Quarter
                curr_period = current_q.get("period", "Current Q")
                curr_income = current_q.get("total_income", "N/A")
                curr_expenses = current_q.get("total_expenses", "N/A")
                curr_pbt = current_q.get("profit_before_tax", "N/A")
                
                # Previous Quarter
                prev_period = previous_q.get("period", "Previous Q")
                prev_income = previous_q.get("total_income", "N/A")
                prev_expenses = previous_q.get("total_expenses", "N/A")
                prev_pbt = previous_q.get("profit_before_tax", "N/A")
                
                # Growth rates
                income_growth = growth.get("income_growth_percent", "N/A")
                expenses_growth = growth.get("expenses_growth_percent", "N/A")
                pbt_growth = growth.get("pbt_growth_percent", "N/A")
                
                quarterly_summary = f"""
📅 {curr_period}:
  • Total Income: ₹{curr_income} Cr
  • Total Expenses: ₹{curr_expenses} Cr
  • Profit Before Tax: ₹{curr_pbt} Cr
  
📅 {prev_period}:
  • Total Income: ₹{prev_income} Cr  
  • Total Expenses: ₹{prev_expenses} Cr
  • Profit Before Tax: ₹{prev_pbt} Cr

📊 QoQ Growth:
  • Income: {income_growth}%
  • Expenses: {expenses_growth}%
  • Profit Before Tax: {pbt_growth}%"""
                
                message_parts.append(quarterly_summary)
                
                # Add stock price comparison (3-month lookback)
                try:
                    from database import get_close_3m_ago, bse_code_to_yahoo_symbol, get_cmp_and_prev
                    
                    yahoo_symbol = bse_code_to_yahoo_symbol(scrip_code)
                    if yahoo_symbol:
                        # Get current price
                        current_price, _, _ = get_cmp_and_prev(yahoo_symbol)
                        
                        # Get 3-month ago price
                        price_3m_ago = get_close_3m_ago(yahoo_symbol)
                        
                        if current_price is not None and price_3m_ago is not None:
                            # Calculate 3-month price change
                            price_change_3m = ((current_price - price_3m_ago) / price_3m_ago) * 100
                            
                            # Determine if price change aligns with fundamentals
                            avg_growth = 0
                            growth_metrics = [income_growth, pbt_growth]
                            valid_metrics = [g for g in growth_metrics if g != "N/A" and isinstance(g, (int, float))]
                            
                            if valid_metrics:
                                avg_growth = sum(valid_metrics) / len(valid_metrics)
                            
                            alignment = "ALIGNED" if abs(price_change_3m - avg_growth) < 20 else "DIVERGENT"
                            alignment_icon = "✅" if alignment == "ALIGNED" else "⚠️"
                            
                            price_comparison = f"""

📈 STOCK PRICE ANALYSIS:
  • Current Price: ₹{current_price:.2f}
  • 3M Ago Price: ₹{price_3m_ago:.2f}
  • 3M Price Change: {price_change_3m:+.2f}%
  • Avg QoQ Growth: {avg_growth:.2f}% (Income + PBT)
  • Price-Growth Alignment: {alignment_icon} {alignment}"""
                            
                            message_parts.append(price_comparison)
                except Exception:
                    # If price comparison fails, continue without it
                    pass
        else:
            # For non-quarterly announcements, add generic AI analysis section
            if not is_quarterly:
                announcement_type = analysis.get("document_type", "Corporate Announcement").title()
                
                # Add announcement type specific section
                message_parts.append(f"\n📋 {announcement_type.upper()} ANALYSIS:")
                
                # Add key financial metrics if available
                financial_summary = analysis.get("financial_summary", "")
                if financial_summary:
                    message_parts.append(f"\n💰 Financial Impact: {financial_summary}")
                
                # Add business impact
                business_impact = analysis.get("business_impact", "")
                if business_impact:
                    message_parts.append(f"\n🏭 Business Impact: {business_impact}")
                
                # Add market implications
                market_impact = analysis.get("market_implications", "")
                if market_impact:
                    message_parts.append(f"\n📈 Market Implications: {market_impact}")
                
                # Add risk assessment if available
                risk_factors = analysis.get("risk_assessment", "")
                if risk_factors:
                    message_parts.append(f"\n⚠️ Risk Factors: {risk_factors}")
        
        # Add AI recommendation
        recommendation = analysis.get("investment_recommendation", "")
        sentiment = analysis.get("sentiment_analysis", "")
        if recommendation:
            message_parts.append(f"\n🤖 AI Analysis: {recommendation}")
        if sentiment:
            message_parts.append(f"📊 Analysis: {sentiment}")
        
        # Add key insights
        gist = analysis.get("gist", "")
        tldr = analysis.get("tldr", "")
        if gist:
            message_parts.append(f"\n📝 Key Impact: {gist}")
        if tldr:
            message_parts.append(f"⚡ Bottom Line: {tldr}")
            
        return "\n".join([part for part in message_parts if part.strip()])
        
    except Exception as e:
        logging.error(f"Error formatting telegram message: {e}")
        # Fallback to basic format
        return f"🏢 {analysis.get('company_name', 'Company')} ({scrip_code})\n📄 {announcement_title}\n📅 {ann_date_ist.strftime('%d/%m/%y %I:%M %p') if ann_date_ist else 'N/A'}"


def is_quarterly_results_document(headline: str, category: str = None) -> bool:
    """Check if document is likely a quarterly results document"""
    if not headline:
        return False
    
    h = headline.lower()
    
    # Check for quarterly results indicators
    quarterly_indicators = [
        "unaudited financial results",
        "quarterly results", 
        "unaudited results",
        "financial results",
        "q1", "q2", "q3", "q4",
        "quarter ended",
        "months ended"
    ]
    
    # Must be categorized as financials and contain quarterly indicators
    is_financial = (category == 'financials' or 
                   ('unaudited' in h and ('result' in h or 'financial' in h)))
    
    has_quarterly_terms = any(indicator in h for indicator in quarterly_indicators)
    
    return is_financial and has_quarterly_terms


def extract_financial_figures(text: str) -> dict:
    """Extract financial figures from text (fallback method)"""
    import re
    
    # Common patterns for financial figures in Crores
    patterns = {
        'total_income': r'total\s+income[\s:]+(?:rs?\.?\s*)?([\d,]+\.?\d*)\s*(?:cr|crore|crores)',
        'total_revenue': r'total\s+revenue[\s:]+(?:rs?\.?\s*)?([\d,]+\.?\d*)\s*(?:cr|crore|crores)',
        'revenue_operations': r'revenue\s+from\s+operations[\s:]+(?:rs?\.?\s*)?([\d,]+\.?\d*)\s*(?:cr|crore|crores)'
    }
    
    results = {}
    text_lower = text.lower()
    
    for key, pattern in patterns.items():
        matches = re.findall(pattern, text_lower, re.IGNORECASE)
        if matches:
            # Take the first match and clean it
            value = matches[0].replace(',', '').strip()
            try:
                results[key] = float(value)
            except ValueError:
                continue
    
    return results


def validate_quarterly_data(quarterly_data: dict) -> bool:
    """Validate that quarterly data contains required fields"""
    if not quarterly_data:
        return False
    
    current_q = quarterly_data.get('current_quarter', {})
    previous_q = quarterly_data.get('previous_quarter', {})
    
    # Check if both quarters have required data
    required_fields = ['period', 'total_income', 'total_expenses', 'profit_before_tax']
    
    current_valid = all(field in current_q and 
                       current_q[field] not in [None, '', 'N/A'] 
                       for field in required_fields)
    
    previous_valid = all(field in previous_q and 
                        previous_q[field] not in [None, '', 'N/A'] 
                        for field in required_fields)
    
    return current_valid and previous_valid