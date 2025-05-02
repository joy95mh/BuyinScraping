import re
import logging

logger = logging.getLogger("price_formatter")

def format_pl_price(price_text):
    """
    Format Polish price strings to a consistent decimal format with 2 digits.
    
    Args:
        price_text (str): The price text to format, which may include currency symbols,
                          spaces, and either comma or dot as decimal separator.
                          Examples: "29,35 zł", "6 899.00 zł", "587.89", "1.234,56 PLN", "2,749 zł", "1,499 zł"
    
    Returns:
        str: A formatted price string with 2 decimal places (e.g., "29.35", "6899.00", "587.89", "2749.00")
             or empty string if parsing fails
    """
    if not price_text:
        return ""
    
    try:
        original_price = str(price_text).strip()
        logger.debug(f"Formatting price: '{original_price}'")
        
        # Remove currency symbols and other non-numeric characters except for dots, commas and spaces
        cleaned = re.sub(r'[^\d,.\s]', '', original_price)
        
        # Remove all whitespace
        cleaned = cleaned.replace(' ', '')
        
        # Case 1: Handle formats like "2,549.00 zł" - comma as thousands separator, dot as decimal
        if ',' in cleaned and '.' in cleaned:
            # In the Polish format, typically dot is used as thousand separator and comma as decimal
            # But in many e-commerce sites, they use comma as thousand separator and dot as decimal (US-style)
            dot_pos = cleaned.rfind('.')
            comma_pos = cleaned.rfind(',')
            
            # If the dot is further right than the comma, it's likely dot is decimal: "2,549.00"
            if dot_pos > comma_pos:
                logger.debug(f"Comma as thousands separator, dot as decimal: '{original_price}'")
                # Remove commas (thousand separators)
                cleaned = cleaned.replace(',', '')
            else:
                # Format like "2.549,00" (Polish style: dot as thousands, comma as decimal)
                logger.debug(f"Dot as thousands separator, comma as decimal: '{original_price}'")
                cleaned = cleaned.replace('.', '')
                cleaned = cleaned.replace(',', '.')
        
        # Case 2: Only comma is present - e.g., "1,499" or "29,99"
        elif ',' in cleaned:
            # Case 2a: For "1,499" type - comma is a thousands separator
            if cleaned.count(',') == 1:
                comma_pos = cleaned.find(',')
                chars_after_comma = len(cleaned) - comma_pos - 1
                
                if chars_after_comma == 3:
                    # This is likely a thousands separator: "1,499" -> "1499.00"
                    logger.debug(f"Single comma as thousands separator: '{original_price}'")
                    cleaned = cleaned.replace(',', '')
                elif chars_after_comma == 2:
                    # This is likely a decimal separator: "29,99" -> "29.99"
                    logger.debug(f"Single comma as decimal separator: '{original_price}'")
                    cleaned = cleaned.replace(',', '.')
                else:
                    # For other cases, check specific format
                    if re.search(r',\d{3}(?!\d)', cleaned):
                        logger.debug(f"Comma followed by 3 digits, treating as thousands: '{original_price}'")
                        cleaned = cleaned.replace(',', '')
                    else:
                        logger.debug(f"Treating comma as decimal for general case: '{original_price}'")
                        cleaned = cleaned.replace(',', '.')
            # Case 2b: Multiple commas - probably all are thousands separators: "1,234,567"
            else:
                logger.debug(f"Multiple commas as thousands separators: '{original_price}'")
                cleaned = cleaned.replace(',', '')
        
        # Case 3: Only dot is present
        elif '.' in cleaned:
            # Check if it looks like a thousands separator (e.g., "2.749")
            dot_pos = cleaned.find('.')
            chars_after_dot = len(cleaned) - dot_pos - 1
            
            if chars_after_dot == 3 and cleaned.count('.') == 1:
                logger.debug(f"Single dot followed by 3 digits, might be thousands: '{original_price}'")
                # Check context - in Polish context, it's likely a thousands separator
                # This is a guess based on the market we're dealing with
                cleaned = cleaned.replace('.', '')
            else:
                # Keep the dot as decimal separator
                logger.debug(f"Keeping dot as decimal separator: '{original_price}'")
                pass
        
        # Parse the float value
        price_value = float(cleaned)
        
        # Format to 2 decimal places
        formatted_price = f"{price_value:.2f}"
        
        logger.debug(f"Formatted '{original_price}' to '{formatted_price}'")
        return formatted_price
        
    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to format price '{price_text}': {str(e)}")
        return "" 