"""
Sitemap URL Filtering Functions
================================

Reusable functions for filtering sitemap URLs based on:
1. Word-based filtering (reject non-news content)
2. Date-based filtering (keep only recent URLs)

Usage:
    from sitemap_filters import filter_by_words, filter_by_date, filter_sitemap_url
    
    # Single URL check
    should_keep, reason = filter_sitemap_url(url, hours_threshold=24)
    
    # Batch filtering
    filtered_urls = batch_filter_sitemaps(urls, hours_threshold=24)
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict


# ============================================================================
# WORD-BASED FILTERING
# ============================================================================

# Pre-compiled pattern for performance (compiled once, used many times)
REJECT_WORDS_PATTERN = re.compile(
    r'(?:^|[/_-])(video|videos|image|images|audio|photo|photos|'
    r'gallery|galleries|multimedia|media|'
    r'sport|sports|score|scores|match|matches|fixture|fixtures|'
    r'team|teams|league|leagues|food|cooking|menu|restaurant|'
    r'weather|forecast|climate|temperature|'
    r'puzzle|puzzles|game|games|quiz|quizzes|crossword|sudoku|'
    r'horoscope|lottery|'
    r'recipe|recipes|food|cooking|menu|restaurant|'
    r'taxonomy|label|labels|keyword|keywords|'
    r'page|pages|static|template|templates|'
    r'product|products|shop|store|stores|cart|ecommerce|shopping|'
    r'event|events|calendar|webinar|webinars|schedule|'
    r'archive|archives|old|backup|legacy|'
    r'location|locations|place|places|city|cities|'
    r'country|countries|region|regions|'
    r'forum|forums|discussion|comment|comments|feedback|'
    r'testimonial|testimonials|review|reviews|'
    r'portfolio|project|projects|partner|partners|'
    r'client|clients|sponsor|sponsors|'
    r'career|careers|job|jobs|vacancy|vacancies)(?:[/_.-]|$)',
    re.IGNORECASE
)


def filter_by_words(url: str) -> Tuple[bool, Optional[str]]:
    """
    Check if URL contains reject words (video, sports, weather, etc.).
    
    Args:
        url: Sitemap URL to check
        
    Returns:
        (should_keep, matched_word)
        - should_keep: False if URL should be rejected
        - matched_word: The word that caused rejection (or None)
        
    Examples:
        >>> filter_by_words("https://cnn.com/video-sitemap.xml")
        (False, 'video')
        
        >>> filter_by_words("https://cnn.com/news-sitemap.xml")
        (True, None)
    """
    match = REJECT_WORDS_PATTERN.search(url)
    if match:
        matched_word = match.group(1)
        return False, matched_word
    return True, None


# ============================================================================
# DATE-BASED FILTERING
# ============================================================================

def extract_date_from_url(url: str) -> Optional[datetime]:
    """
    Extract date from URL using multiple patterns.
    Patterns ordered from MOST to LEAST specific for accurate extraction.
    
    Supported patterns (in priority order):
        0. Query params for robots.txt: ?year=2020, ?date=2025-01-01 (HIGHEST - for fast filtering)
        1. Compact 8 digits: -20251018.xml (MOST SPECIFIC)
        2. Dashes with day: -2025-10-18.xml (VERY SPECIFIC)
        3. Path slashes: /2025/10/18/ (SPECIFIC)
        4. Query params: ?yyyy=2025&mm=10&dd=18 (SPECIFIC)
        5. Year-Month only: -2025-10.xml (LEAST SPECIFIC - tries last)
        
    Args:
        url: URL to extract date from
        
    Returns:
        datetime object (UTC) or None if no date found
        
    Examples:
        >>> extract_date_from_url("sitemap.xml?yyyy=2025&mm=10&dd=18")
        datetime(2025, 10, 18, 0, 0, tzinfo=timezone.utc)
        
        >>> extract_date_from_url("sitemap-2025-10-18.xml")
        datetime(2025, 10, 18, 0, 0, tzinfo=timezone.utc)
        
        >>> extract_date_from_url("news-sitemap.xml")
        None
    """
    import calendar
    
    # ===== PRIORITY 0: Query params for robots.txt URLs (HIGHEST) =====
    # Example: editorial.xml?year=2020
    match = re.search(r'[?&]year=(\d{4})', url)
    if match:
        try:
            year = int(match.group(1))
            # Assume January 1st of that year for year-only params
            return datetime(year, 1, 1, tzinfo=timezone.utc)
        except (ValueError, OverflowError):
            pass
    
    # Example: sitemap.xml?date=2025-01-01
    match = re.search(r'[?&]date=(\d{4})-(\d{2})-(\d{2})', url)
    if match:
        try:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return datetime(year, month, day, tzinfo=timezone.utc)
        except (ValueError, OverflowError):
            pass
    
    # ===== PRIORITY 1: Compact 8 digits (MOST SPECIFIC) =====
    # Example: sitemap-20251018.xml
    match = re.search(r'[-_/](\d{8})(?:[-_.]|$)', url)
    if match:
        try:
            date_str = match.group(1)
            year = int(date_str[0:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            return datetime(year, month, day, tzinfo=timezone.utc)
        except (ValueError, OverflowError, IndexError):
            pass  # Invalid date, try next pattern
    
    # ===== PRIORITY 2: Dashes with full date (VERY SPECIFIC) =====
    # Example: sitemap-2025-10-18.xml
    match = re.search(r'[-_](\d{4})-(\d{2})-(\d{2})', url)
    if match:
        try:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return datetime(year, month, day, tzinfo=timezone.utc)
        except (ValueError, OverflowError):
            pass
    
    # ===== PRIORITY 3: Slashes in path (SPECIFIC) =====
    # Example: /news/2025/10/18/sitemap.xml
    # Improved regex: ensures end of path or trailing slash
    match = re.search(r'/(\d{4})/(\d{1,2})/(\d{1,2})(?:/|$)', url)
    if match:
        try:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return datetime(year, month, day, tzinfo=timezone.utc)
        except (ValueError, OverflowError):
            pass
    
    # ===== PRIORITY 4: Query params (SPECIFIC) =====
    # Example: sitemap.xml?yyyy=2025&mm=10&dd=18
    match = re.search(r'[?&]yyyy=(\d{4}).*?mm=(\d{1,2}).*?dd=(\d{1,2})', url)
    if match:
        try:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return datetime(year, month, day, tzinfo=timezone.utc)
        except (ValueError, OverflowError):
            pass
    
    # ===== PRIORITY 5: Year-Month only (LEAST SPECIFIC - LAST!) =====
    # Example: sitemap-2025-10.xml
    # Improved regex: prevent matching partial dates like 2025-10 in 2025-10-18
    match = re.search(r'[-_/](\d{4})-(\d{2})(?:[-_.]|\.xml|$)(?!-\d{2})', url)
    if match:
        try:
            year = int(match.group(1))
            month = int(match.group(2))
            
            # SMART HANDLING: Use different day based on context
            now = datetime.now(timezone.utc)
            
            # If same year and month as current → use current day
            if year == now.year and month == now.month:
                day = now.day
            else:
                # Past or future month → use last day of that month
                day = calendar.monthrange(year, month)[1]
            
            return datetime(year, month, day, tzinfo=timezone.utc)
        except (ValueError, OverflowError):
            pass
    
    # No date found
    return None


def filter_by_date(
    url: str, 
    hours_threshold: int = 24,
    conservative: bool = True,
    xml_lastmod: Optional[datetime] = None
) -> Tuple[bool, str]:
    """
    Check if URL date is within the time threshold.
    Uses priority system: URL date first, then XML lastmod if provided.
    
    Args:
        url: Sitemap URL to check
        hours_threshold: Maximum age in hours (default: 24)
        conservative: If True, keep URLs with no date (default: True)
        xml_lastmod: Optional datetime from XML <lastmod> tag
        
    Returns:
        (should_keep, reason)
        - should_keep: True if URL should be kept
        - reason: Human-readable explanation
        
    Examples:
        >>> # URL from today
        >>> filter_by_date("sitemap-2025-10-18.xml", hours_threshold=24)
        (True, "Recent URL date (0.5h old)")
        
        >>> # URL from 2 days ago
        >>> filter_by_date("sitemap-2025-10-16.xml", hours_threshold=24)
        (False, "Old URL date (48.0h > 24h)")
        
        >>> # URL with no date, but XML lastmod provided
        >>> filter_by_date("news-sitemap.xml", hours_threshold=24, 
        ...                xml_lastmod=datetime(2025, 10, 18, tzinfo=timezone.utc))
        (True, "Recent XML lastmod (5.5h old)")
    """
    now = datetime.now(timezone.utc)
    
    # PRIORITY 1: Check URL for date pattern
    url_date = extract_date_from_url(url)
    
    if url_date:
        age = now - url_date
        age_hours = age.total_seconds() / 3600
        
        # Handle future dates (timezone issues or server clock ahead)
        if age_hours < 0:
            future_hours = abs(age_hours)
            if future_hours <= 24:
                return True, f"Future URL date ({future_hours:.1f}h ahead) - keeping (timezone?)"
            else:
                return False, f"Far future URL date ({future_hours:.1f}h ahead) - rejecting"
        
        # Normal age check (past dates)
        if age_hours <= hours_threshold:
            return True, f"Recent URL date ({age_hours:.1f}h old)"
        else:
            return False, f"Old URL date ({age_hours:.1f}h > {hours_threshold}h)"
    
    # PRIORITY 2: Check XML lastmod (if provided)
    if xml_lastmod:
        age = now - xml_lastmod
        age_hours = age.total_seconds() / 3600
        
        # Handle future dates
        if age_hours < 0:
            future_hours = abs(age_hours)
            if future_hours <= 24:
                return True, f"Future XML lastmod ({future_hours:.1f}h ahead) - keeping (timezone?)"
            else:
                return False, f"Far future XML lastmod ({future_hours:.1f}h ahead) - rejecting"
        
        # Normal age check
        if age_hours <= hours_threshold:
            return True, f"Recent XML lastmod ({age_hours:.1f}h old)"
        else:
            return False, f"Old XML lastmod ({age_hours:.1f}h > {hours_threshold}h)"
    
    # PRIORITY 3: No date found in URL or XML
    if conservative:
        return True, "No date in URL or XML - keeping (conservative)"
    else:
        return False, "No date in URL or XML - rejecting"


# ============================================================================
# COMBINED FILTERING
# ============================================================================

def filter_sitemap_url(
    url: str,
    hours_threshold: int = 24,
    use_word_filter: bool = True,
    use_date_filter: bool = True,
    conservative: bool = True
) -> Tuple[bool, str, Dict[str, any]]:
    """
    Combined filter: Word-based + Date-based.
    
    Args:
        url: Sitemap URL to check
        hours_threshold: Maximum age in hours for date filter
        use_word_filter: Enable word-based filtering
        use_date_filter: Enable date-based filtering
        conservative: Keep URLs with no date if True
        
    Returns:
        (should_keep, reason, details)
        - should_keep: True if URL should be kept
        - reason: Human-readable explanation
        - details: Dict with filter results
        
    Examples:
        >>> filter_sitemap_url("https://cnn.com/video-sitemap.xml")
        (False, "Rejected by word filter (video)", {...})
        
        >>> filter_sitemap_url("https://cnn.com/sitemap-2025-10-16.xml", hours_threshold=24)
        (False, "Rejected by date filter (48.0h old)", {...})
        
        >>> filter_sitemap_url("https://cnn.com/news-sitemap.xml")
        (True, "Passed all filters", {...})
    """
    details = {
        'word_filter': {'passed': None, 'matched_word': None},
        'date_filter': {'passed': None, 'age_hours': None, 'url_date': None}
    }
    
    # Phase 1: Word Filter
    if use_word_filter:
        word_keep, matched_word = filter_by_words(url)
        details['word_filter']['passed'] = word_keep
        details['word_filter']['matched_word'] = matched_word
        
        if not word_keep:
            return False, f"Rejected by word filter ({matched_word})", details
    
    # Phase 2: Date Filter
    if use_date_filter:
        date_keep, date_reason = filter_by_date(url, hours_threshold, conservative)
        
        url_date = extract_date_from_url(url)
        if url_date:
            now = datetime.now(timezone.utc)
            age_hours = (now - url_date).total_seconds() / 3600
            details['date_filter']['age_hours'] = age_hours
            details['date_filter']['url_date'] = url_date.isoformat()
        
        details['date_filter']['passed'] = date_keep
        
        if not date_keep:
            return False, f"Rejected by date filter ({date_reason})", details
    
    return True, "Passed all filters", details


def batch_filter_sitemaps(
    urls: List[str],
    hours_threshold: int = 24,
    use_word_filter: bool = True,
    use_date_filter: bool = True,
    conservative: bool = True,
    verbose: bool = False
) -> Dict[str, any]:
    """
    Filter multiple sitemap URLs at once.
    
    Args:
        urls: List of sitemap URLs to filter
        hours_threshold: Maximum age in hours
        use_word_filter: Enable word filtering
        use_date_filter: Enable date filtering
        conservative: Keep URLs with no date
        verbose: Print progress messages
        
    Returns:
        Dict with results:
        {
            'passed': [...],        # URLs that passed
            'rejected': [...],      # URLs that were rejected
            'stats': {...}          # Statistics
        }
        
    Example:
        >>> urls = ["video-sitemap.xml", "news-sitemap.xml"]
        >>> results = batch_filter_sitemaps(urls, hours_threshold=24)
        >>> print(results['stats'])
        {'total': 2, 'passed': 1, 'rejected': 1, ...}
    """
    passed = []
    rejected = []
    
    stats = {
        'total': len(urls),
        'passed': 0,
        'rejected': 0,
        'rejected_by_word': 0,
        'rejected_by_date': 0,
    }
    
    for idx, url in enumerate(urls, 1):
        should_keep, reason, details = filter_sitemap_url(
            url,
            hours_threshold=hours_threshold,
            use_word_filter=use_word_filter,
            use_date_filter=use_date_filter,
            conservative=conservative
        )
        
        if should_keep:
            passed.append(url)
            stats['passed'] += 1
            if verbose:
                print(f"[{idx}/{len(urls)}] ✅ {url}")
        else:
            rejected.append({'url': url, 'reason': reason, 'details': details})
            stats['rejected'] += 1
            
            # Count by filter type
            if 'word filter' in reason:
                stats['rejected_by_word'] += 1
            elif 'date filter' in reason:
                stats['rejected_by_date'] += 1
            
            if verbose:
                print(f"[{idx}/{len(urls)}] ❌ {url} → {reason}")
    
    return {
        'passed': passed,
        'rejected': rejected,
        'stats': stats
    }


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_filter_stats(results: Dict[str, any]) -> str:
    """
    Generate human-readable statistics from batch filter results.
    
    Args:
        results: Output from batch_filter_sitemaps()
        
    Returns:
        Formatted statistics string
    """
    stats = results['stats']
    total = stats['total']
    passed = stats['passed']
    rejected = stats['rejected']
    
    if total == 0:
        return "No URLs processed"
    
    pass_pct = (passed / total) * 100
    reject_pct = (rejected / total) * 100
    
    lines = [
        f"Total URLs: {total}",
        f"✅ Passed: {passed} ({pass_pct:.1f}%)",
        f"❌ Rejected: {rejected} ({reject_pct:.1f}%)",
    ]
    
    if stats['rejected_by_word'] > 0:
        lines.append(f"   • By word filter: {stats['rejected_by_word']}")
    if stats['rejected_by_date'] > 0:
        lines.append(f"   • By date filter: {stats['rejected_by_date']}")
    
    return "\n".join(lines)


# ============================================================================
# MAIN (for testing)
# ============================================================================

def filter_sitemaps_by_year(sitemap_urls: List[str]) -> List[str]:
    """
    Filter out sitemap URLs containing ANY year except the current year.
    Detects years in ANY pattern: archived_2019, -2020-, /2021/, ?year=2022, etc.
    
    Rule: If URL contains any year that's NOT current year → REJECT
    
    Args:
        sitemap_urls: List of sitemap URLs to filter
        
    Returns:
        Filtered list with only current-year or no-year URLs
        
    Examples:
        Current year = 2025:
        
        ❌ REJECT:
        - sitemap_archived_2019.xml (has 2019)
        - editorial.xml?year=2024 (has 2024)
        - /2023/sitemap.xml (has 2023)
        
        ✅ KEEP:
        - sitemap_archived_2025.xml (current year)
        - news.xml (no year)
        - sitemap_latest.xml (no year)
    """
    current_year = datetime.now().year
    
    # Comprehensive regex: catches years 1950-2039 anywhere in URL
    # Range 1950-2039 avoids false matches with port numbers (8080) or random IDs
    year_pattern = r'(19[5-9]\d|20[0-3]\d)'
    
    kept = []
    rejected = []
    
    for url in sitemap_urls:
        # Find ALL year occurrences in URL
        years_found = [int(y) for y in re.findall(year_pattern, url)]
        
        if not years_found:
            # No year found → KEEP (conservative)
            kept.append(url)
        elif all(y == current_year for y in years_found):
            # Only current year → KEEP
            kept.append(url)
        else:
            # Has old year(s) → REJECT
            old_years = [str(y) for y in years_found if y != current_year]
            rejected.append((url, f"old year(s): {', '.join(old_years)}"))
    
    # Logging
    if rejected:
        print(f"[year-filter] 🚀 Rejected {len(rejected)} sitemap(s) with old years:")
        for url, reason in rejected[:5]:
            # Shorten URL for display
            short_url = url.split('/')[-1] if '/' in url else url
            print(f"[year-filter]   ❌ {short_url} ({reason})")
        if len(rejected) > 5:
            print(f"[year-filter]   ... and {len(rejected) - 5} more")
    
    print(f"[year-filter] URLs: {len(sitemap_urls)} → {len(kept)} (rejected {len(rejected)} by year)")
    
    return kept


if __name__ == "__main__":
    # Test cases
    test_urls = [
        "https://cnn.com/news-sitemap.xml",
        "https://cnn.com/video-sitemap.xml",
        "https://bbc.com/sitemap.xml?yyyy=2025&mm=10&dd=18",
        "https://bbc.com/sitemap.xml?yyyy=2025&mm=10&dd=16",
        "https://nyt.com/sitemap-2025-10-18.xml",
        "https://nyt.com/sitemap-2025-09-15.xml",
        "https://espn.com/sports-sitemap.xml",
    ]
    
    print("=" * 70)
    print("🧪 SITEMAP FILTER TEST")
    print("=" * 70)
    print()
    
    # Batch filter
    results = batch_filter_sitemaps(
        test_urls,
        hours_threshold=24,
        verbose=True
    )
    
    print()
    print("=" * 70)
    print("📊 STATISTICS")
    print("=" * 70)
    print(get_filter_stats(results))
    print("=" * 70)

